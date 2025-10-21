"""MapReduce framework Manager node."""
from __future__ import annotations

import contextlib
import json
import logging
import os
import shutil
import socket
import tempfile
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Deque, Dict, List, Optional, Tuple

import click

from mapreduce.utils import serve_tcp


# Configure logging
LOGGER = logging.getLogger(__name__)

# Heartbeat configuration
HEARTBEAT_INTERVAL_SECS = 2
HEARTBEAT_TIMEOUT_SECS = HEARTBEAT_INTERVAL_SECS * 3


@dataclass
class Task:
    """Represent a unit of work assigned to a Worker."""

    job_id: int
    task_id: int
    task_type: str  # "map" or "reduce"
    executable: str
    input_paths: List[str]
    output_directory: str
    num_partitions: Optional[int] = None
    assigned_worker: Optional[Tuple[str, int]] = None
    status: str = "pending"  # pending, running, finished


@dataclass
class Job:
    """Keep track of MapReduce job state."""

    job_id: int
    input_directory: Path
    output_directory: Path
    mapper_executable: str
    reducer_executable: str
    num_mappers: int
    num_reducers: int
    job_dir: Path
    map_tasks: Dict[int, Task] = field(default_factory=dict)
    reduce_tasks: Dict[int, Task] = field(default_factory=dict)
    map_pending: Deque[int] = field(default_factory=deque)
    reduce_pending: Deque[int] = field(default_factory=deque)
    map_completed: set[int] = field(default_factory=set)
    reduce_completed: set[int] = field(default_factory=set)
    reduce_created: bool = False


@dataclass
class WorkerInfo:
    """Represent state tracked for a Worker."""

    host: str
    port: int
    last_heartbeat: float = field(default_factory=time.time)
    current_task: Optional[Task] = None
    alive: bool = True
    shutting_down: bool = False
    registered_at: float = field(default_factory=time.time)


class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host: str, port: int) -> None:
        """Construct a Manager instance and start listening for messages."""
        LOGGER.info(
            "Starting manager host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )

        self.host = host
        self.port = int(port)
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

        # Track Workers and their availability
        self._workers: Dict[Tuple[str, int], WorkerInfo] = {}
        self._available_workers: Deque[Tuple[str, int]] = deque()
        self._available_worker_set: set[Tuple[str, int]] = set()

        # Track jobs in submission order
        self._jobs: Dict[int, Job] = {}
        self._job_order: Deque[int] = deque()
        self._next_job_id = 0

        # Temporary directory shared between Manager and Workers
        self._tmpdir_stack = contextlib.ExitStack()
        tmpdir_obj = self._tmpdir_stack.enter_context(
            tempfile.TemporaryDirectory(prefix="mapreduce-shared-")
        )
        self.shared_dir = Path(tmpdir_obj)
        LOGGER.info("Created tmpdir %s", self.shared_dir)

        # Start background threads
        self._threads: List[threading.Thread] = []
        heartbeat_thread = threading.Thread(
            target=self._heartbeat_listener,
            name="ManagerHeartbeatListener",
        )
        heartbeat_thread.start()
        self._threads.append(heartbeat_thread)

        monitor_thread = threading.Thread(
            target=self._monitor_workers,
            name="ManagerWorkerMonitor",
        )
        monitor_thread.start()
        self._threads.append(monitor_thread)

        try:
            self._serve()
        finally:
            self._stop_event.set()
            for thread in self._threads:
                thread.join()
            LOGGER.info("Cleaned up tmpdir %s", self.shared_dir)
            self._tmpdir_stack.close()

    # --------------------- Networking helpers --------------------- #

    def _serve(self) -> None:
        """Listen for TCP messages until shutdown."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
            server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_sock.bind((self.host, self.port))
            server_sock.listen()
            server_sock.settimeout(1.0)
            serve_tcp(
                server_sock,
                self._stop_event,
                LOGGER,
                self._handle_message,
            )

    def _heartbeat_listener(self) -> None:
        """Listen for Worker heartbeat UDP messages."""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp_sock:
            udp_sock.bind((self.host, self.port))
            udp_sock.settimeout(1.0)
            while not self._stop_event.is_set():
                try:
                    payload = udp_sock.recv(4096)
                except socket.timeout:
                    continue
                if not payload:
                    continue
                try:
                    message = json.loads(payload.decode("utf-8"))
                except json.JSONDecodeError:
                    continue
                if message.get("message_type") != "heartbeat":
                    continue
                worker_host = message.get("worker_host")
                worker_port = message.get("worker_port")
                if worker_host is None or worker_port is None:
                    continue
                worker_key = (worker_host, int(worker_port))
                with self._lock:
                    worker = self._workers.get(worker_key)
                    if worker and worker.alive:
                        worker.last_heartbeat = time.time()

    def _monitor_workers(self) -> None:
        """Detect dead Workers that stopped sending heartbeats."""
        while not self._stop_event.is_set():
            time.sleep(1)
            requeue_needed = False
            with self._lock:
                now = time.time()
                for worker_key, worker in list(self._workers.items()):
                    if not worker.alive or worker.shutting_down:
                        continue
                    if now - worker.last_heartbeat > HEARTBEAT_TIMEOUT_SECS:
                        LOGGER.warning(
                            "Worker %s:%s missed heartbeat, marking dead",
                            worker.host,
                            worker.port,
                        )
                        if self._mark_worker_dead_locked(worker_key):
                            requeue_needed = True
            if requeue_needed:
                self._try_dispatch()

    # --------------------- Message handling --------------------- #

    def _handle_message(self, message: dict) -> None:
        """Dispatch incoming TCP message based on its type."""
        message_type = message.get("message_type")
        if message_type == "register":
            self._handle_register(message)
        elif message_type == "new_manager_job":
            self._handle_new_manager_job(message)
        elif message_type == "finished":
            self._handle_finished(message)
        elif message_type == "shutdown":
            self._handle_shutdown()
        else:
            LOGGER.debug("Ignoring unsupported message: %s", message_type)

    def _handle_register(self, message: dict) -> None:
        worker_host = message.get("worker_host")
        worker_port = message.get("worker_port")
        if worker_host is None or worker_port is None:
            return
        worker_key = (worker_host, int(worker_port))

        with self._lock:
            worker = WorkerInfo(worker_host, int(worker_port))
            existing = self._workers.get(worker_key)
            if existing:
                worker.last_heartbeat = existing.last_heartbeat
            self._workers[worker_key] = worker
            self._queue_worker_locked(worker_key)

        try:
            self._send_message(worker_host, int(worker_port), {
                "message_type": "register_ack",
            })
        except OSError:
            with self._lock:
                self._mark_worker_dead_locked(worker_key)
            return

        self._try_dispatch()

    def _handle_new_manager_job(self, message: dict) -> None:
        job = self._build_job_from_message(message)
        if job is None:
            return

        with self._lock:
            self._jobs[job.job_id] = job
            self._job_order.append(job.job_id)

        self._try_dispatch()

    def _build_job_from_message(self, message: dict) -> Optional[Job]:
        parsed = self._parse_job_message(message)
        if parsed is None:
            return None
        (
            input_directory,
            output_directory,
            mapper_exec,
            reducer_exec,
            num_mappers,
            num_reducers,
        ) = parsed

        job_id = self._next_job_id
        self._next_job_id += 1

        self._prepare_output_directory(output_directory)
        job_dir = self._create_job_directory(job_id)

        job = Job(
            job_id=job_id,
            input_directory=input_directory,
            output_directory=output_directory,
            mapper_executable=mapper_exec,
            reducer_executable=reducer_exec,
            num_mappers=num_mappers,
            num_reducers=num_reducers,
            job_dir=job_dir,
        )

        partitions = self._partition_input_files(input_directory, num_mappers)
        self._initialize_map_tasks(job, partitions, num_reducers)
        return job

    def _parse_job_message(
        self,
        message: dict,
    ) -> Optional[Tuple[Path, Path, str, str, int, int]]:
        try:
            input_directory = Path(message["input_directory"])
            output_directory = Path(message["output_directory"])
            mapper_exec = str(message["mapper_executable"])
            reducer_exec = str(message["reducer_executable"])
            num_mappers = int(message["num_mappers"])
            num_reducers = int(message["num_reducers"])
        except (KeyError, TypeError, ValueError):
            LOGGER.error("Invalid job submission message: %s", message)
            return None

        if num_mappers <= 0 or num_reducers <= 0:
            LOGGER.error(
                "Invalid job configuration: num_mappers=%s num_reducers=%s",
                num_mappers,
                num_reducers,
            )
            return None

        return (
            input_directory,
            output_directory,
            mapper_exec,
            reducer_exec,
            num_mappers,
            num_reducers,
        )

    def _prepare_output_directory(self, output_directory: Path) -> None:
        if output_directory.exists():
            shutil.rmtree(output_directory)
        output_directory.mkdir(parents=True, exist_ok=True)

    def _create_job_directory(self, job_id: int) -> Path:
        job_dir = self.shared_dir / f"job-{job_id:05d}"
        job_dir.mkdir(parents=True, exist_ok=False)
        return job_dir

    def _partition_input_files(
        self,
        input_directory: Path,
        num_mappers: int,
    ) -> List[List[str]]:
        input_files = sorted(
            [
                path
                for path in input_directory.iterdir()
                if path.is_file()
            ],
            key=lambda path: path.name,
        )
        partitions: List[List[str]] = [[] for _ in range(num_mappers)]
        for index, path in enumerate(input_files):
            partitions[index % num_mappers].append(str(path))
        return partitions

    def _initialize_map_tasks(
        self,
        job: Job,
        partitions: List[List[str]],
        num_reducers: int,
    ) -> None:
        for task_id, input_paths in enumerate(partitions):
            task = Task(
                job_id=job.job_id,
                task_id=task_id,
                task_type="map",
                executable=job.mapper_executable,
                input_paths=input_paths,
                output_directory=str(job.job_dir),
                num_partitions=num_reducers,
            )
            job.map_tasks[task_id] = task
            job.map_pending.append(task_id)

    def _handle_finished(self, message: dict) -> None:
        worker_host = message.get("worker_host")
        worker_port = message.get("worker_port")
        if worker_host is None or worker_port is None:
            return
        worker_key = (worker_host, int(worker_port))

        with self._lock:
            worker = self._workers.get(worker_key)
            if not worker or not worker.current_task:
                return
            task = worker.current_task
            job = self._jobs.get(task.job_id)
            if not job:
                worker.current_task = None
                self._queue_worker_locked(worker_key)
                return

            task.status = "finished"
            task.assigned_worker = None
            worker.current_task = None
            worker.last_heartbeat = time.time()
            self._queue_worker_locked(worker_key)

            if task.task_type == "map":
                job.map_completed.add(task.task_id)
            else:
                job.reduce_completed.add(task.task_id)

            tasks_to_cleanup: List[Path] = []
            job_complete = False

            if (
                task.task_type == "map"
                and len(job.map_completed) == job.num_mappers
                and not job.reduce_created
            ):
                self._create_reduce_tasks_locked(job)

            if (
                task.task_type == "reduce"
                and len(job.reduce_completed) == job.num_reducers
            ):
                job_complete = True
                tasks_to_cleanup.append(job.job_dir)
                self._finalize_job_locked(job.job_id)

        if job_complete:
            for path in tasks_to_cleanup:
                shutil.rmtree(path, ignore_errors=True)

        self._try_dispatch()

    def _handle_shutdown(self) -> None:
        """Handle shutdown message from the client."""
        worker_destinations: List[Tuple[str, int]] = []
        with self._lock:
            self._stop_event.set()
            for worker_key, worker in self._workers.items():
                if worker.alive and not worker.shutting_down:
                    worker.shutting_down = True
                    worker_destinations.append(worker_key)
            self._available_workers.clear()
            self._available_worker_set.clear()

        for host, port in worker_destinations:
            try:
                self._send_message(host, port, {"message_type": "shutdown"})
            except OSError:
                continue

    # --------------------- Worker / job helpers --------------------- #

    def _queue_worker_locked(
        self,
        worker_key: Tuple[str, int],
        *,
        front: bool = False,
    ) -> None:
        worker = self._workers.get(worker_key)
        if (
            not worker
            or not worker.alive
            or worker.current_task
            or worker.shutting_down
        ):
            return
        if worker_key in self._available_worker_set:
            return
        if front:
            self._available_workers.appendleft(worker_key)
        else:
            self._available_workers.append(worker_key)
        self._available_worker_set.add(worker_key)

    def _create_reduce_tasks_locked(self, job: Job) -> None:
        if job.reduce_created:
            return
        for reducer_id in range(job.num_reducers):
            input_paths = [
                str(
                    job.job_dir /
                    f"maptask{map_task_id:05d}-part{reducer_id:05d}"
                )
                for map_task_id in range(job.num_mappers)
            ]
            task = Task(
                job_id=job.job_id,
                task_id=reducer_id,
                task_type="reduce",
                executable=job.reducer_executable,
                input_paths=input_paths,
                output_directory=str(job.output_directory),
            )
            job.reduce_tasks[reducer_id] = task
            job.reduce_pending.append(reducer_id)
        job.reduce_created = True

    def _finalize_job_locked(self, job_id: int) -> None:
        self._jobs.pop(job_id, None)
        self._job_order = deque(
            job for job in self._job_order if job != job_id
        )

    def _mark_worker_dead_locked(
        self,
        worker_key: Tuple[str, int],
        task: Optional[Task] = None,
    ) -> bool:
        worker = self._workers.get(worker_key)
        if not worker:
            return False
        if task is None:
            task = worker.current_task
        worker.alive = False
        worker.shutting_down = True
        worker.current_task = None
        self._available_worker_set.discard(worker_key)
        # Any stale entries in the deque will be skipped when popped.

        if not task or task.status != "running":
            return False

        task.status = "pending"
        task.assigned_worker = None
        job = self._jobs.get(task.job_id)
        if job:
            if task.task_type == "map":
                job.map_pending.appendleft(task.task_id)
            else:
                job.reduce_pending.appendleft(task.task_id)
        return True

    def _next_idle_worker_locked(
        self,
    ) -> Optional[Tuple[Tuple[str, int], WorkerInfo]]:
        while self._available_workers:
            worker_key = self._available_workers.popleft()
            self._available_worker_set.discard(worker_key)
            worker = self._workers.get(worker_key)
            if (
                worker
                and worker.alive
                and not worker.current_task
                and not worker.shutting_down
            ):
                return worker_key, worker
        return None

    def _next_ready_task_locked(self) -> Optional[Tuple[Job, Task]]:
        for job_id in list(self._job_order):
            job = self._jobs.get(job_id)
            if not job:
                continue
            if job.map_pending:
                task_id = job.map_pending.popleft()
                task = job.map_tasks[task_id]
                if task.status != "pending":
                    continue
                task.status = "running"
                return job, task
            if (
                not job.reduce_created
                and len(job.map_completed) == job.num_mappers
            ):
                self._create_reduce_tasks_locked(job)
            if job.reduce_pending:
                task_id = job.reduce_pending.popleft()
                task = job.reduce_tasks[task_id]
                if task.status != "pending":
                    continue
                task.status = "running"
                return job, task
        return None

    def _try_dispatch(self) -> None:
        while True:
            with self._lock:
                next_worker = self._next_idle_worker_locked()
                if not next_worker:
                    return
                worker_key, worker = next_worker
                next_task = self._next_ready_task_locked()
                if not next_task:
                    self._queue_worker_locked(worker_key, front=True)
                    return
                job, task = next_task
                worker.current_task = task
                task.assigned_worker = worker_key
                worker.last_heartbeat = time.time()
                message = self._task_to_message(job, task)
            try:
                self._send_message(worker.host, worker.port, message)
            except OSError:
                with self._lock:
                    worker.current_task = None
                    self._mark_worker_dead_locked(worker_key, task)
                continue

    def _task_to_message(self, job: Job, task: Task) -> dict:
        if task.task_type == "map":
            return {
                "message_type": "new_map_task",
                "task_id": task.task_id,
                "executable": task.executable,
                "input_paths": task.input_paths,
                "output_directory": task.output_directory,
                "num_partitions": job.num_reducers,
            }
        return {
            "message_type": "new_reduce_task",
            "task_id": task.task_id,
            "executable": task.executable,
            "input_paths": task.input_paths,
            "output_directory": str(job.output_directory),
        }

    # --------------------- Socket helper --------------------- #

    @staticmethod
    def _send_message(host: str, port: int, message: dict) -> None:
        payload = json.dumps(message).encode("utf-8")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((host, port))
            sock.sendall(payload)


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
@click.option("--shared_dir", "shared_dir", default=None)
def main(host, port, logfile, loglevel, shared_dir):
    """Run Manager."""
    tempfile.tempdir = shared_dir
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(
        f"Manager:{port} [%(levelname)s] %(message)s"
    )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Manager(host, port)


if __name__ == "__main__":
    main()
