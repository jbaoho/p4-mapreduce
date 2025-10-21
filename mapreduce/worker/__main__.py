"""MapReduce framework Worker node."""
from __future__ import annotations

import contextlib
import hashlib
import heapq
import itertools
import json
import logging
import os
import socket
import subprocess
import tempfile
import threading
from pathlib import Path
from typing import Iterable, List, Optional, TextIO

import shutil
import click

from mapreduce.utils import serve_tcp


# Configure logging
LOGGER = logging.getLogger(__name__)

# Heartbeat configuration matches manager/tests
HEARTBEAT_INTERVAL_SECS = 2


class Worker:
    """A class representing a Worker node in a MapReduce cluster."""

    def __init__(
        self,
        host: str,
        port: int,
        manager_host: str,
        manager_port: int,
    ):
        """Construct a Worker instance and start listening for messages."""
        LOGGER.info(
            "Starting worker host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )
        LOGGER.info(
            "manager_host=%s manager_port=%s",
            manager_host, manager_port,
        )

        self.host = host
        self.port = int(port)
        self.manager_host = manager_host
        self.manager_port = int(manager_port)

        self._stop_event = threading.Event()
        self._registered_event = threading.Event()
        self._heartbeat_thread: Optional[threading.Thread] = None

        try:
            with socket.socket(
                socket.AF_INET,
                socket.SOCK_STREAM,
            ) as server_sock:
                server_sock.setsockopt(
                    socket.SOL_SOCKET,
                    socket.SO_REUSEADDR,
                    1,
                )
                server_sock.bind((self.host, self.port))
                server_sock.listen()
                server_sock.settimeout(1.0)

                self._send_manager_message({
                    "message_type": "register",
                    "worker_host": self.host,
                    "worker_port": self.port,
                })
                self._serve(server_sock)
        finally:
            self._stop_event.set()
            if self._heartbeat_thread and self._heartbeat_thread.is_alive():
                self._heartbeat_thread.join()

    # ------------------------------------------------------------------ #
    # Networking helpers
    # ------------------------------------------------------------------ #

    def _serve(self, server_sock: socket.socket) -> None:
        """Accept and handle TCP messages from the Manager."""
        serve_tcp(
            server_sock=server_sock,
            stop_event=self._stop_event,
            logger=LOGGER,
            handler=self._handle_message,
        )

    def _handle_message(self, message: dict) -> None:
        """Dispatch a Manager message."""
        message_type = message.get("message_type")
        if message_type == "register_ack":
            self._handle_register_ack()
        elif message_type == "new_map_task":
            self._handle_map_task(message)
        elif message_type == "new_reduce_task":
            self._handle_reduce_task(message)
        elif message_type == "shutdown":
            self._handle_shutdown()
        else:
            LOGGER.debug("Ignoring unsupported message: %s", message_type)

    def _send_manager_message(self, message: dict) -> None:
        """Send a TCP message to the Manager."""
        payload = json.dumps(message).encode("utf-8")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.manager_host, self.manager_port))
            sock.sendall(payload)

    # ------------------------------------------------------------------ #
    # Message handlers
    # ------------------------------------------------------------------ #

    def _handle_register_ack(self) -> None:
        if self._registered_event.is_set():
            return
        self._registered_event.set()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            name="WorkerHeartbeatSender",
            daemon=True,
        )
        self._heartbeat_thread.start()

    def _handle_shutdown(self) -> None:
        self._stop_event.set()

    def _handle_map_task(self, message: dict) -> None:
        try:
            task_id = int(message["task_id"])
            executable = str(message["executable"])
            input_paths = [Path(path) for path in message["input_paths"]]
            output_directory = Path(message["output_directory"])
            num_partitions = int(message["num_partitions"])
        except (KeyError, TypeError, ValueError):
            LOGGER.error("Invalid map task message: %s", message)
            return

        prefix = f"mapreduce-local-task{task_id:05d}-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            tmp_path = Path(tmpdir)
            partition_paths = self._execute_mapper(
                executable=executable,
                input_paths=input_paths,
                temp_dir=tmp_path,
                task_id=task_id,
                num_partitions=num_partitions,
            )
            output_directory.mkdir(parents=True, exist_ok=True)
            self._finalize_map_partitions(
                partition_paths=partition_paths,
                destination=output_directory,
            )

        self._notify_task_finished(task_id)

    def _handle_reduce_task(self, message: dict) -> None:
        try:
            task_id = int(message["task_id"])
            executable = str(message["executable"])
            input_paths = [Path(path) for path in message["input_paths"]]
            output_directory = Path(message["output_directory"])
        except (KeyError, TypeError, ValueError):
            LOGGER.error("Invalid reduce task message: %s", message)
            return

        prefix = f"mapreduce-local-task{task_id:05d}-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            tmp_path = Path(tmpdir)
            temp_output = tmp_path / f"part-{task_id:05d}"
            self._run_reducer(
                executable=executable,
                input_paths=input_paths,
                destination=temp_output,
            )
            output_directory.mkdir(parents=True, exist_ok=True)
            shutil.move(str(temp_output), output_directory / temp_output.name)

        self._notify_task_finished(task_id)

    # ------------------------------------------------------------------ #
    # Task helpers
    # ------------------------------------------------------------------ #

    def _execute_mapper(
        self,
        executable: str,
        input_paths: Iterable[Path],
        temp_dir: Path,
        task_id: int,
        num_partitions: int,
    ) -> List[Path]:
        partition_paths: List[Path] = []
        with contextlib.ExitStack() as stack:
            partition_data: List[tuple[Path, TextIO]] = []
            for partition in range(num_partitions):
                path = temp_dir / (
                    f"maptask{task_id:05d}-part{partition:05d}.unsorted"
                )
                handle = stack.enter_context(
                    path.open("w", encoding="utf-8", newline="")
                )
                partition_data.append((path, handle))
                partition_paths.append(path)
            for input_path in input_paths:
                self._stream_mapper_output(
                    executable=executable,
                    input_path=input_path,
                    partition_data=partition_data,
                    num_partitions=num_partitions,
                )

        return partition_paths

    def _stream_mapper_output(
        self,
        executable: str,
        input_path: Path,
        partition_data: List[tuple[Path, TextIO]],
        num_partitions: int,
    ) -> None:
        if not input_path.exists():
            return
        with open(input_path, "r", encoding="utf-8") as infile:
            with subprocess.Popen(
                [executable],
                stdin=infile,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            ) as process:
                if process.stdout is None:
                    raise RuntimeError("Mapper stdout unavailable")
                for line in process.stdout:
                    key = line.split("\t", 1)[0]
                    partition_index = self._partition_for_key(
                        key,
                        num_partitions,
                    )
                    partition_data[partition_index][1].write(line)
                stderr_output = ""
                if process.stderr:
                    stderr_output = process.stderr.read()
                return_code = process.wait()
                if return_code != 0:
                    message = (
                        "Mapper exited with "
                        f"{return_code}: {stderr_output}"
                    )
                    raise RuntimeError(message)

    CHUNK_SIZE_LINES = 2000

    def _finalize_map_partitions(
        self,
        partition_paths: List[Path],
        destination: Path,
    ) -> None:
        for unsorted_path in partition_paths:
            sorted_path = self._sorted_partition_path(unsorted_path)
            final_name = unsorted_path.name.replace(".unsorted", "")
            final_path = destination / final_name
            shutil.move(str(sorted_path), final_path)
            final_path.touch(exist_ok=True)

    def _sorted_partition_path(self, path: Path) -> Path:
        if self._try_external_sort(path):
            return path
        self._fallback_chunked_sort(path)
        return path

    def _try_external_sort(self, path: Path) -> bool:
        try:
            subprocess.run(
                ["sort", "-o", str(path), str(path)],
                check=True,
                stderr=subprocess.PIPE,
                text=True,
            )
            return True
        except (FileNotFoundError, subprocess.CalledProcessError):
            return False

    def _fallback_chunked_sort(self, path: Path) -> None:
        chunk_paths: List[Path] = []
        try:
            with path.open("r", encoding="utf-8") as infile:
                while True:
                    chunk = list(
                        itertools.islice(
                            infile,
                            self.CHUNK_SIZE_LINES,
                        )
                    )
                    if not chunk:
                        break
                    chunk.sort()
                    temp_file = tempfile.NamedTemporaryFile(
                        mode="w",
                        encoding="utf-8",
                        delete=False,
                        prefix="mapreduce-chunk-",
                        dir=path.parent,
                    )
                    temp_path = Path(temp_file.name)
                    with temp_file:
                        temp_file.writelines(chunk)
                    chunk_paths.append(temp_path)

            if not chunk_paths:
                path.write_text("", encoding="utf-8")
                return

            with contextlib.ExitStack() as stack:
                streams = [
                    stack.enter_context(
                        chunk_path.open("r", encoding="utf-8")
                    )
                    for chunk_path in chunk_paths
                ]
                with path.open("w", encoding="utf-8") as outfile:
                    for line in heapq.merge(*streams):
                        outfile.write(line)
        finally:
            for chunk_path in chunk_paths:
                try:
                    chunk_path.unlink()
                except FileNotFoundError:
                    continue

    def _run_reducer(
        self,
        executable: str,
        input_paths: Iterable[Path],
        destination: Path,
    ) -> None:
        with contextlib.ExitStack() as stack:
            streams = [
                stack.enter_context(path.open("r", encoding="utf-8"))
                for path in input_paths
            ]
            merged_stream = heapq.merge(*streams)
            with destination.open("w", encoding="utf-8") as outfile:
                with subprocess.Popen(
                    [executable],
                    stdin=subprocess.PIPE,
                    stdout=outfile,
                    stderr=subprocess.PIPE,
                    text=True,
                ) as process:
                    if process.stdin is None:
                        raise RuntimeError("Reducer stdin unavailable")
                    for line in merged_stream:
                        process.stdin.write(line)
                    process.stdin.close()
                    stderr_output = ""
                    if process.stderr:
                        stderr_output = process.stderr.read()
                    return_code = process.wait()
                    if return_code != 0:
                        message = (
                            "Reducer exited with "
                            f"{return_code}: {stderr_output}"
                        )
                        raise RuntimeError(message)

    def _notify_task_finished(self, task_id: int) -> None:
        self._send_manager_message({
            "message_type": "finished",
            "task_id": task_id,
            "worker_host": self.host,
            "worker_port": self.port,
        })

    @staticmethod
    def _partition_for_key(key: str, num_partitions: int) -> int:
        digest = hashlib.md5(key.encode("utf-8")).hexdigest()
        return int(digest, 16) % num_partitions

    # ------------------------------------------------------------------ #
    # Heartbeat handling
    # ------------------------------------------------------------------ #

    def _heartbeat_loop(self) -> None:
        """Send UDP heartbeat messages to the Manager."""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp_sock:
            try:
                udp_sock.connect(
                    (self.manager_host, self.manager_port),
                )
            except OSError:
                return
            heartbeat = json.dumps(
                {
                    "message_type": "heartbeat",
                    "worker_host": self.host,
                    "worker_port": self.port,
                }
            ).encode("utf-8")
            while not self._stop_event.is_set():
                try:
                    udp_sock.sendall(heartbeat)
                except OSError:
                    break
                if self._stop_event.wait(HEARTBEAT_INTERVAL_SECS):
                    break


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6001)
@click.option("--manager-host", "manager_host", default="localhost")
@click.option("--manager-port", "manager_port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
def main(host, port, manager_host, manager_port, logfile, loglevel):
    """Run Worker."""
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(f"Worker:{port} [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Worker(host, port, manager_host, manager_port)


if __name__ == "__main__":
    main()
