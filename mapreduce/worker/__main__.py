"""MapReduce framework Worker node."""
from __future__ import annotations

import hashlib
import json
import logging
import os
import socket
import subprocess
import tempfile
import threading
from pathlib import Path
from typing import Iterable, List, Optional

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
            raw_outputs = self._run_mapper_commands(
                executable, input_paths, tmp_path
            )
            sorted_output = tmp_path / "sorted-mapper-output"
            self._sort_files(raw_outputs, sorted_output)
            self._write_map_partitions(
                sorted_output,
                output_directory,
                task_id,
                num_partitions,
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
            sorted_input = tmp_path / "sorted-reducer-input"
            input_strings = [str(path) for path in input_paths]
            self._sort_files(input_strings, sorted_input)
            self._run_reducer_command(
                executable,
                sorted_input,
                output_directory / f"part-{task_id:05d}",
            )

        self._notify_task_finished(task_id)

    # ------------------------------------------------------------------ #
    # Task helpers
    # ------------------------------------------------------------------ #

    def _run_mapper_commands(
        self,
        executable: str,
        input_paths: Iterable[Path],
        temp_dir: Path,
    ) -> List[str]:
        """Run mapper executable for each input path and capture outputs."""
        raw_outputs: List[str] = []
        for index, input_path in enumerate(input_paths):
            raw_path = temp_dir / f"mapper-{index:05d}.out"
            raw_outputs.append(str(raw_path))
            if not input_path.exists():
                raw_path.touch()
                continue
            with open(input_path, "rb") as infile, open(
                raw_path,
                "wb",
            ) as outfile:
                with subprocess.Popen(
                    [executable],
                    stdin=infile,
                    stdout=outfile,
                    stderr=subprocess.PIPE,
                ) as process:
                    _, stderr = process.communicate()
                    if process.returncode != 0:
                        error_msg = stderr.decode()
                        message = (
                            "Mapper exited with "
                            f"{process.returncode}: {error_msg}"
                        )
                        raise RuntimeError(message)
        if not raw_outputs:
            empty_file = temp_dir / "mapper-empty.out"
            empty_file.touch()
            raw_outputs.append(str(empty_file))
        return raw_outputs

    def _sort_files(self, inputs: Iterable[str], destination: Path) -> None:
        """Sort concatenated files using external sort."""
        with open(destination, "wb") as outfile:
            with subprocess.Popen(
                ["sort", *inputs],
                stdout=outfile,
                stderr=subprocess.PIPE,
                env=self._sort_env(),
            ) as process:
                _, stderr = process.communicate()
                if process.returncode != 0:
                    error_msg = stderr.decode()
                    raise RuntimeError(
                        f"sort exited with {process.returncode}: {error_msg}"
                    )

    def _write_map_partitions(
        self,
        sorted_output: Path,
        output_directory: Path,
        task_id: int,
        num_partitions: int,
    ) -> None:
        """Partition sorted mapper output into num_partitions files."""
        if num_partitions <= 0:
            raise ValueError("num_partitions must be > 0")

        output_directory.mkdir(parents=True, exist_ok=True)
        partition_paths = [
            output_directory / (
                f"maptask{task_id:05d}-part{partition:05d}"
            )
            for partition in range(num_partitions)
        ]
        partition_files = [
            path.open("w", encoding="utf-8", newline="")
            for path in partition_paths
        ]
        try:
            with sorted_output.open("r", encoding="utf-8") as infile:
                for line in infile:
                    key = line.split("\t", 1)[0]
                    partition_index = self._partition_for_key(
                        key,
                        num_partitions,
                    )
                    partition_files[partition_index].write(line)
        finally:
            for handle in partition_files:
                handle.close()

        # Ensure partition files exist even if no data was written.
        for path in partition_paths:
            path.touch(exist_ok=True)

    def _run_reducer_command(
        self,
        executable: str,
        sorted_input: Path,
        destination: Path,
    ) -> None:
        """Run reducer executable against sorted input."""
        destination.parent.mkdir(parents=True, exist_ok=True)
        with open(sorted_input, "rb") as infile, open(
            destination,
            "wb",
        ) as outfile:
            with subprocess.Popen(
                [executable],
                stdin=infile,
                stdout=outfile,
                stderr=subprocess.PIPE,
            ) as process:
                _, stderr = process.communicate()
                if process.returncode != 0:
                    error_msg = stderr.decode()
                    message = (
                        "Reducer exited with "
                        f"{process.returncode}: {error_msg}"
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

    @staticmethod
    def _sort_env() -> dict:
        env = dict(os.environ)
        env.setdefault("LC_ALL", "C")
        return env

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
