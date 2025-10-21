"""Networking helpers shared by Manager and Worker."""
from __future__ import annotations

import json
import logging
import socket
from typing import Callable, Optional


LOGGER = logging.getLogger(__name__)


def serve_tcp(
    server_sock: socket.socket,
    stop_event,
    logger: logging.Logger,
    handler: Callable[[dict], None],
) -> None:
    """Accept connections and invoke handler for each decoded JSON message."""
    while not stop_event.is_set():
        try:
            client_sock, _ = server_sock.accept()
        except socket.timeout:
            continue
        except OSError:
            break
        with client_sock:
            message = _receive_json(client_sock, logger)
            if message is None:
                continue
            logger.debug("TCP recv\n%s", json.dumps(message, indent=2))
            handler(message)


def _receive_json(
    client_sock: socket.socket,
    logger: logging.Logger,
) -> Optional[dict]:
    data = bytearray()
    while True:
        chunk = client_sock.recv(4096)
        if not chunk:
            break
        data.extend(chunk)
    if not data:
        return None
    try:
        return json.loads(data.decode("utf-8"))
    except json.JSONDecodeError:
        logger.debug("Ignoring invalid JSON message: %s", data)
        return None
