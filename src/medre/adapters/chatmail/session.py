"""Own the official blocking client, native process and coalesced event pump."""

from __future__ import annotations

import asyncio
import contextlib
import shutil
import subprocess
import sys
import threading
from importlib.metadata import version
from pathlib import Path
from typing import Any, Callable

from .config import ChatmailConfig
from .state import ProfileLease


class SessionError(RuntimeError):
    """Native errors are deliberately secret-free at the provider boundary."""


class RpcCallError(SessionError):
    """The running native server rejected a request with a valid RPC response."""


def server_path(configured: str = "") -> str:
    if configured:
        return configured
    adjacent = Path(sys.executable).parent / (
        "deltachat-rpc-server.exe"
        if sys.platform == "win32"
        else "deltachat-rpc-server"
    )
    if adjacent.is_file():
        return str(adjacent)
    result = shutil.which("deltachat-rpc-server")
    if result is None:
        raise SessionError(
            "Chatmail RPC server is missing; reinstall medre-adapter-chatmail"
        )
    return result


def build_rpc(accounts_dir: str, executable: str, notify: Callable[[], None]) -> Any:
    from deltachat_rpc_client import Rpc
    from deltachat_rpc_client.rpc import JsonRpcError

    class BridgeRpc(Rpc):
        error_type = JsonRpcError

        def events_loop(self) -> None:
            # Account queues in the official client are unbounded. Native
            # messages already live in its database, so retain only a wakeup.
            try:
                while events := self.get_next_event_batch():
                    if self.closing:
                        return
                    if _should_wake(events):
                        notify()
            except Exception:
                if not self.closing:
                    notify()

    # PIPE stderr is never drained by the SDK and can block the server.
    # The wrapper sanitizes startup errors and owns teardown even when the
    # SDK's startup-error formatter assumes a readable stderr pipe.
    return BridgeRpc(
        accounts_dir, rpc_server_path=executable, stderr=subprocess.DEVNULL
    )


# Native 2.63.0 batches include records with an ``event.kind`` tag.
# Ignore only recognized diagnostic/idle events: unknown event types must wake
# the scanner so new SDK events cannot cause silent message loss.
_IDLE_EVENTS = frozenset(
    {
        "Info",
        "Warning",
        "Error",
        "ImapInboxIdle",
    }
)


def _should_wake(events: object) -> bool:
    if not isinstance(events, list):
        return True
    for item in events:
        if not isinstance(item, dict):
            return True
        event = item.get("event")
        if not isinstance(event, dict) or event.get("kind") not in _IDLE_EVENTS:
            return True
    return False


class ChatmailSession:
    def __init__(
        self, config: ChatmailConfig, *, rpc_factory: Callable = build_rpc
    ) -> None:
        self.config = config
        self.rpc_factory = rpc_factory
        self.rpc = None
        self.lease = None
        self.wake = asyncio.Event()
        self._scheduled = threading.Event()
        self._lock = asyncio.Lock()
        self._closing = False
        self._used = False
        self._active: set[asyncio.Task] = set()

    async def start(self) -> None:
        if self._used:
            raise RuntimeError("Chatmail sessions cannot be reused")
        self._used = True
        loop = asyncio.get_running_loop()
        self._closing = False

        def notify() -> None:
            if not self._closing and not self._scheduled.is_set():
                self._scheduled.set()
                with contextlib.suppress(RuntimeError):
                    loop.call_soon_threadsafe(self._notify)

        try:
            # Non-waiting OS lock; acquire before creating an off-thread
            # process so cancellation cannot orphan a profile lease.
            self.lease = ProfileLease(self.config.accounts_dir)
            executable = await asyncio.to_thread(
                server_path, self.config.rpc_server_path
            )
            self.rpc = self.rpc_factory(self.config.accounts_dir, executable, notify)
            await self._run(self.rpc.start)
            info = await self.call("get_system_info")
            expected = version("deltachat-rpc-client")
            if (
                version("deltachat-rpc-server") != expected
                or info.get("deltachat_core_version") != f"v{expected}"
            ):
                raise SessionError(
                    "Chatmail RPC client and native server versions must match"
                )
        except BaseException:
            await self.stop()
            raise

    def _notify(self) -> None:
        self._scheduled.clear()
        if not self._closing:
            self.wake.set()

    def _kill(self) -> None:
        # Rpc.__getattr__ synthesizes methods for every missing name.
        # Native process/thread attributes must be read from its instance.
        process = vars(self.rpc).get("process") if self.rpc is not None else None
        if process is not None and process.poll() is None:
            with contextlib.suppress(ProcessLookupError):
                process.kill()

    async def _run(self, function: Callable, *args: Any) -> Any:
        def invoke() -> Any:
            try:
                return function(*args)
            finally:
                # A startup completing after cancellation must not leave an
                # unowned native process alive.
                if self._closing:
                    self._kill()

        task = asyncio.create_task(asyncio.to_thread(invoke))
        self._active.add(task)
        task.add_done_callback(self._finished)
        try:
            return await asyncio.wait_for(
                asyncio.shield(task), self.config.rpc_timeout_seconds
            )
        except asyncio.CancelledError:
            self._closing = True
            self._kill()
            raise
        except Exception as exc:
            error_type = getattr(self.rpc, "error_type", ())
            if isinstance(exc, error_type) and self.alive():
                raise RpcCallError("Chatmail RPC request was rejected") from None
            self._closing = True
            self._kill()
            raise SessionError(
                "Chatmail RPC operation failed; inspect the native profile"
            ) from None

    def _finished(self, task: asyncio.Task) -> None:
        self._active.discard(task)
        if not task.cancelled():
            task.exception()

    async def call(self, method: str, *args: Any) -> Any:
        async with self._lock:
            if self.rpc is None or self._closing:
                raise SessionError("Chatmail RPC session is unavailable")
            return await self._run(getattr(self.rpc, method), *args)

    def alive(self) -> bool:
        attributes = vars(self.rpc) if self.rpc is not None else {}
        process = attributes.get("process")
        events = attributes.get("events_thread")
        return bool(
            process is not None
            and process.poll() is None
            and events is not None
            and events.is_alive()
        )

    async def stop(self, timeout: float = 5.0) -> None:
        self._closing = True
        rpc = self.rpc
        if rpc is not None:
            close = asyncio.create_task(asyncio.to_thread(self._close, rpc))
            try:
                await asyncio.wait_for(asyncio.shield(close), max(timeout, 0.01))
            except TimeoutError:
                self._kill()
                await asyncio.wait_for(asyncio.shield(close), 2.0)
            except asyncio.CancelledError:
                self._kill()
                await asyncio.wait_for(asyncio.shield(close), 2.0)
                raise
            finally:
                self._kill()
                if self._active:
                    await asyncio.wait_for(
                        asyncio.gather(*self._active, return_exceptions=True), 2.0
                    )
                self.rpc = None
        if self.lease is not None:
            lease, self.lease = self.lease, None
            await asyncio.to_thread(lease.close)

    @staticmethod
    def _close(rpc: Any) -> None:
        process = vars(rpc).get("process")
        if process is None:
            return
        with contextlib.suppress(Exception):
            rpc.close()
        if process.poll() is None:
            process.kill()
        process.wait(timeout=2.0)
        for name in ("stdin", "stdout", "stderr"):
            stream = getattr(process, name, None)
            if stream is not None:
                with contextlib.suppress(OSError):
                    stream.close()
