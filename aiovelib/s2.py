import time
import math
import logging
import asyncio
from enum import Enum
from dataclasses import dataclass
from typing import AsyncIterator, Optional

try:
    import dbus_fast
except ImportError:
    from dbus_next.service import method, signal
else:
    from dbus_fast.service import method, signal

from aiovelib.service import Item


logger = logging.getLogger(__name__)


IFACE="com.victronenergy.S2"


class S2ConnEventType(int, Enum):
    CONNECTED = 0
    DISCONNECTED = 1
    UNEXPECTED_RECONNECT = 2


@dataclass(frozen=True)
class S2ConnEvent:
    type: S2ConnEventType
    client_id: Optional[str]
    reason: Optional[str] = None


class S2ServerItem(Item):
    def __init__(self, path):
        super().__init__(path)
        self.name = IFACE
        self._runningloop = asyncio.get_running_loop()

        self._conn_events: asyncio.Queue[S2ConnEvent] = asyncio.Queue()
        self._keepalive_task: asyncio.Task | None = None
        self._s2_dbus_disconnect_event: asyncio.Event | None = None

        self.client_id = None
        self.keep_alive_interval = None
        self.keep_alive_leeway = None
        self.last_seen = None

    @property
    def is_connected(self):
        return self.client_id is not None

    def _emit(self, ev: S2ConnEvent) -> None:
        # non-blocking; queue is unbounded by default
        self._conn_events.put_nowait(ev)

    async def connection_events(self) -> AsyncIterator[S2ConnEvent]:
        """Consumer: `async for ev in item.connection_events(): ...`"""
        while True:
            yield await self._conn_events.get()

    async def wait_connected(self) -> S2ConnEvent:
        """One-shot helper."""
        while True:
            ev = await self._conn_events.get()
            if ev.type is S2ConnEventType.CONNECTED:
                return ev

    async def wait_disconnected(self) -> S2ConnEvent:
        """One-shot helper."""
        while True:
            ev = await self._conn_events.get()
            if ev.type is S2ConnEventType.DISCONNECTED:
                return ev

    async def _create_connection(self, client_id: str, keep_alive_interval: int):
        self.client_id = client_id
        self.keep_alive_interval = keep_alive_interval
        self.keep_alive_leeway = int(math.ceil(0.2 * keep_alive_interval))
        self.last_seen = time.time()

        self._s2_dbus_disconnect_event = asyncio.Event()
        self._keepalive_task = self._runningloop.create_task(self._monitor_keep_alive())

        self._emit(S2ConnEvent(S2ConnEventType.CONNECTED, client_id))

    async def _destroy_connection(self, reason: str = "client disconnected"):
        if self._s2_dbus_disconnect_event:
            if self._s2_dbus_disconnect_event.is_set():
                return
            self._s2_dbus_disconnect_event.set()

        t = self._keepalive_task
        if t and not t.done():
            t.cancel()
            if asyncio.current_task() is not t:
                try:
                    await t
                except asyncio.CancelledError:
                    pass

        old_client = self.client_id
        self.client_id = None
        self.keep_alive_interval = None
        self.last_seen = None

        self._emit(S2ConnEvent(S2ConnEventType.DISCONNECTED, old_client, reason))

    async def _monitor_keep_alive(self):
        diff = self.keep_alive_interval + self.keep_alive_leeway
        while self._s2_dbus_disconnect_event and not self._s2_dbus_disconnect_event.is_set():
            await asyncio.sleep(self.keep_alive_interval)

            if self.last_seen and time.time() - self.last_seen > diff:
                logger.warning(f"{ self.client_id } missed KeepAlive")
                self._send_disconnect("KeepAlive missed")
                await self._destroy_connection("KeepAlive missed")

    async def _on_s2_message(self, message):
        logger.info(f"Received S2 message: {message}")

    @method('Discover')
    async def _on_discover(self) -> 'b':
        """
        Used by the CEM to check if the S2 interface is available on this path
        """
        return True

    @method('Connect')
    async def _on_connect(self, client_id: 's', keep_alive_interval: 'i') -> 'b':
        if self.is_connected:
            if self.client_id == client_id:
                self._emit(S2ConnEvent(S2ConnEventType.UNEXPECTED_RECONNECT, client_id))
                return True
            else:
                return False
        await self._create_connection(client_id, keep_alive_interval)
        return True

    @method('Disconnect')
    async def _on_disconnect(self, client_id: 's'):
        if client_id != self.client_id:
            self._send_disconnect('Not connected', client_id)
            return
        await self._destroy_connection("client requested disconnect")

    @method('Message')
    async def _on_message(self, client_id: 's', message: 's'):
        if client_id != self.client_id:
            self._send_disconnect('Not connected', client_id)
            return
        try:
            logger.debug(f"S2 IN:  {message}")
            await self._on_s2_message(message)
        except Exception as e:
            logger.exception(e)
            raise

    @method('KeepAlive')
    async def _on_keep_alive(self, client_id: 's') -> 'b':
        if client_id != self.client_id:
            self._send_disconnect('Not connected', client_id)
            return False

        self.last_seen = time.time()
        return True

    @signal('Message')
    def _send_message(self, message: str) -> 'ss':
        if not self.is_connected:
            raise Exception("No client connected")
        logger.debug(f"S2 OUT: {message}")
        return [self.client_id, message]

    @signal('Disconnect')
    def _send_disconnect(self, reason: str, client_id=None) -> 'ss':
        return [client_id if client_id else self.client_id, reason]
