import time
import math
import inspect
import logging
import asyncio
from enum import Enum
from dataclasses import dataclass
from typing import AsyncGenerator, AsyncIterator, Awaitable, Callable, Optional, List, Union

try:
    import dbus_fast
except ImportError:
    from dbus_next.service import method, signal
else:
    from dbus_fast.service import method, signal

from s2python.common import (
    ReceptionStatus,
)
from s2python.message import S2MessageWithID
from s2python.connection.asset_details import AssetDetails
from s2python.connection.async_ import S2AsyncConnection
from s2python.connection.async_.medium.s2_medium import (
    MediumClosedConnectionError,
    S2AsyncMediumConnection,
    UnparsedMediumData,
)
from s2python.connection.async_.control_type.class_based import S2ControlType, ResourceManagerHandler

from aiovelib.service import TextItem


logger = logging.getLogger(__name__)


IFACE="com.victronenergy.S2"


class S2ConnEventType(int, Enum):
    NOT_READY = 0
    READY = 1
    CONNECTED = 2
    DISCONNECTED = 3
    UNEXPECTED_RECONNECT = 4


@dataclass(frozen=True)
class S2ConnEvent:
    type: S2ConnEventType
    client_id: Optional[str]
    reason: Optional[str] = None


class S2ServerItem(TextItem):
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
    def is_ready(self):
        return self.value is not None

    @is_ready.setter
    def is_ready(self, v: bool):
        if v and self.value is not None:
            return  # don't override an existing value
        self.set_local_value("Ready" if v else None)
        event_type = S2ConnEventType.READY if v else S2ConnEventType.NOT_READY
        self._emit(S2ConnEvent(event_type, None))

    @property
    def is_connected(self):
        return self.client_id is not None

    async def set_ready(self, ready: bool = True):
        if not ready and self.is_connected:
            await self._destroy_connection("Not ready")
        self.is_ready = ready

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
        self.set_local_value(f"CEM: {client_id}")

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
        self.set_local_value("Ready")

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
        return self.is_ready

    @method('Connect')
    async def _on_connect(self, client_id: 's', keep_alive_interval: 'i') -> 'b':
        if not self.is_ready:
            self._send_disconnect('Not ready', client_id)
            return False
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
        if not self.is_ready:
            self._send_disconnect('Not ready', client_id)
            return
        if client_id != self.client_id:
            self._send_disconnect('Not connected', client_id)
            return
        await self._destroy_connection("client requested disconnect")

    @method('Message')
    async def _on_message(self, client_id: 's', message: 's'):
        if not self.is_ready:
            self._send_disconnect('Not ready', client_id)
            return
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
        if not self.is_ready:
            self._send_disconnect('Not ready', client_id)
            return False
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


SendMessageCallback = Callable[[str, str], Union[None, Awaitable[None]]]


class DBusServerMedium(S2AsyncMediumConnection):
    _client_id: Optional[str]
    _closed: bool
    _incoming_messages: asyncio.Queue[Optional[UnparsedMediumData]]
    _send_message: SendMessageCallback

    def __init__(self, send_message: SendMessageCallback) -> None:
        self._client_id = None
        self._closed = True
        self._incoming_messages = asyncio.Queue()
        self._send_message = send_message

    async def connect(self, client_id: str) -> None:
        if self._client_id is not None and self._client_id != client_id:
            raise RuntimeError(
                "A D-Bus client is already connected. Disconnect first before switching clients."
            )

        self._client_id = client_id
        self._closed = False

    async def disconnect(self) -> None:
        self._closed = True
        self._client_id = None
        self._incoming_messages.put_nowait(None)

    async def receive_message(self, client_id: str, message: UnparsedMediumData) -> None:
        if self._closed or self._client_id is None:
            raise MediumClosedConnectionError("Cannot receive message when D-Bus medium is closed.")

        if client_id != self._client_id:
            raise MediumClosedConnectionError(
                f"Received message for client '{client_id}' but active client is '{self._client_id}'."
            )

        await self._incoming_messages.put(message)

    async def is_connected(self) -> bool:
        return not self._closed and self._client_id is not None

    async def messages(self) -> AsyncGenerator[UnparsedMediumData, None]:
        if self._closed:
            raise MediumClosedConnectionError("D-Bus medium is closed.")

        while True:
            msg = await self._incoming_messages.get()
            if msg is None:
                raise MediumClosedConnectionError("D-Bus medium was disconnected.")
            yield msg

    async def send(self, message: str) -> None:
        if self._closed or self._client_id is None:
            raise MediumClosedConnectionError("Cannot send message when D-Bus medium is closed.")

        result = self._send_message(self._client_id, message)
        if inspect.isawaitable(result):
            await result


class S2ResourceManagerItem(S2ServerItem):
    control_types: List[S2ControlType]
    asset_details: AssetDetails

    def __init__(
        self,
        path,
        control_types: List[S2ControlType] | None = None,
        asset_details: AssetDetails | None = None,
    ):
        super().__init__(path)

        self.control_types = list(control_types) if control_types is not None else []
        self.asset_details = asset_details

        self._dbus_medium = DBusServerMedium(send_message=self._send_to_client)
        self._s2_conn = S2AsyncConnection(self._dbus_medium)
        self._rm_handler = ResourceManagerHandler(
            control_types=self.control_types,
            asset_details=self.asset_details,
        )
        self._rm_handler.register_handlers(self._s2_conn)

        self._main_task: asyncio.Task | None = None

    async def _send_to_client(self, client_id: str, message: str) -> None:
        if self.client_id != client_id:
            logger.warning(
                "S2 message intended for client %s but active client is %s",
                client_id,
                self.client_id,
            )
        self._send_message(message)

    async def set_ready(
        self,
        ready: bool,
        control_types: List[S2ControlType] | None = None,
        asset_details: AssetDetails | None = None,
    ):
        if control_types is not None:
            self.control_types[:] = control_types
        if asset_details is not None:
            self.asset_details = asset_details

        # Keep the handler metadata in sync for the next handshake cycle.
        self._rm_handler.asset_details = self.asset_details
        for control_type in self.control_types:
            control_type.set_asset_details(self.asset_details)

        await super().set_ready(ready)

    async def close(self) -> None:
        await self.set_ready(False)
        await self._stop_s2_session()

    async def _stop_s2_session(self) -> None:
        await self._dbus_medium.disconnect()

        task = self._main_task
        self._main_task = None
        if task is not None and not task.done() and asyncio.current_task() is not task:
            await task

    async def update_resource_manager_details(
        self,
        control_types: List[S2ControlType] | None = None,
        asset_details: AssetDetails | None = None,
    ) -> ReceptionStatus | None:
        if control_types is not None:
            self.control_types[:] = control_types
        if asset_details is not None:
            self.asset_details = asset_details

        self._rm_handler.asset_details = self.asset_details
        for control_type in self.control_types:
            control_type.set_asset_details(self.asset_details)

        if not self.is_connected:
            return None

        return await self._s2_conn.send_msg_and_await_reception_status(
            self.asset_details.to_resource_manager_details(self.control_types)
        )

    async def _create_connection(self, client_id: str, keep_alive_interval: int):
        await super()._create_connection(client_id, keep_alive_interval)
        await self._dbus_medium.connect(client_id)
        if self._main_task is None or self._main_task.done():
            self._main_task = self._runningloop.create_task(self._s2_conn.run())

    async def _destroy_connection(self, reason: str = "client disconnected"):
        await self._stop_s2_session()
        await super()._destroy_connection(reason)

    async def _on_s2_message(self, message):
        if self.client_id is None:
            raise RuntimeError("No client connected.")
        await self._dbus_medium.receive_message(self.client_id, message)

    async def send_msg_and_await_reception_status(
        self,
        s2_msg: S2MessageWithID,
        timeout_reception_status: float = 5.0,
        raise_on_error: bool = True,
    ) -> ReceptionStatus:
        return await self._s2_conn.send_msg_and_await_reception_status(
            s2_msg=s2_msg,
            timeout_reception_status=timeout_reception_status,
            raise_on_error=raise_on_error,
        )

    async def send_resource_manager_details(
        self,
        control_types: List[S2ControlType] | None = None,
        asset_details: AssetDetails | None = None,
    ) -> ReceptionStatus:
        reception_status = await self.update_resource_manager_details(
            control_types=control_types,
            asset_details=asset_details,
        )
        if reception_status is None:
            raise RuntimeError("Cannot send ResourceManagerDetails if no client is connected.")
        return reception_status
