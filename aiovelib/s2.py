import time
import math
import logging
import asyncio
from enum import Enum
from dataclasses import dataclass
from typing import AsyncGenerator, AsyncIterator, List, Optional

try:
    import dbus_fast
except ImportError:
    from dbus_next.service import method, signal
else:
    from dbus_fast.service import method, signal

# Note: only import from module paths that do not require the optional
# websockets dependency of s2python (available since s2python v0.11.0).
from s2python.common import EnergyManagementRole, ReceptionStatus
from s2python.message import S2Message
from s2python.reception_status_awaiter import ReceptionStatusAwaiter
from s2python.connection.asset_details import AssetDetails
from s2python.connection.async_.connection import S2AsyncConnection
from s2python.connection.async_.medium.s2_medium import (
    MediumClosedConnectionError,
    S2AsyncMediumConnection,
    UnparsedMediumData,
)
from s2python.connection.async_.control_type.class_based import (
    ResourceManagerHandler,
    S2ControlType,
    FRBCControlType,
    PPBCControlType,
    OMBCControlType,
    PEBCControlType,
    DDBCControlType,
    NoControlControlType,
)

from aiovelib.service import TextItem

__all__ = [
    # aiovelib S2-over-D-Bus API
    "S2ConnEventType",
    "S2ConnEvent",
    "S2ServerItem",
    "S2ResourceManagerItem",
    "DbusMediumConnection",
    "DynamicResourceManagerHandler",
    # re-exported s2python API, so that users of aiovelib.s2 do not need to
    # track s2python module paths themselves
    "AssetDetails",
    "ReceptionStatusAwaiter",
    "S2AsyncConnection",
    "ResourceManagerHandler",
    "S2ControlType",
    "FRBCControlType",
    "PPBCControlType",
    "OMBCControlType",
    "PEBCControlType",
    "DDBCControlType",
    "NoControlControlType",
]


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



class DbusMediumConnection(S2AsyncMediumConnection):
    """
    S2 transport medium over the Victron D-Bus S2 interface.

    Incoming messages are fed by the owning S2ServerItem from the D-Bus
    `Message` method, outgoing messages are emitted as the D-Bus `Message`
    signal. One medium instance represents a single CEM connection; it is
    closed for good when the CEM disconnects.
    """

    def __init__(self, item: "S2ResourceManagerItem") -> None:
        self._item = item
        self._incoming: asyncio.Queue[Optional[str]] = asyncio.Queue()
        self._closed = False

    async def is_connected(self) -> bool:
        return not self._closed and self._item.is_connected

    async def messages(self) -> AsyncGenerator[UnparsedMediumData, None]:
        while True:
            message = await self._incoming.get()
            if message is None:  # close sentinel
                return
            yield message

    async def send(self, message: str) -> None:
        if self._closed or not self._item.is_connected:
            raise MediumClosedConnectionError(
                "Cannot send messages if client connection is not yet established."
            )
        self._item._send_message(message)

    def feed(self, message: str) -> None:
        """Feed an incoming S2 message received over D-Bus."""
        if not self._closed:
            self._incoming.put_nowait(message)

    def close(self) -> None:
        """Close the medium, ending the message generator."""
        if not self._closed:
            self._closed = True
            self._incoming.put_nowait(None)


class DynamicResourceManagerHandler(ResourceManagerHandler):
    """
    The upstream ResourceManagerHandler fixes its control types and asset
    details at construction time. This variant reads them live from the
    owning S2ResourceManagerItem on every use, so they can be updated at
    runtime (see S2ResourceManagerItem.send_resource_manager_details).
    """

    # pylint: disable=super-init-not-called
    def __init__(self, item: "S2ResourceManagerItem") -> None:
        self._item = item
        self._current_control_type: Optional[S2ControlType] = None

    @property
    def asset_details(self) -> Optional[AssetDetails]:
        return self._item.asset_details

    @property
    def _control_types(self) -> List[S2ControlType]:
        return list(self._item.control_types or [])

    @property
    def current_control_type(self) -> Optional[S2ControlType]:
        return self._current_control_type


class S2ResourceManagerItem(S2ServerItem):
    """
    S2 resource manager exposed over D-Bus.

    The S2 protocol state (handshake, reception statuses, control type
    selection and message dispatch) is handled by s2python's
    S2AsyncConnection; this item provides the D-Bus medium for it and the
    service-facing API. A fresh S2AsyncConnection is created for every CEM
    connection.
    """

    role: EnergyManagementRole
    control_types: Optional[List[S2ControlType]]
    asset_details: Optional[AssetDetails]

    def __init__(self, path,
                 control_types: Optional[List[S2ControlType]] = None,
                 asset_details: Optional[AssetDetails] = None):
        super().__init__(path)

        self.role = EnergyManagementRole.RM
        self.control_types = control_types
        self.asset_details = asset_details
        self._apply_asset_details()

        self._connection: Optional[S2AsyncConnection] = None
        self._medium: Optional[DbusMediumConnection] = None
        self._rm_handler: Optional[DynamicResourceManagerHandler] = None
        self._main_task: Optional[asyncio.Task] = None

        # Setting this event tears the current S2 connection down and returns
        # the item to the ready state, so the CEM can connect again.
        self._restart_connection_event = asyncio.Event()

    @property
    def connection(self) -> Optional[S2AsyncConnection]:
        """The S2 protocol connection of the currently connected CEM, if any."""
        return self._connection

    @property
    def current_control_type(self) -> Optional[S2ControlType]:
        handler = self._rm_handler
        return handler.current_control_type if handler else None

    def _apply_asset_details(self) -> None:
        if self.asset_details is None:
            return
        for control_type in self.control_types or []:
            control_type.set_asset_details(self.asset_details)

    async def set_ready(self, ready: bool,
                        control_types: Optional[List[S2ControlType]] = None,
                        asset_details: Optional[AssetDetails] = None):
        if control_types is not None:
            self.control_types = control_types
        if asset_details is not None:
            self.asset_details = asset_details
        self._apply_asset_details()
        await super().set_ready(ready)

    async def close(self) -> None:
        # Set not ready and destroy an existing connection
        await self.set_ready(False)
        self._restart_connection_event.set()

        # The main task unwinds on its own once the connection is destroyed;
        # cancellation is only a fallback.
        task = self._main_task
        if task is not None and not task.done():
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=5)
            except asyncio.TimeoutError:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("Error while closing S2ResourceManagerItem")

        self._main_task = None

    async def _create_connection(self, client_id: str, keep_alive_interval: int):
        await super()._create_connection(client_id, keep_alive_interval)

        self._restart_connection_event = asyncio.Event()
        self._medium = DbusMediumConnection(self)
        self._connection = S2AsyncConnection(
            medium=self._medium, eventloop=self._runningloop)
        self._rm_handler = DynamicResourceManagerHandler(self)
        self._rm_handler.register_handlers(self._connection)
        self._main_task = self._runningloop.create_task(self._run_connection())

    async def _destroy_connection(self, reason: str = "client disconnected"):
        connection = self._connection
        medium = self._medium
        if connection is not None:
            await connection.stop()
        if medium is not None:
            medium.close()
        await super()._destroy_connection(reason)

    async def _run_connection(self) -> None:
        connection = self._connection
        medium = self._medium
        restart_event = self._restart_connection_event

        logger.debug("Connecting as S2 resource manager over D-Bus")

        run_task = self._runningloop.create_task(connection.run())
        restart_task = self._runningloop.create_task(restart_event.wait())
        try:
            await asyncio.wait(
                (run_task, restart_task), return_when=asyncio.FIRST_COMPLETED)
        finally:
            await connection.stop()
            medium.close()
            restart_task.cancel()
            for task in (run_task, restart_task):
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    logger.exception("S2 connection terminated with an error")

            if self._connection is connection:
                self._connection = None
                self._medium = None
                self._rm_handler = None

            await self._destroy_connection("connection stopped")
            logger.debug("Finished S2 connection eventloop.")

    async def _on_s2_message(self, message):
        medium = self._medium
        if medium is None:
            logger.warning("Received S2 message without an active connection, ignoring")
            return
        medium.feed(message)

    def _require_connection(self) -> S2AsyncConnection:
        connection = self._connection
        if connection is None or not self.is_connected:
            raise RuntimeError(
                "Cannot send messages if client connection is not yet established."
            )
        return connection

    async def send_msg_and_await_reception_status(
        self,
        s2_msg: S2Message,
        timeout_reception_status: float = 5.0,
        raise_on_error: bool = True,
    ) -> ReceptionStatus:
        return await self._require_connection().send_msg_and_await_reception_status(
            s2_msg, timeout_reception_status, raise_on_error
        )

    def send_msg_and_await_reception_status_sync(
        self,
        s2_msg: S2Message,
        timeout_reception_status: float = 5.0,
        raise_on_error: bool = True,
    ) -> ReceptionStatus:
        return asyncio.run_coroutine_threadsafe(
            self.send_msg_and_await_reception_status(
                s2_msg, timeout_reception_status, raise_on_error
            ),
            self._runningloop,
        ).result()

    async def send_msg_and_forget(self, s2_msg: S2Message) -> None:
        await self._require_connection().send_and_forget(s2_msg)

    async def send_resource_manager_details(
        self,
        control_types: Optional[List[S2ControlType]] = None,
        asset_details: Optional[AssetDetails] = None
    ) -> ReceptionStatus:
        if control_types is not None:
            self.control_types = control_types
        if asset_details is not None:
            self.asset_details = asset_details
        self._apply_asset_details()
        return await self.send_msg_and_await_reception_status(
            self.asset_details.to_resource_manager_details(self.control_types)
        )

    def send_resource_manager_details_sync(
        self,
        control_types: Optional[List[S2ControlType]] = None,
        asset_details: Optional[AssetDetails] = None
    ) -> ReceptionStatus:
        return asyncio.run_coroutine_threadsafe(
            self.send_resource_manager_details(
                control_types, asset_details
            ),
            self._runningloop,
        ).result()
