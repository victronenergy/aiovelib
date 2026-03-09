import time
import math
import json
import uuid
import logging
import asyncio
from enum import Enum
from dataclasses import dataclass
from typing import AsyncIterator, Optional, Dict, List, Awaitable

try:
    import dbus_fast
except ImportError:
    from dbus_next.service import method, signal
else:
    from dbus_fast.service import method, signal

from s2python.common import (
    ReceptionStatusValues,
    ReceptionStatus,
    Handshake,
    EnergyManagementRole,
    HandshakeResponse,
    SelectControlType,
)
from s2python.s2_control_type import S2ControlType
from s2python.s2_parser import S2Parser
from s2python.s2_validation_error import S2ValidationError
from s2python.message import S2Message
from s2python.version import S2_VERSION
from s2python.s2_asset_details import AssetDetails
from s2python.s2_message_handlers import MessageHandlers

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


class ReceptionStatusAwaiter:
    """"
    This fixes races and memory leaks compared to the original
    ReceptionStatusAwaiter provided in s2python v0.8.1.
    """
    received: Dict[uuid.UUID, ReceptionStatus]
    awaiting: Dict[uuid.UUID, asyncio.Event]

    def __init__(self) -> None:
        self.received = {}
        self.awaiting = {}

    async def wait_for_reception_status(
        self, message_id: uuid.UUID, timeout_reception_status: float
    ) -> ReceptionStatus:

        existing = self.received.pop(message_id, None)
        if existing is not None:
            return existing

        received_event = self.awaiting.get(message_id)
        if received_event is None:
            received_event = asyncio.Event()
            self.awaiting[message_id] = received_event

        try:
            await asyncio.wait_for(received_event.wait(), timeout_reception_status)
            return self.received.pop(message_id)
        finally:
            self.awaiting.pop(message_id, None)

    async def receive_reception_status(self, reception_status: ReceptionStatus) -> None:
        if not isinstance(reception_status, ReceptionStatus):
            raise RuntimeError(
                f"Expected a ReceptionStatus but received message {reception_status}"
            )

        mid = reception_status.subject_message_id

        if mid in self.received:
            raise RuntimeError(
                f"ReceptionStatus for message_subject_id {mid} has already been received!"
            )

        self.received[mid] = reception_status

        awaiting = self.awaiting.get(mid)
        if awaiting is not None:
            awaiting.set()
            self.awaiting.pop(mid, None)


class S2ResourceManagerItem(S2ServerItem):

    _received_messages: asyncio.Queue
    _restart_connection_event: asyncio.Event

    reception_status_awaiter: ReceptionStatusAwaiter
    s2_parser: S2Parser
    control_types: List[S2ControlType]
    role: EnergyManagementRole
    asset_details: AssetDetails

    _handlers: MessageHandlers
    _current_control_type: S2ControlType | None

    def __init__(self, path,
                 control_types: List[S2ControlType] | None = None,
                 asset_details: AssetDetails | None = None):
        super().__init__(path)

        self.role = EnergyManagementRole.RM
        self.reception_status_awaiter = ReceptionStatusAwaiter()
        self.s2_parser = S2Parser()
        self._handlers = MessageHandlers()
        self._current_control_type = None

        self.control_types = control_types
        self.asset_details = asset_details

        self._main_task: asyncio.Task | None = None

        self._handlers.register_handler(SelectControlType, self.handle_select_control_type_as_rm)
        self._handlers.register_handler(Handshake, self.handle_handshake)
        self._handlers.register_handler(HandshakeResponse, self.handle_handshake_response_as_rm)

    async def set_ready(self, ready: bool, control_types: List[S2ControlType] | None = None, asset_details: AssetDetails | None = None):
        if control_types is not None:
            self.control_types = control_types
        if asset_details is not None:
            self.asset_details = asset_details
        await super().set_ready(ready)

    async def close(self) -> None:
        # Set not ready and destroy existing connection
        await self.set_ready(False)

        # Try to unwind the internal logic to unwind if possible,
        # guarded in case it does not exist yet
        if hasattr(self, "_restart_connection_event") \
                and self._restart_connection_event:
            self._restart_connection_event.set()
        if hasattr(self, "_s2_dbus_disconnect_event") \
                and self._s2_dbus_disconnect_event:
            self._s2_dbus_disconnect_event.set()

        # Cancel the main task with its children
        task = self._main_task
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("Error while closing S2ResourceManagerItem")

        self._main_task = None

    async def _create_connection(self, client_id: str, keep_alive_interval: int):
        await super()._create_connection(client_id, keep_alive_interval)
        self._main_task = self._runningloop.create_task(self._connect_and_run())

    async def _connect_and_run(self) -> None:
        self._received_messages = asyncio.Queue()
        self._restart_connection_event = asyncio.Event()

        logger.debug("Connecting as S2 resource manager.")

        async def wait_till_disconnect() -> None:
            await self._s2_dbus_disconnect_event.wait()

        async def wait_till_connection_restart() -> None:
            await self._restart_connection_event.wait()

        background_tasks = [
            self._runningloop.create_task(wait_till_disconnect()),
            self._runningloop.create_task(self._connect_as_rm()),
            self._runningloop.create_task(wait_till_connection_restart()),
        ]

        (done, pending) = await asyncio.wait(
            background_tasks, return_when=asyncio.FIRST_COMPLETED
        )
        if self._current_control_type:
            if asyncio.iscoroutinefunction(self._current_control_type.deactivate):
                await self._current_control_type.deactivate(self)
            else:
                await self._runningloop.run_in_executor(
                    None, self._current_control_type.deactivate, self
                )
            self._current_control_type = None

        for task in done:
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.exception(f"S2: {e}")

        for task in pending:
            try:
                task.cancel()
                await task
            except asyncio.CancelledError:
                pass

        await self._destroy_connection("service shutdown")
        logger.debug("Finished S2 connection eventloop.")

    async def _on_s2_message(self, message):
        try:
            s2_msg: S2Message = self.s2_parser.parse_as_any_message(message)
        except json.JSONDecodeError:
            await self._send_and_forget(
                ReceptionStatus(
                    subject_message_id=uuid.UUID("00000000-0000-0000-0000-000000000000"),
                    status=ReceptionStatusValues.INVALID_DATA,
                    diagnostic_label="Not valid json.",
                )
            )
        except S2ValidationError as e:
            json_msg = json.loads(message)
            message_id = json_msg.get("message_id")
            if message_id:
                await self._respond_with_reception_status(
                    subject_message_id=message_id,
                    status=ReceptionStatusValues.INVALID_MESSAGE,
                    diagnostic_label=str(e),
                )
            else:
                await self._respond_with_reception_status(
                    subject_message_id=uuid.UUID("00000000-0000-0000-0000-000000000000"),
                    status=ReceptionStatusValues.INVALID_DATA,
                    diagnostic_label="Message appears valid json but could not find a message_id field.",
                )
        else:
            logger.debug("Received message %s", s2_msg.to_json())

            if isinstance(s2_msg, ReceptionStatus):
                logger.debug(
                    "Message is a reception status for %s so registering in cache.",
                    s2_msg.subject_message_id,
                )
                await self.reception_status_awaiter.receive_reception_status(s2_msg)
            else:
                await self._received_messages.put(s2_msg)

    async def _connect_as_rm(self) -> None:
        await self.send_msg_and_await_reception_status(
            Handshake(
                message_id=uuid.uuid4(),
                role=self.role,
                supported_protocol_versions=[S2_VERSION],
            )
        )
        logger.debug(
            "Send handshake to CEM, expecting Handshake and HandshakeResponse."
        )

        await self._handle_received_messages()

    async def handle_handshake(
        self, _: "S2ResourceManagerItem", message: S2Message, send_okay: Awaitable[None]
    ) -> None:
        if not isinstance(message, Handshake):
            logger.error(
                "Handler for Handshake received a message of the wrong type: %s",
                type(message),
            )
            return

        logger.debug(
            "%s supports S2 protocol versions: %s",
            message.role,
            message.supported_protocol_versions,
        )
        await send_okay

    async def handle_handshake_response_as_rm(
        self, _: "S2ResourceManagerItem", message: S2Message, send_okay: Awaitable[None]
    ) -> None:
        if not isinstance(message, HandshakeResponse):
            logger.error(
                "Handler for HandshakeResponse received a message of the wrong type: %s",
                type(message),
            )
            return

        logger.debug("Received HandshakeResponse %s", message.to_json())

        logger.debug(
            "CEM selected to use version %s", message.selected_protocol_version
        )
        await send_okay
        logger.debug("Handshake complete. Sending first ResourceManagerDetails.")

        await self.send_resource_manager_details()

    async def handle_select_control_type_as_rm(
        self, _: "S2ResourceManagerItem", message: S2Message, send_okay: Awaitable[None]
    ) -> None:
        if not isinstance(message, SelectControlType):
            logger.error(
                "Handler for SelectControlType received a message of the wrong type: %s",
                type(message),
            )
            return

        await send_okay

        logger.debug(
            "CEM selected control type %s. Activating control type.",
            message.control_type,
        )

        control_types_by_protocol_name = {
            c.get_protocol_control_type(): c for c in self.control_types
        }
        selected_control_type: S2ControlType | None = (
            control_types_by_protocol_name.get(message.control_type)
        )

        if self._current_control_type is not None:
            if asyncio.iscoroutinefunction(self._current_control_type.deactivate):
                await self._current_control_type.deactivate(self)
            else:
                await self._runningloop.run_in_executor(
                    None, self._current_control_type.deactivate, self
                )

        self._current_control_type = selected_control_type

        if self._current_control_type is not None:
            if asyncio.iscoroutinefunction(self._current_control_type.activate):
                await self._current_control_type.activate(self)
            else:
                await self._runningloop.run_in_executor(
                    None, self._current_control_type.activate, self
                )
            self._current_control_type.register_handlers(self._handlers)

    async def _send_and_forget(self, s2_msg: S2Message) -> None:
        if not self.is_connected:
            raise RuntimeError(
                "Cannot send messages if client connection is not yet established."
            )

        json_msg = s2_msg.to_json()
        logger.debug("Sending message %s", json_msg)

        try:
            self._send_message(json_msg)
        except Exception as e:
            logger.exception("Unable to send message %s due to %s", s2_msg, str(e))
            self._restart_connection_event.set()

    async def _respond_with_reception_status(
        self, subject_message_id: uuid.UUID, status: ReceptionStatusValues, diagnostic_label: str
    ) -> None:
        logger.debug(
            "Responding to message %s with status %s", subject_message_id, status
        )
        await self._send_and_forget(
            ReceptionStatus(
                subject_message_id=subject_message_id,
                status=status,
                diagnostic_label=diagnostic_label,
            )
        )

    def _respond_with_reception_status_sync(
        self, subject_message_id: uuid.UUID, status: ReceptionStatusValues, diagnostic_label: str
    ) -> None:
        asyncio.run_coroutine_threadsafe(
            self._respond_with_reception_status(
                subject_message_id, status, diagnostic_label
            ),
            self._runningloop,
        ).result()

    async def send_msg_and_await_reception_status(
        self,
        s2_msg: S2Message,
        timeout_reception_status: float = 5.0,
        raise_on_error: bool = True,
    ) -> ReceptionStatus:
        await self._send_and_forget(s2_msg)
        logger.debug(
            "Waiting for ReceptionStatus for %s %s seconds",
            s2_msg.message_id,  # type: ignore[attr-defined, union-attr]
            timeout_reception_status,
        )
        try:
            reception_status = await self.reception_status_awaiter.wait_for_reception_status(
                s2_msg.message_id, timeout_reception_status  # type: ignore[attr-defined, union-attr]
            )
        except TimeoutError:
            logger.error(
                "Did not receive a reception status on time for %s",
                s2_msg.message_id,  # type: ignore[attr-defined, union-attr]
            )
            self._restart_connection_event.set()
            raise

        if reception_status.status != ReceptionStatusValues.OK and raise_on_error:
            raise RuntimeError(
                f"ReceptionStatus was not OK but rather {reception_status.status}"
            )

        return reception_status

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

    async def _handle_received_messages(self) -> None:
        while True:
            msg = await self._received_messages.get()
            await self._handlers.handle_message(self, msg)

    async def send_resource_manager_details(
        self,
        control_types: List[S2ControlType] = None,
        asset_details: AssetDetails = None
    ) -> ReceptionStatus:
        if control_types is not None:
            self.control_types = control_types
        if asset_details is not None:
            self.asset_details = asset_details
        reception_status: ReceptionStatus = await self.send_msg_and_await_reception_status(
            self.asset_details.to_resource_manager_details(self.control_types)
        )
        return reception_status

    def send_resource_manager_details_sync(
        self,
        control_types: List[S2ControlType] = None,
        asset_details: AssetDetails = None
    ) -> ReceptionStatus:
        return asyncio.run_coroutine_threadsafe(
            self.send_resource_manager_details(
                control_types, asset_details
            ),
            self._runningloop,
        ).result()
