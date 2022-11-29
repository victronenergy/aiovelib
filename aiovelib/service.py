import asyncio
from dbus_next.service import ServiceInterface, method, signal
from dbus_next import Variant, Message, MessageFlag, MessageType

IFACE="com.victronenergy.BusItem"

class Item(ServiceInterface):
	def __init__(self, path, value=None, writeable=False, onchange=None, text=None):
		super().__init__(IFACE)
		self.path = path
		self.value = value
		self.writeable = writeable
		self.onchange = onchange
		self.text = text
		self.service = None

	def __str__(self):
		if self.text:
			return self.text(self.value)
		return str(self.value)

	@method()
	def GetValue(self) -> 'v':
		return self.get_value()

	@method()
	def GetText(self) -> 's':
		return self.get_text()

	@method()
	def SetValue(self, v: 'v') -> 'i':
		if not self.writeable:
			return 1

		try:
			newvalue = self.valuetype(v.value)
		except (ValueError, TypeError):
			return 1

		return self.set_value(newvalue)

	def get_value(self):
		if self.value is None:
			return Variant('ai', [])
		return Variant(self.coding, self.value)

	def get_text(self):
		if self.value is None:
			return ''
		return str(self)

	def set_value(self, v):
		try:
			change = self._set_value(v)
		except ValueError:
			return 1

		if change is not None:
			# Send ItemsChanged
			self.service.send_items_changed({
				self.path: change
			})
		return 0

	def _set_value(self, v):
		if v == self.value:
			return None

		if self.onchange is None or self.onchange(v):
			self.value = v
			return {'Value': self.get_value(), 'Text': Variant('s', self.get_text()) }

		raise ValueError(v)

class IntegerItem(Item):
	coding = 'i'
	valuetype = int

class DoubleItem(Item):
	coding = 'd'
	valuetype = float

class TextItem(Item):
	coding = 's'
	valuetype = str

class TextArrayItem(Item):
	coding = 'as'
	valuetype = list

class RootItemInterface(ServiceInterface):
	def __init__(self, service):
		super().__init__(IFACE)
		self.service = service

	@method()
	def GetItems(self) -> 'a{sa{sv}}':
		return {
			p: {'Value': v.get_value(), 'Text': Variant('s', v.get_text()) } \
			for p, v in self.service.objects.items()
		}

	@signal()
	def ItemsChanged(self, changes) -> 'a{sa{sv}}':
		""" This is here for introspection. We don't use it because dbus-next
		    does not allow us to set the NO_REPLY_EXPECTED flag. """
		return changes

	@method()
	def GetValue(self) -> 'v':
		return Variant('ai', [])

	@method()
	def GetText(self) -> 's':
		return ''

	@method()
	def SetValue(self, v: 'v') -> 'i':
		return 1

class ItemChangeCollector(object):
	def __init__(self, service):
		self.service = service
		self.changes = {}

	def __setitem__(self, path, value):
		change = self.service.objects[path]._set_value(value)
		if change is not None:
			self.changes[path] = change

	def flush(self):
		self.service.send_items_changed(self.changes)

class Service(object):
	@classmethod
	async def create(cls, bus, name):
		self = cls(bus, name)
		self.interface = RootItemInterface(self)
		bus.export('/', self.interface)
		await bus.request_name(name)
		return self

	def __init__(self, bus, name):
		self.bus = bus
		self.name = name
		self.objects = {}
		self.changecollectors = []

	def __enter__(self):
		l = ItemChangeCollector(self)
		self.changecollectors.append(l)
		return l

	def __exit__(self, *exc):
		if self.changecollectors:
			self.changecollectors.pop().flush()

	def __getitem__(self, path):
		return self.objects[path].value

	def __del__(self):
		while self.objects:
			path, ob = self.objects.popitem()
			self.bus.unexport(path)
		self.bus.unexport("/")
		asyncio.get_event_loop().create_task(self.bus.release_name(self.name))

	def add_item(self, item):
		self.bus.export(item.path, item)
		self.objects[item.path] = item
		item.service = self

	def send_items_changed(self, changes):
		# Send the signal ourselves, so we can set NO_REPLY_EXPECTED.
		msg = Message(
			message_type=MessageType.SIGNAL,
			interface=IFACE,
			path="/",
			flags=MessageFlag.NO_REPLY_EXPECTED,
			member="ItemsChanged",
			signature="a{sa{sv}}",
			body=[changes])
		self.bus.send(msg)

if __name__ == "__main__":
	from dbus_next.aio import MessageBus
	from dbus_next.constants import BusType

	async def main():
		bus = await MessageBus(bus_type=BusType.SESSION).connect()
		service = await Service.create(bus, 'com.victronenergy.grid.example')

		def check_int(v):
			if 0 < v < 100:
				return True
			return False

		service.add_item(IntegerItem('/DeviceInstance', 0))
		service.add_item(IntegerItem('/Int', 1, writeable=True, onchange=check_int))
		service.add_item(DoubleItem('/Double', 2.0, writeable=True))
		service.add_item(TextItem('/Text', 'This is text', writeable=True))
		service.add_item(TextArrayItem('/Array', ['a', 'b']))

		# emit the changed signal after two seconds.
		await asyncio.sleep(3)

		with service as s:
			s['/Int'] = 11
			s['/Double'] = 22.0
			s['/Text'] = 'This is not text'

		await bus.wait_for_disconnect()

	asyncio.get_event_loop().run_until_complete(main())
