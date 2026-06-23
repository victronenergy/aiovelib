from aiovelib.client import make_variant, Monitor, ServiceHandler, DbusException
from aiovelib.client import Service

SETTINGS_SERVICE = "com.victronenergy.settings"
SETTINGS_INTERFACE = "com.victronenergy.Settings"
SETTINGS_PATH = "/Settings"

class SettingException(DbusException):
	pass

class Setting(dict):
	def __init__(self, path, default, _min=None, _max=None, silent=False, alias=None):
		super().__init__(path=make_variant(path), default=make_variant(default))
		if _min is not None:
			self["min"] = make_variant(_min)
		if _max is not None:
			self["max"] = make_variant(_max)
		if silent:
			self["silent"] = make_variant(1)
		self.alias = alias

class AbstractSettingsService(Service):
	""" Bus-agnostic core of the settings client. Holds the alias and value
	    bookkeeping that does not touch the bus, so it can be shared by the
	    real SettingsService and by an in-memory test double. Subclasses
	    implement add_settings (the bus call lives there). """
	servicetype = SETTINGS_SERVICE
	paths = set() # Empty set
	aliases = {}

	def _register_aliases(self, settings):
		self.aliases.update((s.alias, s["path"].value) for s in settings)

	def _apply_setting(self, path, value):
		# Store the current value locally, so we avoid an additional GetValue.
		self.paths.add(path)
		self.values[path].update(value)

	def alias(self, a):
		return self.aliases.get(a)

class SettingsService(AbstractSettingsService):
	async def add_settings(self, *settings):
		# Update the aliases
		self._register_aliases(settings)

		# add settings
		reply = await self.monitor.dbus_call(SETTINGS_SERVICE, "/",
			"AddSettings", "aa{sv}", list(settings),
			interface=SETTINGS_INTERFACE)

		# process results, store current values.
		for result in reply[0]:
			if result["error"].value == 0:
				self._apply_setting(result["path"].value, result["value"].value)
			else:
				raise SettingException(result["path"].value)

if __name__ == "__main__":
	import asyncio
	try:
		from dbus_fast.aio import MessageBus
		from dbus_fast.constants import BusType
	except ImportError:
		from dbus_next.aio import MessageBus
		from dbus_next.constants import BusType

	class MyMonitor(Monitor):
		def itemsChanged(self, service, values):
			""" Callback """
			for p, v in values.items():
				print (f"{service.name}{p} changed to {v}")

	class MySettingsService(SettingsService, ServiceHandler):
		pass

	async def main():
		bus = await MessageBus(bus_type=BusType.SESSION).connect()
		monitor = await MyMonitor.create(bus)

		service = await monitor.wait_for_service(SETTINGS_SERVICE)
		await service.add_settings(
			Setting("/Settings/AioVelib/OptionA", 3, 0, 5),
			Setting("/Settings/AioVelib/OptionB", "x")
		)

		await bus.wait_for_disconnect()

	asyncio.run(main())
