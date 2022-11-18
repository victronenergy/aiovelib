from aiovelib.client import make_variant, Monitor, ServiceHandler, DbusException
from aiovelib.client import Service

SETTINGS_SERVICE = "com.victronenergy.settings"
SETTINGS_INTERFACE = "com.victronenergy.Settings"
SETTINGS_PATH = "/Settings"

class SettingException(DbusException):
	pass

class Setting(dict):
	def __init__(self, path, default, _min=None, _max=None, silent=False):
		super().__init__(path=make_variant(path), default=make_variant(default))
		if _min is not None:
			self["min"] = make_variant(_min)
		if _max is not None:
			self["max"] = make_variant(_max)
		if silent:
			self["silent"] = make_variant(1)

class SettingsService(Service):
	servicetype = SETTINGS_SERVICE
	paths = set() # Empty set

	async def add_settings(self, *settings):
		reply = await self.monitor.dbus_call(SETTINGS_SERVICE, "/",
			"AddSettings", "aa{sv}", list(settings),
			interface=SETTINGS_INTERFACE)

		for result in reply[0]:
			path = result["path"].value
			if result["error"].value == 0:
				self.paths.add(path)
				self.values[path].update(result["value"].value)
			else:
				raise SettingException(path)

if __name__ == "__main__":
	import asyncio
	from dbus_next.aio import MessageBus
	from dbus_next.constants import BusType

	class MyMonitor(Monitor):
		def itemsChanged(self, name, values):
			""" Callback """
			for p, v in values.items():
				print (f"{name}{p} changed to {v}")

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

	asyncio.get_event_loop().run_until_complete(main())
