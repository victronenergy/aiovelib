""" In-process test double for localsettings.

	MockSettingsService applies each Setting's default straight into an
	in-memory dictionary (the Service's `values` map), so there is no bus and
	no AddSettings round-trip. MockSettingsMonitor is a MockMonitor preset to
	serve it, with the settings service already registered:

		monitor = await MockSettingsMonitor.create()
		settings = await monitor.wait_for_service(SETTINGS_SERVICE)
		await settings.add_settings(Setting("/Settings/Foo", 3))
		assert settings.get_value("/Settings/Foo") == 3 """

from aiovelib.localsettings import AbstractSettingsService, SETTINGS_SERVICE
from aiovelib.test.client import MockMonitor

class MockSettingsService(AbstractSettingsService):
	""" Keeps settings in memory; no bus. """

	def __init__(self, monitor, name, owner):
		super().__init__(monitor, name, owner)
		# Instance-level state so repeated test runs don't leak settings
		# across instances (the base keeps these on the class).
		self.paths = set()
		self.aliases = {}

	async def add_settings(self, *settings):
		self._register_aliases(settings)
		for s in settings:
			self._apply_setting(s["path"].value, s["default"].value)

class MockSettingsMonitor(MockMonitor):
	""" A MockMonitor that already serves an in-memory localsettings. """

	@classmethod
	async def create(cls, bus=None, **kwargs):
		kwargs.setdefault("handlers", {SETTINGS_SERVICE: MockSettingsService})
		self = cls(**kwargs)
		await self.add_service(SETTINGS_SERVICE)
		return self
