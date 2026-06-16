""" In-process test double for aiovelib.client.Monitor.

	MockMonitor reuses all of the bus-agnostic orchestration in
	AbstractMonitor, but instead of talking to D-Bus you add the services you
	want to test explicitly, with their values:

		monitor = MockMonitor(handlers={GridService.servicetype: GridService})
		await monitor.add_service("com.victronenergy.grid.test",
			{"/Ac/Power": 123.0})

		assert monitor.get_value("com.victronenergy.grid.test", "/Ac/Power") == 123.0

	update_service() simulates a runtime ItemsChanged signal, and set_value() /
	set_value_async() update the cached value directly (observable via get_value). """

from aiovelib.client import AbstractMonitor, make_variant

class MockMonitor(AbstractMonitor):
	""" A Monitor that needs no bus. Add fake services with add_service(). """

	@classmethod
	async def create(cls, *args, **kwargs):
		return cls(*args, **kwargs)

	async def add_service(self, name, values=None, owner=None):
		""" Explicitly register a fake service. values is an optional
		    path -> value mapping used to seed the service (without
		    firing itemsChanged, like an initial scan does). Returns the
		    Service, or None if no handler is registered for it. """
		if owner is None:
			owner = name # owner identity is irrelevant for tests

		if owner in self._services:
			return None

		service = self._make_service(name, owner)
		if service is None:
			return None

		if values:
			service.update_unseen_items(
				{p: {"Value": make_variant(v)} for p, v in values.items()})

		await self._activate_service(service)
		return service

	def update_service(self, name, values):
		""" Simulate an ItemsChanged signal for an already-added service:
		    update the given path -> value mapping and fire itemsChanged.
		    Returns the dict of paths that actually changed. """
		service = self.get_service(name)
		if service is None:
			return None

		updated = service.update_items(
			{p: {"Value": make_variant(v)} for p, v in values.items()})
		if updated:
			self.itemsChanged(service, updated)
		return updated

	async def remove_service(self, name, owner=None):
		""" Simulate the service disappearing from the bus. """
		if owner is None:
			owner = name
		if owner in self._services:
			await self._forget_service(name, owner)

	async def set_value(self, name, path, value):
		""" Directly update the cached value; no D-Bus involved. Returns 0
		    on success (matching the real Monitor) or -1 for an unknown
		    service. The update is observable via get_value(). """
		service = self.get_service(name)
		if service is None:
			return -1
		service.values[path].update(value)
		return 0
