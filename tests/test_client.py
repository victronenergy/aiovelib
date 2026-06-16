""" Example tests for the consumer side, driven by the in-process MockMonitor.
	These need no D-Bus / running session bus. """

import asyncio

from aiovelib import client
from aiovelib.client import ServiceHandler
from aiovelib.test.client import MockMonitor


class GridService(client.Service, ServiceHandler):
	servicetype = "com.victronenergy.grid"
	paths = {"/Ac/Power", "/Ac/L1/Voltage"}


class VebusService(client.Service, ServiceHandler):
	# No paths restriction: track everything.
	servicetype = "com.victronenergy.vebus"
	paths = None


# Pass handlers explicitly (rather than relying on the global registry that
# ServiceHandler populates) so tests stay isolated from each other.
HANDLERS = {
	GridService.servicetype: GridService,
	VebusService.servicetype: VebusService,
}

GRID = "com.victronenergy.grid.test"
VEBUS = "com.victronenergy.vebus.test"


def make_monitor(**kwargs):
	return MockMonitor(handlers=HANDLERS, **kwargs)


async def test_add_service_seeds_values():
	monitor = make_monitor()
	service = await monitor.add_service(GRID, {"/Ac/Power": 123.0})

	assert isinstance(service, GridService)
	assert monitor.get_value(GRID, "/Ac/Power") == 123.0
	assert service.get_value("/Ac/Power") == 123.0
	assert monitor.seen(GRID, "/Ac/Power")
	# A seeded value is "seen" but did not fire itemsChanged.
	assert monitor.get_value(GRID, "/Missing") is None
	assert not monitor.seen(GRID, "/Missing")


async def test_unknown_servicetype_is_ignored():
	monitor = make_monitor()
	assert await monitor.add_service("com.victronenergy.solarcharger.x") is None


async def test_create_classmethod_needs_no_bus():
	monitor = await MockMonitor.create(handlers=HANDLERS)
	await monitor.add_service(VEBUS, {"/State": 3})
	assert monitor.get_value(VEBUS, "/State") == 3


async def test_update_service_fires_itemschanged():
	changes = []
	monitor = make_monitor(itemsChanged=lambda s, v: changes.append((s.name, v)))
	await monitor.add_service(GRID, {"/Ac/Power": 1.0})

	updated = monitor.update_service(GRID, {"/Ac/Power": 250.0})

	assert updated == {"/Ac/Power": 250.0}
	assert monitor.get_value(GRID, "/Ac/Power") == 250.0
	assert changes == [(GRID, {"/Ac/Power": 250.0})]


async def test_paths_filtering_ignores_untracked_paths():
	changes = []
	monitor = make_monitor(itemsChanged=lambda s, v: changes.append(v))
	await monitor.add_service(GRID, {"/Ac/Power": 1.0, "/Untracked": 9})

	# Seeding ignored the untracked path entirely.
	assert not monitor.seen(GRID, "/Untracked")

	updated = monitor.update_service(GRID, {"/Untracked": 42})

	assert updated == {}
	assert changes == []  # nothing tracked changed, so no callback
	assert monitor.get_value(GRID, "/Untracked") is None


async def test_set_value_updates_cache():
	monitor = make_monitor()
	await monitor.add_service(GRID, {"/Ac/Power": 1.0})

	result = await monitor.set_value(GRID, "/Ac/Power", 500.0)

	assert result == 0
	assert monitor.get_value(GRID, "/Ac/Power") == 500.0

	# Unknown service -> -1.
	assert await monitor.set_value("com.victronenergy.grid.absent", "/Ac/Power", 1) == -1


async def test_set_value_async_fire_and_forget():
	monitor = make_monitor()
	await monitor.add_service(GRID, {"/Ac/Power": 1.0})

	# Unknown path -> -1, no task scheduled.
	assert monitor.set_value_async(GRID, "/Nope", 1) == -1
	assert monitor.set_value_async("com.victronenergy.grid.absent", "/Ac/Power", 1) == -1

	monitor.set_value_async(GRID, "/Ac/Power", 7.0)
	# Let the scheduled task run.
	await asyncio.sleep(0)

	assert monitor.get_value(GRID, "/Ac/Power") == 7.0


async def test_wait_for_service_resolves_on_add():
	monitor = make_monitor()
	waiter = asyncio.ensure_future(monitor.wait_for_service(VEBUS))
	await asyncio.sleep(0)  # let the waiter register its future
	assert not waiter.done()

	await monitor.add_service(VEBUS, {"/State": 3})
	service = await waiter

	assert service is monitor.get_service(VEBUS)


async def test_remove_service():
	removed = []
	monitor = make_monitor()

	async def on_removed(service):
		removed.append(service.name)

	monitor.serviceRemoved = on_removed
	await monitor.add_service(VEBUS, {"/State": 3})

	await monitor.remove_service(VEBUS)

	assert monitor.get_service(VEBUS) is None
	assert removed == [VEBUS]
