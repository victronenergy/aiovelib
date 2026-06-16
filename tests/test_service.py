""" Example tests for the provider side, driven by the in-process MockService.
	These need no D-Bus / running session bus. """

import asyncio

from aiovelib.service import (
	IntegerItem, DoubleItem, TextItem, TextArrayItem)
from aiovelib.test.service import MockService

GRID = "com.victronenergy.grid.test"


def make_service():
	return MockService(GRID)


async def test_add_item_and_read_back():
	service = make_service()
	service.add_item(IntegerItem("/Ac/Power", 123))

	assert service["/Ac/Power"] == 123
	assert service.get_item("/Ac/Power").value == 123
	# add_item binds the service back-reference.
	assert service.get_item("/Ac/Power").service is service


async def test_value_coercion():
	service = make_service()
	service.add_item(IntegerItem("/Int", "5"))
	service.add_item(DoubleItem("/Double", 2))

	assert service["/Int"] == 5
	assert isinstance(service["/Int"], int)
	assert service["/Double"] == 2.0
	assert isinstance(service["/Double"], float)


async def test_batch_update_context_manager():
	service = make_service()
	service.add_item(IntegerItem("/Int", 1))
	service.add_item(TextItem("/Text", "before"))

	with service as s:
		s["/Int"] = 11
		s["/Text"] = "after"

	assert service["/Int"] == 11
	assert service["/Text"] == "after"


async def test_set_local_value():
	service = make_service()
	item = TextArrayItem("/Array", ["a", "b"])
	service.add_item(item)

	item.set_local_value(["c", "d"])

	assert service["/Array"] == ["c", "d"]


async def test_set_value_applies_and_returns_zero():
	# set_value() is the local setter; it applies the value and returns 0 on
	# success. The writeable gate lives in the D-Bus SetValue method, which is
	# only reachable over the bus, so it is not exercised here.
	service = make_service()
	item = IntegerItem("/Int", 0)
	service.add_item(item)

	assert item.set_value(42) == 0
	assert service["/Int"] == 42


async def test_set_value_coercion_failure_returns_one():
	service = make_service()
	item = IntegerItem("/Int", 0, writeable=True)
	service.add_item(item)

	assert item.set_value("not a number") == 1
	assert service["/Int"] == 0


async def test_sync_onchange_accept_and_reject():
	def only_below_100(v):
		return 0 <= v < 100

	service = make_service()
	item = IntegerItem("/Int", 0, writeable=True, onchange=only_below_100)
	service.add_item(item)

	# Accepted: onchange returns True, value applied.
	assert item.set_value(50) == 0
	assert service["/Int"] == 50

	# Rejected: onchange returns False, SetValue surfaces failure.
	assert item.set_value(150) == 1
	assert service["/Int"] == 50


async def test_async_onchange_applies_value():
	async def apply_after_sleep(item, v):
		await asyncio.sleep(0)
		item.set_local_value(v)

	service = make_service()
	item = IntegerItem("/Int", 0, writeable=True, onchange=apply_after_sleep)
	service.add_item(item)

	# Async onchange schedules a task and returns 0 immediately; value not
	# applied yet.
	assert item.set_value(7) == 0
	assert service["/Int"] == 0

	# Let the scheduled task run to completion (it awaits internally).
	await asyncio.sleep(0.01)
	assert service["/Int"] == 7


async def test_remove_item():
	service = make_service()
	service.add_item(IntegerItem("/Int", 1))

	service.remove_item("/Int")

	assert service.get_item("/Int") is None


async def test_close_clears_items_without_bus():
	service = make_service()
	service.add_item(IntegerItem("/Int", 1))

	await service.close()

	assert service.objects == {}
	# Idempotent.
	await service.close()


async def test_async_context_manager_closes():
	async with MockService(GRID) as service:
		service.add_item(IntegerItem("/Int", 1))
		assert service["/Int"] == 1

	assert service._closed
	assert service.objects == {}
