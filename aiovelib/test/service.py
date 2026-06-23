""" In-process test double for aiovelib.service.Service.

	MockService reuses all of the bus-agnostic provider logic in
	AbstractService (item bookkeeping, the batch-update context manager and
	the accessors), but emits no D-Bus signal and exports nothing to a bus:

		service = MockService("com.victronenergy.grid.test")
		service.add_item(IntegerItem("/Ac/Power", 123))
		assert service["/Ac/Power"] == 123

	Use it to drive provider-side code (items, onchange handlers, batch
	updates) without a running session bus. """

from aiovelib.service import AbstractService

class MockService(AbstractService):
	""" A provider Service that needs no bus. See AbstractService for the
	    full API. """
	pass
