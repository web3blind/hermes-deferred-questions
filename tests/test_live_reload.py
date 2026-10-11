"""Real manager/base-adapter rewire on the same live native client."""
import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest
sys.path.append(str(Path(__file__).parent))
from test_deferred_questions import ControlledAdapter, runtime


@pytest.mark.asyncio
async def test_force_reload_rewires_new_runtime_on_existing_client(installed, monkeypatch):
    manager = installed[0]
    monkeypatch.setattr('hermes_cli.plugins.get_plugin_manager', lambda: manager)
    adapter = ControlledAdapter.make('telegram')
    handlers = []
    native = NS(bot=NS(send_message=AsyncMock()),
                add_handler=lambda h, group: handlers.append(h),
                remove_handler=lambda h, group: handlers.remove(h))
    adapter._wire_plugin_handlers(native)
    old = runtime(manager)
    assert 'telegram' in old.adapters
    manager.discover_and_load(force=True)
    await asyncio.sleep(0)
    fresh = runtime(manager)
    assert fresh is not old and not old.live
    adapter.rewire_plugin_handlers()
    assert 'telegram' in fresh.adapters
    assert not fresh.workers['telegram'].done()
    assert len(handlers) == 1
    adapter.rewire_plugin_handlers()
    assert len(handlers) == 1
