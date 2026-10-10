"""Native entrypoint recovery before Telegram connect completes; no network."""
import asyncio
from pathlib import Path

import pytest

from test_deferred_questions import runtime
from test_entrypoint_lifecycle import adapter_for
from test_entrypoint_edges import until


def connecting(manager, module):
    # Guard against accidentally testing a pre-imported installed distribution.
    assert Path(module.runtime.__file__).parent.parent == Path(runtime(manager).store.home).parent
    adapter, native = adapter_for('telegram')
    adapter._running = False
    # Native connect calls factories before PTB initialize/start completes.
    adapter._wire_plugin_handlers(native)
    assert runtime(manager).adapters.get('telegram') == (native, adapter)
    return adapter, native


@pytest.mark.asyncio
@pytest.mark.parametrize('generations', [1, 2])
@pytest.mark.parametrize('offloop', [False, True])
async def test_force_before_connect_recovers_after_delayed_readiness(ep, generations, offloop):
    manager, module, _ = ep
    adapter, native = connecting(manager, module)
    retired = [runtime(manager)]
    for _ in range(generations):
        if offloop:
            await asyncio.to_thread(manager.discover_and_load, force=True)
        else:
            manager.discover_and_load(force=True)
        await asyncio.sleep(.05)
        retired.append(runtime(manager))
    fresh = retired.pop()
    await asyncio.sleep(1.1)  # Later than the registration-publication poll budget.
    assert not adapter.is_connected
    assert fresh.adapters == {}
    assert native.handlers.get(-20, []) == []
    adapter._running = True
    try:
        await until(lambda: fresh.adapters.get('telegram') == (native, adapter), timeout=2)
        assert fresh.health()['telegram']['worker_alive']
        assert len(native.handlers[-20]) == 1
        assert all(not rt.live for rt in retired)
        assert all(t.done() for rt in retired for tasks in rt._tasks.values() for t in tasks)
    finally:
        manager.unload()
        await asyncio.sleep(.05)
    assert native.handlers.get(-20, []) == []


@pytest.mark.asyncio
@pytest.mark.parametrize('replacement', ['native', 'adapter'])
async def test_new_connect_candidate_supersedes_readiness_waiter(ep, replacement):
    manager, module, _ = ep
    adapter, native = connecting(manager, module)
    await asyncio.to_thread(manager.discover_and_load, force=True)
    fresh = runtime(manager)
    await until(lambda: fresh.health()['telegram']['reason'] == 'not_connected')
    newer_adapter, newer_native = adapter_for('telegram')
    if replacement == 'native':
        newer_adapter = adapter
        newer_adapter._running = True
    newer_adapter._wire_plugin_handlers(newer_native)
    await until(lambda: fresh.adapters.get('telegram') == (newer_native, newer_adapter))
    adapter._running = True
    await asyncio.sleep(.05)
    assert fresh.adapters['telegram'] == (newer_native, newer_adapter)
    assert native.handlers.get(-20, []) == []
    assert len(newer_native.handlers[-20]) == 1
    await until(lambda: all(t.done() for _, t in fresh._recovery))
    manager.unload()
    await asyncio.sleep(.05)


@pytest.mark.asyncio
@pytest.mark.parametrize('mode', ['unload', 'disable', 'failed_registration'])
async def test_readiness_waiter_is_cancelled_without_resurrection(ep, monkeypatch, mode):
    manager, module, _ = ep
    adapter, native = connecting(manager, module)
    await asyncio.to_thread(manager.discover_and_load, force=True)
    fresh = runtime(manager)
    await until(lambda: fresh.health()['telegram']['reason'] == 'not_connected')
    waiting = [t for tasks in fresh._tasks.values() for t in tasks]
    assert waiting and not all(t.done() for t in waiting)
    if mode == 'unload':
        manager.unload()
    else:
        if mode == 'disable':
            Path(fresh.store.home, 'config.yaml').write_text('plugins:\n  enabled: []\n')
        else:
            original = module.register
            def fail(ctx):
                original(ctx)
                raise RuntimeError('controlled registration failure')
            monkeypatch.setattr(module, 'register', fail)
        await asyncio.to_thread(manager.discover_and_load, force=True)
    adapter._running = True
    await until(lambda: all(t.done() for t in waiting))
    await asyncio.sleep(.05)
    assert not fresh.live and fresh.adapters == {}
    assert native.handlers.get(-20, []) == []
    assert not any('Runtime._recover' in repr(t.get_coro()) for t in asyncio.all_tasks())
    assert not any('Runtime.worker' in repr(t.get_coro()) for t in asyncio.all_tasks())


@pytest.mark.asyncio
async def test_failed_drain_stays_sticky_after_readiness(ep, monkeypatch):
    manager, module, _ = ep
    adapter, native = connecting(manager, module)
    def fail(*args, **kwargs):
        raise RuntimeError('controlled cleanup failure')
    monkeypatch.setattr(type(native), 'remove_handler', fail)
    await asyncio.to_thread(manager.discover_and_load, force=True)
    fresh = runtime(manager)
    await until(lambda: fresh.health()['telegram']['reason'] == 'not_connected')
    adapter._running = True
    await until(lambda: fresh.health()['telegram']['reason'] == 'drain_failed')
    assert fresh.adapters == {} and fresh.workers == {}
    newer_adapter, newer_native = adapter_for('telegram')
    newer_adapter._wire_plugin_handlers(newer_native)
    await asyncio.sleep(.05)
    assert fresh.adapters == {} and fresh.workers == {}
    assert newer_native.handlers.get(-20, []) == []
