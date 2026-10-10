"""Adversarial official entrypoint handoff boundaries."""
import asyncio
import gc
import weakref

import pytest
from test_deferred_questions import runtime, request
from test_entrypoint_lifecycle import adapter_for


async def until(predicate, timeout=5):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(.01)
    assert predicate()


@pytest.mark.asyncio
@pytest.mark.parametrize('replace_count', [1, 4])
async def test_quick_generations_share_one_drain_and_gc_old_runtime(ep, monkeypatch, replace_count):
    manager, module, _ = ep
    asyncio.get_running_loop().set_debug(True)
    monkeypatch.setattr(module.lifecycle, 'DRAIN_TIMEOUT', 30.0)
    old = runtime(manager)
    release, started = asyncio.Event(), asyncio.Event()
    active = 0
    high = 0
    async def worker(self, platform):
        nonlocal active, high
        active += 1
        high = max(high, active)
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            await release.wait()
            active -= 1
    monkeypatch.setattr(module.runtime.Runtime, 'worker', worker)
    adapter, native = adapter_for('telegram')
    adapter._wire_plugin_handlers(native)
    await started.wait()
    old_ref = weakref.ref(old)
    for _ in range(replace_count):
        await asyncio.to_thread(manager.discover_and_load, force=True)
        await asyncio.sleep(.02)
    fresh = runtime(manager)
    assert fresh.health()['telegram']['draining']
    assert len(native.handlers.get(-20, [])) == 0
    release.set()
    await until(lambda: 'telegram' in fresh.adapters)
    assert high == 1
    del old
    await asyncio.sleep(.05)
    gc.collect()
    assert old_ref() is None
    manager.unload()
    await asyncio.sleep(.05)
    assert active == 0


@pytest.mark.asyncio
async def test_cancellation_resistant_finalizer_times_out_sticky_fail_closed(ep, monkeypatch):
    manager, module, _ = ep
    old = runtime(manager)
    release, started, finalizing = asyncio.Event(), asyncio.Event(), asyncio.Event()
    async def worker(self, platform):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            finalizing.set()
            await release.wait()
    monkeypatch.setattr(module.runtime.Runtime, 'worker', worker)
    adapter, native = adapter_for('vk')
    adapter._wire_plugin_handlers(native)
    await started.wait()
    manager.discover_and_load(force=True)
    await finalizing.wait()
    fresh = runtime(manager)
    await until(lambda: fresh.health()['vk']['reason'] == 'drain_failed',
                timeout=module.lifecycle.DRAIN_TIMEOUT + 2)
    assert fresh.adapters == {} and fresh.workers == {}
    release.set()
    await asyncio.sleep(.05)
    adapter.rewire_plugin_handlers()
    await asyncio.sleep(.05)
    assert fresh.adapters == {}  # Late drain completion cannot turn false into true.
    assert all(t.done() for t in old.workers.values())


@pytest.mark.asyncio
@pytest.mark.parametrize('mode', ['disable', 'uninstall'])
async def test_unload_while_draining_never_resurrects(ep, monkeypatch, mode):
    manager, module, metadata = ep
    old = runtime(manager)
    release, started = asyncio.Event(), asyncio.Event()
    async def worker(self, platform):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            await release.wait()
    monkeypatch.setattr(module.runtime.Runtime, 'worker', worker)
    adapter, native = adapter_for('telegram')
    adapter._wire_plugin_handlers(native)
    await started.wait()
    manager.discover_and_load(force=True)
    await asyncio.sleep(.03)
    fresh = runtime(manager)
    if mode == 'disable':
        from pathlib import Path
        Path(old.store.home, 'config.yaml').write_text('plugins:\n  enabled: []\n')
    else:
        metadata.joinpath('entry_points.txt').unlink()
    manager.discover_and_load(force=True)
    release.set()
    await asyncio.sleep(.1)
    assert not fresh.live and fresh.adapters == {}
    assert native.handlers.get(-20, []) == []


@pytest.mark.asyncio
async def test_partial_wire_rolls_back_listener_when_worker_creation_fails(ep, monkeypatch):
    manager, module, _ = ep
    rt = runtime(manager)
    adapter, native = adapter_for('telegram')
    original = rt._spawn
    def fail(coro, platform):
        coro.close()
        raise RuntimeError('controlled')
    monkeypatch.setattr(rt, '_spawn', fail)
    with pytest.raises(RuntimeError):
        rt.wire('telegram', native, adapter)
    await asyncio.sleep(.03)
    assert rt.adapters == {} and native.handlers.get(-20, []) == []
    monkeypatch.setattr(rt, '_spawn', original)
    rt.wire('telegram', native, adapter)
    assert len(native.handlers[-20]) == 1
    assert rt.health()['telegram']['worker_alive']
    rt.wire('telegram', native, adapter)
    assert len(native.handlers[-20]) == 1


@pytest.mark.asyncio
async def test_native_replacement_drains_before_new_listener(ep, monkeypatch):
    manager, module, _ = ep
    rt = runtime(manager)
    adapter, native = adapter_for('telegram')
    _, replacement = adapter_for('telegram')
    rt.wire('telegram', native, adapter)
    await asyncio.sleep(.01)
    old_task = rt.workers['telegram']
    rt.wire('telegram', replacement, adapter)
    await until(lambda: rt.adapters.get('telegram', (None,))[0] is replacement)
    assert old_task.done()
    assert native.handlers.get(-20, []) == []
    assert len(replacement.handlers[-20]) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('ack', [True, False])
async def test_old_inflight_receipt_is_preserved_without_resend(ep, monkeypatch, ack):
    manager, module, _ = ep
    rt = runtime(manager)
    adapter, native = adapter_for('vk')
    adapter.set_authorization_check(lambda *a, **kw: True)
    release, started = asyncio.Event(), asyncio.Event()
    sends = 0
    async def send(self, *args):
        nonlocal sends
        sends += 1
        started.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            await release.wait()
        return module.runtime.Delivery('42') if ack else None
    monkeypatch.setattr(module.runtime.Runtime, 'send', send)
    row = rt.store.create(request('vk'), rt.platforms)
    adapter._wire_plugin_handlers(native)
    await asyncio.wait_for(started.wait(), 2)
    manager.discover_and_load(force=True)
    fresh = runtime(manager)
    release.set()
    await until(lambda: 'vk' in fresh.adapters)
    await asyncio.sleep(.05)
    assert rt.store.get(row['id'])['state'] == ('sent' if ack else 'unknown')
    assert sends == 1


@pytest.mark.asyncio
async def test_failed_registration_cannot_recover_cached_connection(ep, monkeypatch):
    manager, module, _ = ep
    adapter, native = adapter_for('vk')
    adapter._wire_plugin_handlers(native)
    await asyncio.sleep(.02)
    original = module.register
    def failed(ctx):
        original(ctx)
        raise RuntimeError('controlled registration failure')
    monkeypatch.setattr(module, 'register', failed)
    await asyncio.to_thread(manager.discover_and_load, force=True)
    await asyncio.sleep(.1)
    assert not manager._plugins['deferred-questions'].enabled
    assert not [t for t in asyncio.all_tasks() if 'Runtime.worker' in repr(t.get_coro())]


@pytest.mark.asyncio
async def test_timed_out_registration_remains_inert_after_loader_finishes(ep, monkeypatch):
    import threading
    from pathlib import Path
    manager, module, _ = ep
    adapter, native = adapter_for('vk')
    adapter._wire_plugin_handlers(native)
    await asyncio.sleep(.02)
    rt = runtime(manager)
    Path(rt.store.home, 'config.yaml').write_text(
        'plugins:\n  enabled: [deferred-questions]\n  load_timeout_seconds: 0.1\n')
    original = module.register
    release, finished = threading.Event(), threading.Event()
    def slow(ctx):
        try:
            original(ctx)
            release.wait(2)
        finally:
            finished.set()
    monkeypatch.setattr(module, 'register', slow)
    await asyncio.to_thread(manager.discover_and_load, force=True)
    assert not manager._plugins['deferred-questions'].enabled
    release.set()
    assert await asyncio.to_thread(finished.wait, 2)
    await asyncio.sleep(.1)
    assert not [t for t in asyncio.all_tasks() if 'Runtime.worker' in repr(t.get_coro())]


@pytest.mark.asyncio
async def test_sdk_partial_add_is_rolled_back_on_owning_loop(ep, monkeypatch):
    manager, module, _ = ep
    rt = runtime(manager)
    adapter, native = adapter_for('telegram')
    original = native.add_handler
    def partial(*a, **kw):
        original(*a, **kw)
        raise RuntimeError('controlled partial add')
    monkeypatch.setattr(type(native), 'add_handler', lambda self, *a, **kw: partial(*a, **kw))
    with pytest.raises(RuntimeError):
        rt.wire('telegram', native, adapter)
    await asyncio.sleep(.01)
    assert native.handlers.get(-20, []) == [] and rt.adapters == {}


@pytest.mark.asyncio
async def test_same_import_package_keeps_homes_isolated(ep, monkeypatch, tmp_path):
    from hermes_cli.plugins import PluginManager
    manager, module, _ = ep
    first, _ = adapter_for('vk')
    first._wire_plugin_handlers(first)
    home = tmp_path / 'secondary'
    home.mkdir()
    home.joinpath('config.yaml').write_text('plugins:\n  enabled: [deferred-questions]\n')
    second = PluginManager(scope_key=str(home))
    second.discover_and_load()
    secondary = runtime(second)
    adapter, native = adapter_for('vk')
    secondary.wire('vk', native, adapter)
    await asyncio.sleep(.02)
    try:
        await asyncio.to_thread(manager.discover_and_load, force=True)
        await until(lambda: 'vk' in runtime(manager).adapters)
        assert runtime(manager).adapters['vk'][1] is first
        assert runtime(second) is secondary and secondary.live
        assert secondary.adapters['vk'][1] is adapter
    finally:
        second.unload()
        await asyncio.sleep(.03)


@pytest.mark.asyncio
async def test_stopped_loop_receipt_is_false_and_has_no_new_effects(ep):
    manager, module, _ = ep
    loop = asyncio.new_event_loop()
    try:
        receipt = module.lifecycle.drain(loop, (), ())
        assert receipt.done() and receipt.result() is False
    finally:
        loop.close()
