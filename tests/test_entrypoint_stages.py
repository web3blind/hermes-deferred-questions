"""Feedback drain and fault-stage isolation for the supported entrypoint."""
import asyncio

import pytest
from test_deferred_questions import runtime
from test_entrypoint_lifecycle import adapter_for
from test_entrypoint_edges import until


@pytest.mark.asyncio
async def test_owned_feedback_finalizer_is_part_of_handoff(ep, monkeypatch):
    manager, module, _ = ep
    old = runtime(manager)
    adapter, native = adapter_for('vk')
    adapter._wire_plugin_handlers(native)
    release, started, finalizing = asyncio.Event(), asyncio.Event(), asyncio.Event()
    async def feedback():
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            finalizing.set()
            await release.wait()
    task = old._spawn(feedback(), 'vk')
    old.feedback_workers['vk'] = task
    await started.wait()
    manager.discover_and_load(force=True)
    await finalizing.wait()
    fresh = runtime(manager)
    await asyncio.sleep(.03)
    assert fresh.health()['vk']['draining'] and fresh.adapters == {}
    release.set()
    await until(lambda: 'vk' in fresh.adapters)
    assert task.done()


@pytest.mark.asyncio
async def test_dispatch_stage_fault_does_not_block_continuation(ep, monkeypatch):
    manager, module, _ = ep
    rt = runtime(manager)
    reached = asyncio.Event()
    async def fail(platform):
        raise RuntimeError('recipient-private-secret')
    async def continuation(platform):
        reached.set()
    monkeypatch.setattr(rt, 'dispatch_once', fail)
    monkeypatch.setattr(rt, 'dispatch_answers_once', continuation)
    adapter, native = adapter_for('vk')
    adapter._wire_plugin_handlers(native)
    await asyncio.wait_for(reached.wait(), 2)
    assert rt.health()['vk']['reason'] == 'dispatch_stage_failed'
    assert 'private' not in repr(rt.health())


@pytest.mark.asyncio
async def test_cache_capacity_refuses_new_connection_instead_of_losing_barrier(ep, monkeypatch):
    manager, module, _ = ep
    rt = runtime(manager)
    adapter, native = adapter_for('vk')
    adapter._wire_plugin_handlers(native)
    monkeypatch.setattr(module.lifecycle, 'LIMIT', 1)
    other, other_native = adapter_for('telegram')
    with pytest.raises(RuntimeError, match='binding_cache_full'):
        rt.wire('telegram', other_native, other)
    assert 'telegram' not in rt.adapters
    assert rt.workers.get('telegram') is None
    assert len(module.lifecycle.CACHE) == 1


@pytest.mark.asyncio
async def test_replaced_recovery_waiter_finalizer_joins_prior_barrier(ep, monkeypatch):
    manager, module, _ = ep
    monkeypatch.setattr(module.lifecycle, 'DRAIN_TIMEOUT', 30.0)
    worker_started, worker_release = asyncio.Event(), asyncio.Event()
    waiter_started, waiter_release = asyncio.Event(), asyncio.Event()
    async def worker(self, platform):
        worker_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            if not self.live:
                await worker_release.wait()
    original = module.runtime.Runtime._await_wire
    async def waiter(self, *args):
        waiter_started.set()
        try:
            await original(self, *args)
        finally:
            if not self.live:
                await waiter_release.wait()
    monkeypatch.setattr(module.runtime.Runtime, 'worker', worker)
    monkeypatch.setattr(module.runtime.Runtime, '_await_wire', waiter)
    adapter, native = adapter_for('vk')
    adapter._wire_plugin_handlers(native)
    await worker_started.wait()
    await asyncio.to_thread(manager.discover_and_load, force=True)
    await waiter_started.wait()
    intermediate = runtime(manager)
    await asyncio.to_thread(manager.discover_and_load, force=True)
    fresh = runtime(manager)
    worker_release.set()
    await asyncio.sleep(.05)
    assert fresh.adapters == {}
    waiter_release.set()
    await until(lambda: 'vk' in fresh.adapters)
    assert all(t.done() for tasks in intermediate._tasks.values() for t in tasks)


@pytest.mark.asyncio
async def test_native_cleanup_failure_blocks_replacement_worker(ep, monkeypatch):
    manager, module, _ = ep
    adapter, native = adapter_for('telegram')
    adapter._wire_plugin_handlers(native)
    await asyncio.sleep(.02)
    def fail(*args, **kwargs):
        raise RuntimeError('private-cleanup-detail')
    monkeypatch.setattr(type(native), 'remove_handler', fail)
    await asyncio.to_thread(manager.discover_and_load, force=True)
    fresh = runtime(manager)
    await until(lambda: fresh.health()['telegram']['reason'] == 'drain_failed')
    assert fresh.adapters == {} and fresh.workers == {}
    assert len(native.handlers[-20]) == 1
    assert 'private-cleanup-detail' not in repr(fresh.health())
