"""Event-driven runtime, exercised through real registration and CLI."""
import asyncio
import json
import os
import subprocess
import sys
from unittest.mock import Mock
import pytest
from test_deferred_questions import CORE, ControlledAdapter, request, runtime

async def until(predicate):
    import sqlite3
    async with asyncio.timeout(5):
        while True:
            try:
                if predicate():
                    return
            except sqlite3.OperationalError as exc:
                if 'store busy' not in str(exc):
                    raise
            await asyncio.sleep(.01)

@pytest.mark.asyncio
async def test_no_idle_database_polling(installed, monkeypatch):
    manager, module, home = installed
    rt = runtime(manager)
    spies = []
    for name in ('queued', 'pending_actions', 'pending_feedback'):
        spy = Mock(wraps=getattr(rt.store, name))
        monkeypatch.setattr(rt.store, name, spy)
        spies.append(spy)
    adapter = ControlledAdapter.make('signal')
    rt.wire('signal', None, adapter)
    await until(lambda: all(s.call_count for s in spies))
    counts = [s.call_count for s in spies]
    await asyncio.sleep(1.15)
    assert [s.call_count for s in spies] == counts
    rt.changed()
    await until(lambda: spies[0].call_count > counts[0])
    manager.unload()
    await asyncio.sleep(.02)

@pytest.mark.asyncio
async def test_cli_notice_delivers_without_polling(installed, tmp_path):
    manager, module, home = installed
    rt = runtime(manager)
    adapter = ControlledAdapter.make('signal')
    rt.wire('signal', None, adapter)
    await until(lambda: (home/'deferred-questions/events-address.json').exists())
    path = tmp_path/'request.json'
    path.write_text(json.dumps(request('signal')))
    env = dict(os.environ, HERMES_HOME=str(home), HOME=str(home.parent), PYTHONPATH=str(CORE))
    command = [sys.executable, '-m', 'hermes_cli.main', 'deferred-questions', 'request', '--json-file', str(path)]
    done = await asyncio.to_thread(subprocess.run, command, cwd=CORE, env=env, capture_output=True, text=True, timeout=45)
    assert done.returncode == 0, done.stdout + done.stderr
    result = json.loads(done.stdout.strip().splitlines()[-1])
    assert result['result']['notification_sent'] is True
    await until(lambda: rt.store.get(result['result']['id'])['state'] == 'sent')
    assert len(adapter.sent) == 1
    manager.unload()
    await asyncio.sleep(.02)

@pytest.mark.asyncio
async def test_offline_saved_question_recovers_on_binding(installed):
    manager, module, home = installed
    rt = runtime(manager)
    row = rt.store.create(request('signal'), rt.platforms)
    assert rt.changed() is False
    adapter = ControlledAdapter.make('signal')
    rt.wire('signal', None, adapter)
    await until(lambda: rt.store.get(row['id'])['state'] == 'sent')
    assert len(adapter.sent) == 1
    manager.unload()
    await asyncio.sleep(.02)


@pytest.mark.asyncio
async def test_notice_during_feedback_await_gets_one_followup_pass(installed, monkeypatch):
    manager, module, home = installed
    rt = runtime(manager)
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = []
    async def feedback(platform):
        calls.append(platform)
        if len(calls) == 1:
            entered.set()
            await release.wait()
    monkeypatch.setattr(rt, 'dispatch_feedback_once', feedback)
    rt.wire('signal', None, ControlledAdapter.make('signal'))
    await entered.wait()
    rt.changed()
    await until(lambda: 'signal' in rt._feedback_dirty)
    release.set()
    await until(lambda: len(calls) == 2)
    await asyncio.sleep(.15)
    assert len(calls) == 2
    manager.unload()
    await asyncio.sleep(.02)


@pytest.mark.asyncio
async def test_early_native_factory_waits_for_connection_without_db_polling(installed, monkeypatch):
    manager, module, home = installed
    rt = runtime(manager)
    row = rt.store.create(request('signal'), rt.platforms)
    adapter = ControlledAdapter.make('signal')
    adapter._running = False
    spy = Mock(wraps=rt.store.queued)
    monkeypatch.setattr(rt.store, 'queued', spy)
    rt.wire('signal', None, adapter)
    await asyncio.sleep(.05)
    assert spy.call_count == 0
    adapter._running = True
    await until(lambda: len(adapter.sent) == 1)
    await until(lambda: rt.store.get(row['id'])['state'] == 'sent')
    assert rt.store.get(row['id'])['state'] == 'sent'
    manager.unload()
    await asyncio.sleep(.02)


@pytest.mark.asyncio
async def test_store_lease_release_wakes_once_without_timer(installed, monkeypatch):
    manager, module, home = installed
    rt = runtime(manager)
    spy = Mock(wraps=rt.store.queued)
    monkeypatch.setattr(rt.store, 'queued', spy)
    rt.wire('signal', None, ControlledAdapter.make('signal'))
    await until(lambda: spy.call_count > 0)
    await asyncio.sleep(.02)
    with rt.store.connect():
        rt.changed()
        await until(lambda: (home/'deferred-questions/store-wake').exists())
        before = spy.call_count
    await until(lambda: spy.call_count > before)
    await asyncio.sleep(.05)
    count = spy.call_count
    await asyncio.sleep(.15)
    assert spy.call_count == count
    assert not (home/'deferred-questions/store-wake').exists()
    manager.unload()
    await asyncio.sleep(.02)


@pytest.mark.asyncio
async def test_replacement_binding_uses_candidate_loop(installed, monkeypatch):
    from concurrent.futures import Future
    import threading
    manager, module, home = installed
    rt = runtime(manager)
    other = asyncio.new_event_loop()
    thread = threading.Thread(target=other.run_forever)
    thread.start()
    seen = []
    called = threading.Event()
    adapter = ControlledAdapter.make('signal')
    rt._desired['signal'] = (None, adapter, other)
    def wire(*args):
        seen.append(asyncio.get_running_loop())
        called.set()
    monkeypatch.setattr(rt, 'wire', wire)
    receipt = Future(); receipt.set_result(True)
    try:
        await rt._await_wire('signal', None, adapter, receipt)
        assert await asyncio.to_thread(called.wait, 2)
        assert seen == [other]
    finally:
        other.call_soon_threadsafe(other.stop)
        await asyncio.to_thread(thread.join, 2)
        other.close()
        manager.unload()


@pytest.mark.asyncio
async def test_ack_receipt_survives_store_contention_and_cancel(installed, monkeypatch):
    from unittest.mock import AsyncMock
    manager, module, home = installed
    rt = runtime(manager)
    adapter = ControlledAdapter.make('signal')
    rt.adapters['signal'] = (None, adapter)
    row = rt.store.create(request('signal'), rt.platforms)
    assert rt.store.claim(row['id'])
    from importlib import import_module
    implementation = import_module(type(rt).__module__)
    transport = AsyncMock(return_value=implementation.Delivery('ack-42'))
    monkeypatch.setattr(rt, 'send', transport)
    with rt.store.connect():
        task = asyncio.create_task(rt._deliver('signal', None, adapter, row))
        await until(lambda: transport.await_count == 1)
        await until(lambda: (home/'deferred-questions/store-wake').exists())
        task.cancel()
        await asyncio.sleep(.02)
        assert not task.done()
    await asyncio.gather(task, return_exceptions=True)
    receipt = rt.store.get(row['id'])
    assert receipt['state'] == 'sent' and receipt['message_id'] == 'ack-42'
    assert transport.await_count == 1
    manager.unload()
