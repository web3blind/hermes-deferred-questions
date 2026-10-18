"""No polling: a real local notice wakes persisted work; no external messages."""
import asyncio
import importlib.util
from pathlib import Path
import socket

import pytest

spec = importlib.util.spec_from_file_location('dq_notify_under_test',
    Path(__file__).resolve().parents[1] / 'hermes_deferred_questions' / 'notify.py')
n = importlib.util.module_from_spec(spec)
spec.loader.exec_module(n)


@pytest.mark.asyncio
async def test_cross_process_style_notice_and_idle(tmp_path):
    (tmp_path/'deferred-questions').mkdir()
    notices = []
    endpoint = n.Endpoint(tmp_path, lambda: notices.append('wake'))
    await endpoint.start()
    await asyncio.sleep(.03)
    assert notices == []
    assert await asyncio.to_thread(n.notify, tmp_path)
    assert notices == ['wake']
    endpoint.close()
    await asyncio.sleep(0)
    assert not endpoint.path.exists()
    assert not await asyncio.to_thread(n.notify, tmp_path)


@pytest.mark.asyncio
async def test_listener_lease_prevents_replacement(tmp_path):
    (tmp_path/'deferred-questions').mkdir()
    one = n.Endpoint(tmp_path, lambda: None)
    await one.start()
    two = n.Endpoint(tmp_path, lambda: None)
    before = one.path.stat().st_ino
    with pytest.raises(BlockingIOError):
        await two.start()
    assert one.path.stat().st_ino == before
    assert await asyncio.to_thread(n.notify, tmp_path)
    one.close()
    await asyncio.sleep(0)


def test_missing_gateway_reports_queued_not_notified(tmp_path):
    assert n.notify(tmp_path) is False


@pytest.mark.asyncio
async def test_concurrent_starters_share_one_ready_listener(tmp_path):
    (tmp_path/'deferred-questions').mkdir()
    endpoint = n.Endpoint(tmp_path, lambda: None)
    await asyncio.gather(endpoint.start(), endpoint.start())
    assert endpoint.ready.done()
    assert await asyncio.to_thread(n.notify, tmp_path)
    endpoint.close()
    await asyncio.sleep(0)
    assert n.notify(tmp_path) is False


@pytest.mark.asyncio
async def test_close_fences_inflight_start(tmp_path, monkeypatch):
    (tmp_path/'deferred-questions').mkdir()
    endpoint = n.Endpoint(tmp_path, lambda: None)
    entered = asyncio.Event()
    parked = asyncio.Event()
    original = n.asyncio.start_unix_server
    async def delayed(*args, **kwargs):
        entered.set()
        await parked.wait()
        return await original(*args, **kwargs)
    monkeypatch.setattr(n.asyncio, 'start_unix_server', delayed)
    task = asyncio.create_task(endpoint.start())
    await entered.wait()
    endpoint.close()
    await asyncio.gather(task, return_exceptions=True)
    assert endpoint.lease is None
    assert not (tmp_path/'deferred-questions/events-address.json').exists()



@pytest.mark.asyncio
async def test_starters_on_different_threads_keep_one_owner_loop(tmp_path):
    import threading
    (tmp_path/'deferred-questions').mkdir()
    endpoint = n.Endpoint(tmp_path, lambda: None)
    stop = threading.Event()
    barrier = threading.Barrier(2)
    loops, errors = [], []
    def runner():
        async def run():
            await asyncio.to_thread(barrier.wait, 2)
            await endpoint.start()
            loops.append(asyncio.get_running_loop())
            await asyncio.to_thread(stop.wait)
        try:
            asyncio.run(run())
        except BaseException as error:
            errors.append(error)
    threads = [threading.Thread(target=runner) for _ in range(2)]
    for thread in threads: thread.start()
    try:
        async with asyncio.timeout(5):
            while len(loops) != 2 and not errors:
                await asyncio.sleep(.01)
        assert not errors
        assert endpoint.loop in loops
        assert await asyncio.to_thread(n.notify, tmp_path)
        endpoint.close()
        async with asyncio.timeout(5):
            while endpoint.lease is not None:
                await asyncio.sleep(.01)
    finally:
        endpoint.close()
        stop.set()
        for thread in threads:
            await asyncio.to_thread(thread.join, 3)
    assert not errors
