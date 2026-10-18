"""Feedback limits, migration, policy and fencing contracts."""
import asyncio
import json
import sys
import time
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest

sys.path.append(str(Path(__file__).parent))
from test_deferred_questions import request, runtime, ControlledAdapter
from test_answer_feedback import answered


@pytest.mark.asyncio
async def test_rate_limit_respects_server_delay(installed, monkeypatch):
    monkeypatch.setenv('PTB_TIMEDELTA', '1')
    from telegram.error import RetryAfter
    rt = runtime(installed[0]); qid = answered(rt)
    bot = NS(edit_message_text=AsyncMock(side_effect=RetryAfter(60)))
    rt.adapters['telegram'] = (NS(bot=bot), ControlledAdapter.make('telegram'))
    before = time.time()
    await rt.answer_feedback(qid)
    assert rt.store.feedback(qid)['next_attempt'] >= before + 60


@pytest.mark.asyncio
async def test_permanent_bad_request_is_held(installed):
    from telegram.error import BadRequest
    rt = runtime(installed[0]); qid = answered(rt)
    bot = NS(edit_message_text=AsyncMock(side_effect=BadRequest('invalid message id SECRET')))
    rt.adapters['telegram'] = (NS(bot=bot), ControlledAdapter.make('telegram'))
    assert not await rt.answer_feedback(qid)
    assert rt.store.feedback(qid)['state'] == 'held'
    assert rt.store.feedback(qid)['diagnostic'] == 'edit_rejected'


@pytest.mark.asyncio
@pytest.mark.parametrize('terminal', ['expired', 'cancelled'])
async def test_cleanup_edits_status_and_removes_keyboard(installed, terminal):
    rt = runtime(installed[0])
    row = rt.store.create(request(), rt.platforms)
    rt.store.claim(row['id']); rt.store.receipt(row['id'], '42')
    if terminal == 'cancelled':
        rt.store.cancel(row['id'])
    else:
        with rt.store.connect() as db:
            r = dict(row['request']); r['expires_at'] = 0
            db.execute('UPDATE questions SET request=? WHERE id=?', (json.dumps(r), row['id']))
        rt.store.expire()
    bot = NS(edit_message_text=AsyncMock())
    rt.adapters['telegram'] = (NS(bot=bot), ControlledAdapter.make('telegram'))
    await rt.dispatch_feedback_once('telegram')
    assert bot.edit_message_text.call_args.kwargs['reply_markup'] is None
    assert bot.edit_message_text.call_args.kwargs['text'].endswith(
        'Question cancelled.' if terminal == 'cancelled' else 'The answer deadline has expired.')
    assert rt.store.feedback(row['id'])['state'] == 'done'


def test_additive_old_database_migration_is_repeatable(installed):
    rt = runtime(installed[0]); qid = answered(rt)
    with rt.store.connect() as db:
        db.execute('DROP TABLE feedback_edits')
        columns = [r['name'] for r in db.execute('PRAGMA table_info(questions)')]
    reopened = type(rt.store)(rt.store.home, rt.store.profile)
    assert reopened.feedback(qid)['state'] == 'pending'
    assert reopened.finish_feedback(qid, 1, 'done')
    reopened = type(rt.store)(rt.store.home, rt.store.profile)
    assert reopened.feedback(qid)['state'] == 'done'
    assert reopened.feedback(qid)['version'] == 1
    assert reopened.get(qid)['answer']['value'] == 'Bus'
    with reopened.connect() as db:
        assert columns == [r['name'] for r in db.execute('PRAGMA table_info(questions)')]


@pytest.mark.asyncio
async def test_stale_expiry_intent_cannot_overwrite_answer(installed):
    rt = runtime(installed[0]); qid = answered(rt)
    with rt.store.connect() as db:
        db.execute("UPDATE feedback_edits SET target_state='expired' WHERE question_id=?", (qid,))
    bot = NS(edit_message_text=AsyncMock())
    rt.adapters['telegram'] = (NS(bot=bot), ControlledAdapter.make('telegram'))
    assert not await rt.answer_feedback(qid)
    bot.edit_message_text.assert_not_called()
    assert rt.store.feedback(qid)['diagnostic'] == 'stale_intent'
    assert rt.store.get(qid)['answer']['value'] == 'Bus'


def test_version_fences_stale_completion(installed):
    rt = runtime(installed[0]); qid = answered(rt)
    with rt.store.connect() as db:
        db.execute('UPDATE feedback_edits SET version=version+1 WHERE question_id=?', (qid,))
    assert not rt.store.finish_feedback(qid, 1, 'done')
    assert not rt.store.finish_feedback(qid, 1, 'held', 'stale')
    assert rt.store.feedback(qid)['version'] == 2
    assert rt.store.feedback(qid)['state'] == 'pending'


@pytest.mark.asyncio
@pytest.mark.parametrize('denial', ['auth', 'route', 'disconnected', 'unloaded'])
async def test_feedback_rechecks_current_binding_policy(installed, denial):
    rt = runtime(installed[0]); qid = answered(rt)
    adapter = ControlledAdapter.make('telegram')
    if denial == 'auth':
        adapter.set_authorization_check(lambda *a, **k: False)
    if denial == 'route':
        adapter._owner_profile = 'different-profile'
    if denial == 'disconnected':
        adapter._running = False
    if denial == 'unloaded':
        rt.close()
    bot = NS(edit_message_text=AsyncMock())
    rt.adapters['telegram'] = (NS(bot=bot), adapter)
    assert not await rt.answer_feedback(qid)
    bot.edit_message_text.assert_not_called()
    assert rt.store.feedback(qid)['state'] == 'pending'


@pytest.mark.asyncio
async def test_feedback_failure_does_not_block_real_continuation(installed):
    rt = runtime(installed[0])
    row = rt.store.create(request(continue_agent=True), rt.platforms)
    rt.store.claim(row['id']); rt.store.receipt(row['id'], '42')
    assert rt.store.answer(row['id'], '2', row['request'], authorized=True) == 'answered'
    ready = asyncio.Event()
    async def inbound(event):
        assert event.internal and not event.allow_gateway_control
        ready.set()
    adapter = ControlledAdapter.make('telegram')
    adapter.set_message_handler(inbound)
    bot = NS(edit_message_text=AsyncMock(side_effect=RuntimeError('SECRET')))
    rt.adapters['telegram'] = (NS(bot=bot), adapter)
    await rt.dispatch_feedback_once('telegram')
    await rt.dispatch_answers_once('telegram')
    await asyncio.wait_for(ready.wait(), 3)
    assert rt.store.get(row['id'])['continuation_state'] == 'admitted'
    assert rt.store.feedback(row['id'])['state'] == 'held'


@pytest.mark.asyncio
async def test_slow_feedback_does_not_stall_worker_continuation(installed):
    rt = runtime(installed[0]); answered(rt)
    entered, release, ready = asyncio.Event(), asyncio.Event(), asyncio.Event()
    async def edit(**kwargs):
        entered.set(); await release.wait()
    async def inbound(event):
        ready.set()
    adapter = ControlledAdapter.make('telegram'); adapter.set_message_handler(inbound)
    rt.adapters['telegram'] = (NS(bot=NS(edit_message_text=AsyncMock(side_effect=edit))), adapter)
    task = asyncio.create_task(rt.worker('telegram'))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        row = rt.store.create(request(idempotency_key='second', continue_agent=True), rt.platforms)
        rt.store.claim(row['id']); rt.store.receipt(row['id'], '43')
        assert rt.store.answer(row['id'], '1', row['request'], authorized=True) == 'answered'
        rt.changed()  # A direct Store producer must signal the event-driven runtime.
        await asyncio.wait_for(ready.wait(), 2)
        async def admitted():
            while (await asyncio.to_thread(rt.store.get, row['id']))['continuation_state'] != 'admitted':
                await asyncio.sleep(.01)
        await asyncio.wait_for(admitted(), 2)
        assert rt.store.get(row['id'])['continuation_state'] == 'admitted'
    finally:
        release.set(); rt.live = False; task.cancel()
        await asyncio.gather(task, return_exceptions=True)
