"""Durable feedback regression tests against a real temporary ledger."""
import sys
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest

sys.path.append(str(Path(__file__).parent))
from test_deferred_questions import request, runtime, ControlledAdapter
from test_answer_feedback import answered


def test_answer_intent_survives_reopen(installed):
    rt = runtime(installed[0])
    qid = answered(rt)
    reopened = type(rt.store)(rt.store.home, rt.store.profile)
    pending = reopened.pending_feedback('telegram')
    assert [r['id'] for r in pending] == [qid]
    assert reopened.get(qid)['feedback_state'] == 'pending'
    assert reopened.answer(qid, '1', dict(reopened.get(qid)['request']), authorized=True) == 'already_answered'
    assert reopened.get(qid)['feedback_version'] == 1


@pytest.mark.asyncio
async def test_reopened_worker_retries_same_message_and_stops_after_ack(installed):
    rt = runtime(installed[0]); qid = answered(rt)
    bot = NS(edit_message_text=AsyncMock(side_effect=[TimeoutError('token=SECRET'), None]))
    rt.adapters['telegram'] = (NS(bot=bot), ControlledAdapter.make('telegram'))
    await rt.dispatch_feedback_once('telegram')
    intent = rt.store.feedback(qid)
    assert intent['state'] == 'pending' and intent['diagnostic'] == 'edit_timeout'
    assert intent['attempts'] == 1
    assert rt.store.path.with_name(f'feedback-{qid}.lock').exists()
    assert rt.store.pending_feedback('telegram') == []
    rt.store = type(rt.store)(rt.store.home, rt.store.profile)
    with rt.store.connect() as db:
        db.execute('UPDATE feedback_edits SET next_attempt=0')
    await rt.dispatch_feedback_once('telegram')
    await rt.dispatch_feedback_once('telegram')
    assert rt.store.feedback(qid)['state'] == 'done'
    assert not rt.store.path.with_name(f'feedback-{qid}.lock').exists()
    assert bot.edit_message_text.call_count == 2
    assert not rt.store.path.with_name(f'feedback-{qid}.lock').exists()
    assert {c.kwargs['message_id'] for c in bot.edit_message_text.call_args_list} == {42}


@pytest.mark.asyncio
async def test_vk_native_helper_envelope_ack(installed, monkeypatch):
    from plugins.platforms.vk.adapter import VKAdapter
    import plugins.platforms.vk.adapter as vk
    rt = runtime(installed[0]); qid = answered(rt, 'vk')
    wire = AsyncMock(return_value={'response': 1})
    monkeypatch.setattr(vk, '_http_json_async', wire)
    adapter = ControlledAdapter.make('vk')
    adapter.token = 'test-token'; adapter.api_version = '5.199'
    adapter._vk_method = VKAdapter._vk_method.__get__(adapter)
    rt.adapters['vk'] = (None, adapter)
    assert await rt.answer_feedback(qid)
    assert rt.store.feedback(qid)['state'] == 'done'
    assert not rt.store.path.with_name(f'feedback-{qid}.lock').exists()
    assert wire.call_args.args[0].endswith('/messages.edit')


@pytest.mark.asyncio
async def test_telegram_utf16_budget_preserves_full_answer(installed):
    rt = runtime(installed[0])
    row = rt.store.create(request(question='😀' * 600, options=[]), rt.platforms)
    rt.store.claim(row['id']); rt.store.receipt(row['id'], '42')
    value = '😀' * 4000
    assert rt.store.answer(row['id'], 'text:' + value, row['request'], authorized=True) == 'answered'
    bot = NS(edit_message_text=AsyncMock())
    rt.adapters['telegram'] = (NS(bot=bot), ControlledAdapter.make('telegram'))
    assert await rt.answer_feedback(row['id'])
    text = bot.edit_message_text.call_args.kwargs['text']
    assert len(text.encode('utf-16-le')) // 2 <= 4096
    assert 'Answer saved:' in text and text.endswith('…')
    assert rt.store.get(row['id'])['answer']['value'] == value


@pytest.mark.parametrize('terminal', ['expired', 'cancelled'])
def test_terminal_receipt_cleanup_is_durable(installed, terminal, monkeypatch):
    rt = runtime(installed[0])
    row = rt.store.create(request(), rt.platforms)
    rt.store.claim(row['id']); rt.store.receipt(row['id'], '42')
    if terminal == 'expired':
        import hermes_plugins.deferred_questions.store as storage
        monkeypatch.setattr(storage.time, 'time', lambda: row['request']['expires_at'] + 1)
        rt.store.expire()
    else:
        assert rt.store.cancel(row['id'])
    rt.store = type(rt.store)(rt.store.home, rt.store.profile)
    intent = rt.store.feedback(row['id'])
    assert intent['target_state'] == terminal and intent['message_id'] == '42'
    assert intent['state'] == 'pending'
    assert [r['id'] for r in rt.store.pending_feedback('telegram')] == [row['id']]


def test_answer_wins_over_later_expiry_and_cancel(installed, monkeypatch):
    rt = runtime(installed[0]); qid = answered(rt)
    import hermes_plugins.deferred_questions.store as storage
    expiry = rt.store.get(qid)['request']['expires_at']
    monkeypatch.setattr(storage.time, 'time', lambda: expiry + 1)
    rt.store.expire()
    assert not rt.store.cancel(qid)
    assert rt.store.get(qid)['state'] == 'answered'
    assert rt.store.feedback(qid)['target_state'] == 'answered'
    assert rt.store.feedback(qid)['version'] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('outcome,expected,code', [
    ('unchanged', 'done', 'already_current'),
    ('deleted', 'deleted', 'message_deleted'),
    ('unknown', 'held', 'edit_unknown'),
    ('rate_limit', 'pending', 'edit_transient'),
])
async def test_edit_outcomes_are_safe_and_never_resend(installed, outcome, expected, code):
    from telegram.error import BadRequest, RetryAfter
    errors = {'unchanged': BadRequest('Message is not modified'),
              'deleted': BadRequest('Message to edit not found'),
              'unknown': RuntimeError('SECRET'), 'rate_limit': RetryAfter(60)}
    rt = runtime(installed[0]); qid = answered(rt)
    bot = NS(edit_message_text=AsyncMock(side_effect=errors[outcome]))
    adapter = ControlledAdapter.make('telegram')
    rt.adapters['telegram'] = (NS(bot=bot), adapter)
    await rt.dispatch_feedback_once('telegram')
    intent = rt.store.feedback(qid)
    assert intent['state'] == expected and intent['diagnostic'] == code
    assert rt.store.path.with_name(f'feedback-{qid}.lock').exists() is (expected != 'done')
    assert 'SECRET' not in str(intent)
    assert rt.store.get(qid)['state'] == 'answered'
    await rt.dispatch_feedback_once('telegram')
    assert bot.edit_message_text.call_count == 1
    assert adapter.sent == []


@pytest.mark.asyncio
async def test_concurrent_reopened_runtimes_serialize_one_edit(installed):
    import asyncio
    rt = runtime(installed[0]); qid = answered(rt)
    entered, release = asyncio.Event(), asyncio.Event()
    async def edit(**kwargs):
        entered.set()
        await release.wait()
    bot = NS(edit_message_text=AsyncMock(side_effect=edit))
    binding = (NS(bot=bot), ControlledAdapter.make('telegram'))
    rt.adapters['telegram'] = binding
    other = type(rt)(rt.ctx, type(rt.store)(rt.store.home, rt.store.profile), rt.platforms)
    other.adapters['telegram'] = binding
    task = asyncio.create_task(rt.answer_feedback(qid))
    await entered.wait()
    lock_path = rt.store.path.with_name(f'feedback-{qid}.lock')
    inode = lock_path.stat().st_ino
    assert not await other.answer_feedback(qid)
    assert lock_path.stat().st_ino == inode
    release.set()
    assert await task
    assert not lock_path.exists()
    assert await other.answer_feedback(qid)
    assert not lock_path.exists()
    assert bot.edit_message_text.call_count == 1


@pytest.mark.asyncio
async def test_cancel_inflight_edit_reopens_as_retryable(installed):
    import asyncio
    rt = runtime(installed[0]); qid = answered(rt)
    entered = asyncio.Event()
    async def edit(**kwargs):
        entered.set()
        await asyncio.Event().wait()
    bot = NS(edit_message_text=AsyncMock(side_effect=edit))
    rt.adapters['telegram'] = (NS(bot=bot), ControlledAdapter.make('telegram'))
    task = asyncio.create_task(rt.answer_feedback(qid))
    await entered.wait(); task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    rt.store = type(rt.store)(rt.store.home, rt.store.profile)
    assert rt.store.feedback(qid)['state'] == 'pending'
    assert rt.store.path.with_name(f'feedback-{qid}.lock').exists()
    bot.edit_message_text.side_effect = None
    assert await rt.answer_feedback(qid)
    assert rt.store.feedback(qid)['state'] == 'done'
    assert not rt.store.path.with_name(f'feedback-{qid}.lock').exists()
