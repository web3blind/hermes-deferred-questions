"""Opt-in answer continuation, using the real base-adapter admission path."""
import asyncio
import sys
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
sys.path.insert(0, str(Path(__file__).parent))
from test_deferred_questions import request, runtime, ControlledAdapter


def make_answer(rt, continue_agent=True):
    row = rt.store.create(request('signal', continue_agent=continue_agent), rt.platforms)
    rt.store.claim(row['id']); rt.store.receipt(row['id'], '42')
    assert rt.store.answer(row['id'], '2', row['request'], authorized=True) == 'answered'
    return row


@pytest.mark.asyncio
async def test_real_admission_once_after_store_reopen(installed):
    rt = runtime(installed[0]); row = make_answer(rt)
    original = rt.store
    rt.store = type(original)(original.home, original.profile)
    adapter = ControlledAdapter.make('signal')
    seen = []
    ready = asyncio.Event()
    async def inbound(event):
        seen.append(event); ready.set(); return None
    adapter.set_message_handler(inbound)
    rt.adapters['signal'] = (None, adapter)
    await rt.dispatch_answers_once('signal')
    await asyncio.wait_for(ready.wait(), 3)
    await rt.dispatch_answers_once('signal')
    assert len(seen) == 1
    event = seen[0]
    assert 'Ответ: Bus' in event.text and 'Which route today?' in event.text
    assert event.source.chat_id == '123' and event.source.user_id == '456'
    assert event.internal is True and event.allow_gateway_control is False
    assert event.metadata['deferred_question_id'] == row['id']
    assert rt.store.pending_actions('signal') == []
    assert rt.store.answer(row['id'], '1', row['request'], authorized=True) == 'already_answered'
    assert rt.store.pending_actions('signal') == []


@pytest.mark.asyncio
async def test_disabled_continuation_is_storage_only(installed):
    rt = runtime(installed[0]); make_answer(rt, False)
    assert rt.store.pending_actions('signal') == []


@pytest.mark.asyncio
async def test_unauthorized_continuation_keeps_pending(installed):
    rt = runtime(installed[0]); make_answer(rt)
    adapter = ControlledAdapter.make('signal'); adapter.set_authorization_check(lambda *a, **k: False)
    adapter.handle_message = AsyncMock()
    rt.adapters['signal'] = (None, adapter)
    await rt.dispatch_answers_once('signal')
    adapter.handle_message.assert_not_called()
    assert len(rt.store.pending_actions('signal')) == 1


@pytest.mark.asyncio
async def test_no_handler_does_not_claim_success(installed):
    rt = runtime(installed[0]); make_answer(rt)
    adapter = ControlledAdapter.make('signal')
    rt.adapters['signal'] = (None, adapter)
    await rt.dispatch_answers_once('signal')
    assert len(rt.store.pending_actions('signal')) == 1


@pytest.mark.asyncio
async def test_uncertain_admission_held_no_blind_retry(installed):
    rt = runtime(installed[0]); make_answer(rt)
    adapter = ControlledAdapter.make('signal')
    adapter.handle_message = AsyncMock(side_effect=TimeoutError())
    rt.adapters['signal'] = (None, adapter)
    await rt.dispatch_answers_once('signal')
    await rt.dispatch_answers_once('signal')
    assert adapter.handle_message.await_count == 1
    assert rt.store.pending_actions('signal') == []
