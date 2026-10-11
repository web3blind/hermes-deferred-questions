"""Answer feedback uses persisted answers, never untrusted click labels."""
import sys
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest
sys.path.append(str(Path(__file__).parent))
from test_deferred_questions import request, runtime, ControlledAdapter


def answered(rt, platform='telegram', message_id='42'):
    row = rt.store.create(request(platform), rt.platforms)
    rt.store.claim(row['id'])
    rt.store.receipt(row['id'], message_id)
    route = dict(row['request'])
    assert rt.store.answer(row['id'], '2', route, authorized=True) == 'answered'
    return row['id']


@pytest.mark.asyncio
async def test_telegram_answer_edit_removes_keyboard(installed):
    rt = runtime(installed[0])
    qid = answered(rt)
    bot = NS(edit_message_text=AsyncMock())
    adapter = ControlledAdapter.make('telegram')
    rt.adapters['telegram'] = (NS(bot=bot), adapter)
    assert await rt.answer_feedback(qid) is True
    data = bot.edit_message_text.call_args.kwargs
    assert data['chat_id'] == '123' and data['message_id'] == 42
    assert data['text'] == 'Which route today?\n\nAnswer saved: 2. Bus'
    assert data['reply_markup'] is None and data['parse_mode'] is None
    assert rt.store.get(qid)['answer']['value'] == 'Bus'


@pytest.mark.asyncio
@pytest.mark.parametrize('message_id,key,value', [('42','message_id','42'),('cmid:7','conversation_message_id','7')])
async def test_vk_feedback_uses_receipt_and_empty_keyboard(installed, message_id, key, value):
    import json
    rt = runtime(installed[0]); qid = answered(rt, 'vk', message_id)
    adapter = ControlledAdapter.make('vk'); adapter._vk_method = AsyncMock(return_value=1)
    rt.adapters['vk'] = (None, adapter)
    assert await rt.answer_feedback(qid) is True
    method, data = adapter._vk_method.call_args.args
    assert method == 'messages.edit'
    assert data[key] == value and data['peer_id'] == '123'
    assert json.loads(data['keyboard'])['buttons'] == []
    assert data['message'].endswith('Answer saved: 2. Bus')


@pytest.mark.asyncio
async def test_feedback_failure_does_not_lose_answer(installed):
    rt = runtime(installed[0]); qid = answered(rt)
    bot = NS(edit_message_text=AsyncMock(side_effect=TimeoutError()))
    rt.adapters['telegram'] = (NS(bot=bot), ControlledAdapter.make('telegram'))
    assert await rt.answer_feedback(qid) is False
    assert rt.store.get(qid)['state'] == 'answered'
    assert rt.store.get(qid)['answer']['value'] == 'Bus'


@pytest.mark.asyncio
async def test_no_feedback_to_unauthorized_recipient(installed):
    rt = runtime(installed[0]); qid = answered(rt)
    bot = NS(edit_message_text=AsyncMock())
    adapter = ControlledAdapter.make('telegram')
    adapter.set_authorization_check(lambda *a, **k: False)
    rt.adapters['telegram'] = (NS(bot=bot), adapter)
    assert await rt.answer_feedback(qid) is False
    bot.edit_message_text.assert_not_called()
