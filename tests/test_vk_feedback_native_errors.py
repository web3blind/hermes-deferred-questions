"""Native HTTP failure classifications, not fabricated adapter envelopes."""
import json
import sys
import urllib.error
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.append(str(Path(__file__).parent))
from test_deferred_questions import runtime, ControlledAdapter
from test_answer_feedback import answered


@pytest.mark.asyncio
@pytest.mark.parametrize('failure,retryable', [
    (6, True), (9, True), (10, True), (29, True),
    (5, False), (15, False), ('dns', True),
    ('http429', True), ('http403', False), ('malformed', False),
])
async def test_real_vk_http_error_recovery(installed, monkeypatch, failure, retryable):
    from plugins.platforms.vk.adapter import VKAdapter
    import plugins.platforms.vk.adapter as vk
    rt = runtime(installed[0]); qid = answered(rt, 'vk')
    adapter = ControlledAdapter.make('vk')
    adapter.token = 'test-only-token'; adapter.api_version = '5.199'
    adapter._vk_method = VKAdapter._vk_method.__get__(adapter)
    rt.adapters['vk'] = (None, adapter)
    if failure == 'dns':
        def urlopen(*args, **kwargs):
            raise urllib.error.URLError('temporary DNS test-only-token')
    elif failure in ('http429', 'http403'):
        def urlopen(*args, **kwargs):
            raise urllib.error.HTTPError('https://api.vk.com/method/messages.edit',
                                         int(failure[4:]), 'test-only-token', {}, None)
    else:
        response = MagicMock()
        response.__enter__.return_value = response
        body = 'not JSON test-only-token' if failure == 'malformed' else json.dumps(
            {'error': {'error_code': failure, 'error_msg': 'test-only-token'}})
        response.read.return_value = body.encode()
        def urlopen(*args, **kwargs):
            return response
    monkeypatch.setattr(vk.urllib.request, 'urlopen', urlopen)
    await rt.dispatch_feedback_once('vk')
    intent = rt.store.feedback(qid)
    assert intent['state'] == ('pending' if retryable else 'held')
    assert 'test-only-token' not in str(intent)
    assert rt.store.get(qid)['state'] == 'answered'
    reopened = type(rt.store)(rt.store.home, rt.store.profile)
    assert reopened.feedback(qid)['state'] == intent['state']
    assert intent['next_attempt'] > 0 if retryable else True
    if retryable:
        response = MagicMock(); response.__enter__.return_value = response
        response.read.return_value = b'{"response": 1}'
        monkeypatch.setattr(vk.urllib.request, 'urlopen', lambda *a, **kw: response)
        with reopened.connect() as db:
            db.execute('UPDATE feedback_edits SET next_attempt=0 WHERE question_id=?', (qid,))
        rt.store = reopened
        await rt.dispatch_feedback_once('vk')
        assert reopened.feedback(qid)['state'] == 'done'
