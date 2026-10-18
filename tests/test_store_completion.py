"""Real SQLite tests; no installed plugin, gateway or network needed."""
import importlib.util
from pathlib import Path
import sqlite3
import time

import pytest

spec = importlib.util.spec_from_file_location('dq_store_completion', Path(__file__).resolve().parents[1] / 'hermes_deferred_questions/store.py')
assert spec and spec.loader
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
Store = mod.Store


@pytest.fixture
def store(tmp_path, monkeypatch):
    monkeypatch.setenv('HOME', str(tmp_path))
    monkeypatch.setenv('HERMES_HOME', str(tmp_path / 'hermes'))
    return Store(tmp_path / 'hermes', 'default')


def request(key='one', platform='telegram'):
    return dict(platform=platform, chat_id='123', user_id='456', chat_type='dm',
                question='Private question', options=['Private answer', 'No'],
                expires_at=time.time() + 3600, idempotency_key=key, continue_agent=True)


def answered(store, key='one', platform='telegram'):
    req = request(key, platform)
    row = store.create(req, {'telegram', 'vk'})
    qid = row['id']
    assert store.claim(qid)
    store.receipt(qid, 'message-1')
    assert store.answer(qid, '1', store.get(qid)['request'], authorized=True) == 'answered'
    return qid, req


def feedback(store, qid, outcome='done'):
    row = store.get(qid)
    assert store.finish_feedback(qid, row['feedback_version'], outcome)


def finish(store, turn='turn-1'):
    return store.finish_turn(turn, completed=True, failed=False, interrupted=False)


def test_restart_exact_binding_and_cleanup(store):
    qid, req = answered(store)
    token = store.prepare_continuation(qid)
    assert token and token != qid
    store = Store(store.home, store.profile)
    assert store.prepare_continuation(qid) == token
    assert store.claim_action(qid)
    store.finish_action(qid, 'admitted')
    assert store.bind_turn(token, 'turn-1')
    assert not store.bind_turn(token, 'turn-2')
    assert not store.bind_turn(token, 'turn-1')
    assert not finish(store, 'wrong-turn')
    assert finish(Store(store.home, store.profile))
    assert not store.get(qid).get('tombstone')
    feedback(store, qid)
    tomb = Store(store.home, store.profile).get(qid)
    assert tomb['tombstone'] and tomb['state'] == 'answered'
    assert 'request' not in tomb and 'answer' not in tomb and 'snapshot' not in tomb
    assert store.create(req, {'telegram', 'vk'}) == tomb
    assert store.list() == [tomb]
    assert not finish(store)
    with store.connect() as db:
        for table in ('questions', 'answer_actions', 'feedback_edits'):
            assert db.execute('SELECT COUNT(*) FROM ' + table).fetchone()[0] == 0
    with pytest.raises(ValueError, match='different request'):
        store.create(dict(req, question='Changed'), {'telegram', 'vk'})


@pytest.mark.parametrize('outcome', ['done', 'deleted'])
def test_feedback_before_completion(store, outcome):
    qid, _ = answered(store)
    token = store.prepare_continuation(qid)
    feedback(store, qid, outcome)
    assert store.get(qid)['request']
    assert store.bind_turn(token, 'turn-1')
    assert finish(store)
    assert store.get(qid)['tombstone']


@pytest.mark.parametrize('flags', [{}, {'completed': True}, {'completed': True, 'failed': False},
    {'completed': True, 'failed': False, 'interrupted': True},
    {'completed': True, 'failed': True, 'interrupted': False},
    {'completed': 1, 'failed': False, 'interrupted': False},
    {'completed': True, 'failed': 0, 'interrupted': False}])
def test_missing_or_non_success_flags_never_cleanup(store, flags):
    qid, _ = answered(store)
    token = store.prepare_continuation(qid)
    feedback(store, qid)
    assert store.bind_turn(token, 'turn-1')
    assert not store.finish_turn('turn-1', **flags)
    assert store.get(qid)['request']
    assert finish(store)


def test_turn_cannot_belong_to_two_questions(store):
    first, _ = answered(store)
    second, _ = answered(store, 'second')
    a = store.prepare_continuation(first)
    b = store.prepare_continuation(second)
    assert store.bind_turn(a, 'turn-1')
    assert not store.bind_turn(b, 'turn-1')
    assert not store.bind_turn('unknown-token', 'turn-2')
    assert not store.bind_turn(b, '')
    feedback(store, first)
    assert finish(store)
    assert not store.bind_turn(b, 'turn-1')
    assert store.bind_turn(b, 'turn-2')
    assert store.get(second)['request']


def test_legacy_admission_without_token_is_retained(store):
    qid, _ = answered(store)
    assert store.claim_action(qid)
    store.finish_action(qid, 'admitted')
    feedback(store, qid)
    assert store.prepare_continuation(qid) is None
    assert not finish(store)
    Store(store.home, store.profile).prune()
    assert store.get(qid)['request']


def test_vk_nonce_not_reused_after_full_deletion(store):
    qid, _ = answered(store, platform='vk')
    nonce = store.get(qid)['delivery_nonce']
    token = store.prepare_continuation(qid)
    assert store.bind_turn(token, 'turn-1')
    feedback(store, qid)
    assert finish(store)
    other = Store(store.home, store.profile).create(request('second', 'vk'), {'telegram', 'vk'})
    assert other['delivery_nonce'] > nonce


def test_legacy_nonce_migration_preserves_rowid(tmp_path):
    (tmp_path / 'deferred-questions').mkdir()
    path = tmp_path / 'deferred-questions/questions.sqlite3'
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE questions (id TEXT PRIMARY KEY, idem TEXT UNIQUE NOT NULL, request TEXT NOT NULL, snapshot TEXT NOT NULL, state TEXT NOT NULL, created REAL NOT NULL, message_id TEXT, answer TEXT, diagnostic TEXT)')
        req = request(platform='vk')
        db.execute('INSERT INTO questions(rowid,id,idem,request,snapshot,state,created) VALUES(77,?,?,?,?,?,?)',
                   ('a' * 32, req['idempotency_key'], mod.dumps(req), '{}', 'queued', time.time()))
    store = Store(tmp_path, 'default')
    assert store.get('a' * 32)['delivery_nonce'] == 77
    assert store.create(request('second', 'vk'), {'telegram', 'vk'})['delivery_nonce'] == 78
    assert Store(tmp_path, 'default').get('a' * 32)['delivery_nonce'] == 77


def test_completion_during_dispatch_cannot_be_resurrected(store):
    qid, _ = answered(store)
    assert store.claim_action(qid)
    token = store.prepare_continuation(qid)
    assert store.bind_turn(token, 'turn-1')
    feedback(store, qid)
    assert finish(store)
    store.finish_action(qid, 'admitted')
    assert store.get(qid)['compacted'] is True
    assert store.feedback(qid) is None
    with store.connect() as db:
        assert not db.execute('SELECT 1 FROM answer_actions').fetchone()


def test_feedback_retry_retains_data_and_completion_time(store):
    qid, _ = answered(store)
    token = store.prepare_continuation(qid)
    assert store.bind_turn(token, 'turn-1')
    before = time.time()
    assert finish(store)
    completed_before = time.time()
    feedback(store, qid, 'pending')
    store = Store(store.home, store.profile)
    assert store.get(qid)['request']
    feedback(store, qid, 'done')
    assert before <= store.get(qid)['completed'] <= completed_before


def test_done_feedback_survives_restart_before_completion(store):
    qid, _ = answered(store)
    token = store.prepare_continuation(qid)
    feedback(store, qid)
    store = Store(store.home, store.profile)
    assert store.get(qid)['feedback_state'] == 'done'
    assert store.bind_turn(token, 'turn-1')
    assert finish(store)
    assert store.get(qid)['compacted']


def test_tombstones_retained_for_original_expiry_and_seven_days(store, monkeypatch):
    req = request()
    now = time.time()
    req['expires_at'] = now + 20 * 86400
    row = store.create(req, {'telegram'})
    qid = row['id']
    assert store.claim(qid)
    store.receipt(qid, 'message-1')
    assert store.answer(qid, '1', store.get(qid)['request'], authorized=True) == 'answered'
    token = store.prepare_continuation(qid)
    assert store.bind_turn(token, 'turn-1')
    feedback(store, qid)
    assert finish(store)
    monkeypatch.setattr(mod.time, 'time', lambda: now + 8 * 86400)
    store.prune()
    assert store.create(req, {'telegram'})['compacted']
    monkeypatch.setattr(mod.time, 'time', lambda: now + 21 * 86400)
    store.prune()
    assert store.get(qid) is None


def test_pruning_retains_pending_feedback_and_legacy_work(store, monkeypatch):
    old = time.time()
    row = store.create(request(), {'telegram'})
    qid = row['id']
    assert store.claim(qid)
    store.receipt(qid, 'message-1')
    assert store.cancel(qid)
    monkeypatch.setattr(mod.time, 'time', lambda: old + 8 * 86400)
    store.prune()
    assert store.get(qid)['request']
    feedback(store, qid)
    store.prune()
    assert store.get(qid)['compacted']
    monkeypatch.setattr(mod.time, 'time', lambda: old + 14 * 86400)
    store.prune()
    assert store.get(qid)['compacted']  # Seven days from compaction, not creation.



def test_retired_payload_is_not_left_in_free_pages(store):
    qid, req = answered(store)
    token = store.prepare_continuation(qid)
    assert store.bind_turn(token, 'erase-turn')
    feedback(store, qid)
    assert finish(store, 'erase-turn')
    assert b'Private question' not in store.path.read_bytes()
    assert b'Private answer' not in store.path.read_bytes()


def test_confirmed_text_transport_without_message_id_can_retire(store):
    row = store.create(request(), {'telegram'})
    assert store.claim(row['id'])
    store.receipt(row['id'], confirmed=True)
    assert store.answer(row['id'], '1', store.get(row['id'])['request'], authorized=True) == 'answered'
    assert store.bind_turn(store.prepare_continuation(row['id']), 'text-turn')
    assert finish(store, 'text-turn')
    assert store.get(row['id'])['compacted']


def test_atomic_claim_returns_token_once(store):
    qid, req = answered(store)
    token = store.claim_continuation(qid)
    assert token
    assert store.claim_continuation(qid) is None
    assert store.get(qid)['continuation_state'] == 'dispatching'
    assert store.bind_turn(token, 'atomic-turn')


def test_nonmain_async_marker_release_race_rechecks_lock(store, monkeypatch):
    import asyncio
    import threading
    qid, req = answered(store)
    original = mod.fcntl.flock
    calls = []
    def racing(fd, flags):
        calls.append(flags)
        if len(calls) == 1:
            raise BlockingIOError('owner released immediately after the probe')
        return original(fd, flags)
    monkeypatch.setattr(mod.fcntl, 'flock', racing)
    errors = []
    async def probe():
        assert store.get(qid)['state'] == 'answered'
    def thread_main():
        try:
            asyncio.run(probe())
        except BaseException as error:
            errors.append(error)
    thread = threading.Thread(target=thread_main)
    thread.start(); thread.join(3)
    assert not thread.is_alive() and not errors
    assert len(calls) >= 2
