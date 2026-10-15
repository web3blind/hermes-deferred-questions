"""Built-in protection, exercised without gateway/cron or the real Hermes home."""
import importlib.util
from pathlib import Path
import sqlite3
import time
import pytest

spec = importlib.util.spec_from_file_location('protected_store_test', Path(__file__).resolve().parents[1] / 'hermes_deferred_questions/store.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
Store = module.Store


def make(home):
    s = Store(home, 'default')
    r = dict(platform='telegram', chat_id='123', user_id='456', chat_type='dm',
             question='test', options=['yes'], expires_at=time.time()+3600,
             idempotency_key='protected')
    return s, s.create(r, {'telegram'})


def damage(path):
    with path.open('r+b') as f:
        f.seek(5)
        f.write(bytes.fromhex('17030300133d3448487bce9fa76cb4e0b0269c'))


def test_recovers_header_without_rolling_back_receipts(tmp_path):
    s, row = make(tmp_path)
    s.claim(row['id'])
    s.receipt(row['id'], 'message-1')
    damage(s.path)
    result = Store(tmp_path, 'default').get(row['id'])
    assert result['state'] == 'sent' and result['message_id'] == 'message-1'
    assert list(s.path.parent.glob('quarantine/*/questions.sqlite3'))


def test_running_instance_recovers_on_next_operation(tmp_path):
    s, row = make(tmp_path)
    damage(s.path)
    assert s.get(row['id'])['id'] == row['id']


def test_body_damage_is_not_replaced_with_old_backup(tmp_path):
    s, _ = make(tmp_path)
    with s.path.open('r+b') as f:
        f.seek(100)
        f.write(b'\xff' * 100)
    before = s.path.read_bytes()
    with pytest.raises(sqlite3.DatabaseError):
        Store(tmp_path, 'default')
    assert s.path.read_bytes() == before


@pytest.mark.parametrize('offset,payload', [(16, b'\x00\x00'), (18, b'\x09\x09'), (21, b'\x00\x00\x00')])
def test_format_only_damage_preserves_body(tmp_path, offset, payload):
    s, row = make(tmp_path)
    before = s.path.read_bytes()
    with s.path.open('r+b') as f:
        f.seek(offset)
        f.write(payload)
    assert Store(tmp_path, 'default').get(row['id'])['id'] == row['id']
    assert s.path.read_bytes()[24:] == before[24:]


def test_answer_and_action_are_not_replayed(tmp_path):
    s, row = make(tmp_path)
    # Enable the continuation before delivery, as a real continuable request does.
    request = dict(row['request'])
    request['continue_agent'] = True
    with s.connect() as db:
        db.execute('UPDATE questions SET request=? WHERE id=?', (module.dumps(request), row['id']))
    row = s.get(row['id'])
    s.claim(row['id'])
    s.receipt(row['id'], 'message-1')
    route = dict(row['request'])
    assert s.answer(row['id'], '1', route, authorized=True, message_id='message-1') == 'answered'
    assert s.claim_action(row['id'])
    s.finish_action(row['id'], 'admitted')
    before = s.path.read_bytes()
    damage(s.path)
    restored = Store(tmp_path, 'default').get(row['id'])
    assert restored['state'] == 'answered' and restored['answer']['value'] == 'yes'
    assert restored['continuation_state'] == 'admitted'
    assert not s.claim_action(row['id'])
    assert s.path.read_bytes()[24:] == before[24:]
    assert s.answer(row['id'], '1', route, authorized=True, message_id='message-1') == 'already_answered'


def test_cross_process_open_fails_busy_without_touching_database(tmp_path):
    import subprocess
    import sys
    s, _ = make(tmp_path)
    before = s.path.read_bytes()
    code = ('import importlib.util,sys,sqlite3; '
            'sp=importlib.util.spec_from_file_location("worker_store",sys.argv[1]); '
            'm=importlib.util.module_from_spec(sp);sp.loader.exec_module(m); '
            'm.Store(sys.argv[2],"default")')
    with s.connect():
        child = subprocess.run([sys.executable, '-c', code, str(Path(module.__file__)), str(tmp_path)],
                               capture_output=True, text=True, timeout=10)
    assert child.returncode != 0 and 'store busy' in child.stderr
    assert s.path.read_bytes() == before
    assert Store(tmp_path, 'default').list()


def test_bad_recovery_template_does_not_mutate_database(tmp_path):
    s, _ = make(tmp_path)
    (s.path.parent/'store-header.json').write_text('{"header":"invalid"}')
    before = s.path.read_bytes()
    with pytest.raises(sqlite3.DatabaseError, match='metadata invalid'):
        Store(tmp_path, 'default')
    assert s.path.read_bytes() == before


def test_nonempty_wal_blocks_header_repair(tmp_path):
    s, _ = make(tmp_path)
    damage(s.path)
    wal = Path(str(s.path)+'-wal')
    wal.write_bytes(b'unreconciled committed frames')
    before = s.path.read_bytes()
    with pytest.raises(sqlite3.DatabaseError):
        Store(tmp_path, 'default')
    assert s.path.read_bytes() == before and wal.read_bytes() == b'unreconciled committed frames'


def test_missing_initialized_store_is_not_recreated(tmp_path):
    s, _ = make(tmp_path)
    s.path.unlink()
    with pytest.raises(sqlite3.DatabaseError):
        Store(tmp_path, 'default')
    assert not s.path.exists()
