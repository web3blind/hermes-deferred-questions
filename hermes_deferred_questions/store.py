"""Durable request/receipt/answer ledger. No gateway imports or scheduler."""
from __future__ import annotations

from contextlib import contextmanager
import fcntl
import hashlib
import logging
import os
import shutil
import json
import math
import re
import sqlite3
import time
import threading
import uuid
from pathlib import Path

ROUTE_FIELDS = ('platform', 'chat_id', 'user_id', 'chat_type', 'thread_id', 'scope_id', 'parent_chat_id')
ID_RE = re.compile(r'[0-9a-f]{32}\Z')


def dumps(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def validate_request(request, platforms):
    if not isinstance(request, dict):
        raise ValueError('request must be an object')
    allowed = set(ROUTE_FIELDS) | {'question', 'options', 'allow_text', 'expires_at', 'idempotency_key', 'continue_agent'}
    if set(request) - allowed:
        raise ValueError('unknown request fields')
    r = dict(request)
    for name in ('platform', 'chat_id', 'chat_type', 'user_id', 'question', 'idempotency_key'):
        value = r.get(name)
        if not isinstance(value, str) or not value.strip() or len(value) > (1200 if name == 'question' else 200):
            raise ValueError(f'{name} must be a nonempty bounded string')
        if name != 'question' and value != value.strip():
            raise ValueError(f'{name} must not contain surrounding whitespace')
    if r['platform'] not in platforms:
        raise ValueError('unsupported platform')
    if r['chat_type'] not in {'dm', 'group', 'forum', 'channel', 'thread'}:
        raise ValueError('invalid chat_type')
    for name in ('thread_id', 'scope_id', 'parent_chat_id'):
        value = r.setdefault(name, None)
        if value is not None and (not isinstance(value, str) or not value.strip() or value != value.strip() or len(value) > 200):
            raise ValueError(f'{name} must be null or nonempty string')
    # Never rely on Slack primary-workspace fallback or user-ID -> DM-channel rewrites.
    if r['platform'] == 'slack':
        if not r['scope_id'] or r['chat_id'][0] not in 'CDG':
            raise ValueError('Slack requires workspace scope_id and actual C/D/G conversation ID')
        if r['chat_type'] != ('dm' if r['chat_id'].startswith('D') else 'group'):
            raise ValueError('Slack chat_type must match the actual DM/group conversation')
    if r['platform'] in {'telegram', 'discord'}:
        if not re.fullmatch(r'-?[1-9][0-9]*', r['chat_id']) or not re.fullmatch(r'[1-9][0-9]*', r['user_id']):
            raise ValueError('Telegram/Discord require canonical numeric chat/user IDs')
        if r['thread_id'] is not None and not re.fullmatch(r'[1-9][0-9]*', r['thread_id']):
            raise ValueError('Telegram/Discord thread_id must be canonical numeric ID')
    if r['platform'] == 'telegram' and r['chat_type'] not in {'dm', 'group', 'forum'}:
        raise ValueError('Telegram chat_type must be dm, group or forum')
    if r['platform'] == 'discord':
        if r['chat_type'] == 'thread' and (r['thread_id'] != r['chat_id'] or not r['parent_chat_id']):
            raise ValueError('Discord thread requires actual thread chat_id=thread_id and parent_chat_id')
        if r['chat_type'] not in {'dm', 'channel', 'thread'}:
            raise ValueError('Discord requires dm/channel/thread, matching native message events')
    if r['platform'] == 'whatsapp_cloud' and (r['chat_type'] != 'dm' or r['chat_id'] != r['user_id'] or r['thread_id']):
        raise ValueError('WhatsApp Cloud requires unthreaded DM chat_id=user_id')
    options = r.setdefault('options', [])
    if not isinstance(options, list) or len(options) > 100 or any(
        not isinstance(o, str) or not o.strip() or len(o) > 150 for o in options
    ):
        raise ValueError('options must contain up to 100 nonempty labels (150 chars each)')
    if len(set(options)) != len(options):
        raise ValueError('duplicate option labels')
    r['options'] = list(options)
    if not isinstance(r.setdefault('allow_text', True), bool):
        raise ValueError('allow_text must be boolean')
    if not options and not r['allow_text']:
        raise ValueError('question has no answer mode')
    if 'continue_agent' in r and type(r['continue_agent']) is not bool:
        raise ValueError('continue_agent must be boolean')
    expiry = r.get('expires_at')
    if isinstance(expiry, bool) or not isinstance(expiry, (int, float)) or not math.isfinite(expiry):
        raise ValueError('expires_at must be a finite absolute Unix timestamp')
    return r


def render(qid, r):
    lines = [r['question'], '']
    lines.extend(f'{i}. {label}' for i, label in enumerate(r['options'], 1))
    lines += ['', f'Answer: dq {qid} NUMBER'] if r['options'] else []
    if r['allow_text']:
        lines += [f'Custom answer: dq {qid} text: YOUR ANSWER']
    text = '\n'.join(lines)
    # Single-message budget for Discord/Telegram/Slack, including astral Unicode.
    if len(text.encode('utf-16-le')) // 2 > 1800:
        raise ValueError('rendered question exceeds 1800 UTF-16 units; shorten question/options')
    metadata = {'hermes_profile': r['profile']}
    if r['thread_id']:
        metadata['thread_id'] = r['thread_id']
    if r['platform'] == 'slack':
        metadata['slack_team_id'] = r['scope_id']
    return {'text': text, 'metadata': metadata,
            'buttons': [{'label': o, 'token': f'dq:{qid}:{i}'} for i, o in enumerate(r['options'], 1)]}


class Store:
    def __init__(self, home, profile):
        self.home = str(Path(home).resolve())
        self.profile = profile
        folder = Path(self.home) / 'deferred-questions'
        folder.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.path = folder / 'questions.sqlite3'
        if self.path.is_symlink():
            raise sqlite3.DatabaseError('Deferred Questions ledger symlinks are unsupported')
        with self.connect() as db:
            db.execute('PRAGMA journal_mode=WAL')
            db.execute('''CREATE TABLE IF NOT EXISTS questions (
                id TEXT PRIMARY KEY, idem TEXT UNIQUE NOT NULL,
                request TEXT NOT NULL, snapshot TEXT NOT NULL,
                state TEXT NOT NULL, created REAL NOT NULL,
                message_id TEXT, answer TEXT, answered REAL, diagnostic TEXT)''')
            db.execute('''CREATE TABLE IF NOT EXISTS answer_actions (
                question_id TEXT PRIMARY KEY, state TEXT NOT NULL)''')
            db.execute('''CREATE TABLE IF NOT EXISTS feedback_edits (
                question_id TEXT PRIMARY KEY, version INTEGER NOT NULL,
                target_state TEXT NOT NULL, message_id TEXT NOT NULL,
                state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt REAL NOT NULL DEFAULT 0, diagnostic TEXT)''')
            db.execute('''CREATE TABLE IF NOT EXISTS completion_attempts (
                token TEXT UNIQUE NOT NULL, question_id TEXT UNIQUE NOT NULL,
                turn_id TEXT UNIQUE, state TEXT NOT NULL, completed REAL)''')
            db.execute('''CREATE TABLE IF NOT EXISTS tombstones (
                id TEXT PRIMARY KEY, idem TEXT UNIQUE NOT NULL, request_hash TEXT NOT NULL,
                state TEXT NOT NULL, completed REAL NOT NULL, retain_until REAL NOT NULL)''')
            db.execute('CREATE TABLE IF NOT EXISTS delivery_sequence (value INTEGER NOT NULL)')
            columns = {r['name'] for r in db.execute('PRAGMA table_info(questions)')}
            if 'delivery_nonce' not in columns:
                db.execute('ALTER TABLE questions ADD COLUMN delivery_nonce INTEGER')
                db.execute('UPDATE questions SET delivery_nonce=rowid')
            if not db.execute('SELECT 1 FROM delivery_sequence').fetchone():
                db.execute('INSERT INTO delivery_sequence VALUES (?)',
                           (db.execute('SELECT COALESCE(MAX(delivery_nonce),0) FROM questions').fetchone()[0],))
            # Additive, repeatable migration: old terminal receipts need cleanup too.
            for row in db.execute("SELECT * FROM questions WHERE message_id IS NOT NULL "
                                  "AND state IN ('answered','expired','cancelled') "
                                  "AND NOT EXISTS (SELECT 1 FROM feedback_edits f WHERE f.question_id=questions.id)").fetchall():
                self._feedback_intent(db, row['id'])
        self.path.chmod(0o600)
        self.prune()

    @contextmanager
    def connect(self):
        # One persistent inode fences EVERY participating CLI/gateway connection.
        # Never wait on flock in the gateway event loop, and never unlink the lock.
        lock = self.path.with_name('store.lock').open('a+b')
        acquired = False
        try:
            os.fchmod(lock.fileno(), 0o600)
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                # Ask the current owner for ONE release event, not a timed retry.
                marker = self.path.with_name('store-wake')
                fd = os.open(marker, os.O_CREAT | os.O_WRONLY, 0o600)
                os.close(fd)
                blocking = False
                if threading.current_thread() is not threading.main_thread():
                    try:
                        __import__('asyncio').get_running_loop()
                    except RuntimeError:
                        blocking = True
                try:
                    # Every nonblocking caller retries AFTER publishing its marker:
                    # if release raced with the marker, this owner will consume it.
                    fcntl.flock(lock, fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))
                except BlockingIOError:
                    raise sqlite3.OperationalError('Deferred Questions store busy; release event pending') from None
            acquired = True
            self._preflight()
            db = sqlite3.connect(self.path, timeout=5)
            db.row_factory = sqlite3.Row
            try:
                db.execute('PRAGMA secure_delete=ON')
                with db:
                    yield db
                self._save_header(db)
            finally:
                db.close()
        finally:
            lock.close()
            if acquired:
                self._release_notice()

    def _release_notice(self):
        marker = self.path.with_name('store-wake')
        try:
            if not marker.exists():
                return
            marker.unlink()
            signalled = False
            for reference in tuple(getattr(self, '_wakers', ())):
                callback = reference()
                if callback is not None:
                    signalled = callback() or signalled
            if not signalled:
                from .notify import notify
                notify(self.home)
        except Exception:
            pass  # Never turn a committed operation into a failed write receipt.

    def _header_file(self):
        return self.path.with_name('store-header.json')

    @staticmethod
    def _sync_dir(path):
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def _save_header(self, db):
        # First 24 SQLite header bytes are format metadata, not ledger state.
        # Never recover from an older ledger snapshot: that can replay external sends.
        target = self._header_file()
        if target.exists():
            return
        if db.execute('PRAGMA integrity_check').fetchall()[0][0] != 'ok':
            raise sqlite3.DatabaseError('Deferred Questions integrity check failed')
        with self.path.open('rb') as f:
            header = f.read(24)
        if not header.startswith(b'SQLite format 3\x00'):
            raise sqlite3.DatabaseError('Deferred Questions invalid store header')
        record = dict(path=str(self.path.resolve()), header=header.hex(),
                      checksum=hashlib.sha256(header).hexdigest())
        tmp = target.with_name('.header-' + uuid.uuid4().hex)
        try:
            with tmp.open('xb') as f:
                os.fchmod(f.fileno(), 0o600)
                f.write(dumps(record).encode())
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, target)
            self._sync_dir(target.parent)
        finally:
            tmp.unlink(missing_ok=True)

    def _preflight(self):
        target = self._header_file()
        if not self.path.exists():
            if target.exists():
                raise sqlite3.DatabaseError('Deferred Questions ledger missing; refusing empty replacement')
            return
        with self.path.open('rb') as f:
            header = f.read(24)
        if not target.exists():
            if header.startswith(b'SQLite format 3\x00'):
                return  # First healthy legacy open enrolls protection after migration.
            # Legacy corrupt stores have no trustworthy template. Do not guess.
            raise sqlite3.DatabaseError('Deferred Questions header damaged; no trusted recovery metadata')
        try:
            record = json.loads(target.read_text())
            template = bytes.fromhex(record['header'])
            valid = (record['path'] == str(self.path.resolve()) and len(template) == 24
                     and template.startswith(b'SQLite format 3\x00')
                     and hashlib.sha256(template).hexdigest() == record['checksum'])
        except (ValueError, KeyError, TypeError):
            valid = False
        if not valid:
            raise sqlite3.DatabaseError('Deferred Questions recovery metadata invalid; ledger unchanged')
        if header == template:
            return
        # No safe header-only repair if committed state may still live in a WAL.
        # Keep the entire file set untouched for explicit offline recovery.
        for suffix in ('-wal', '-journal'):
            side = Path(str(self.path) + suffix)
            if side.exists() and side.stat().st_size:
                raise sqlite3.DatabaseError('Deferred Questions damaged header with live journal; offline recovery required')
        folder = self.path.parent / 'quarantine' / uuid.uuid4().hex
        folder.mkdir(mode=0o700, parents=True)
        saved = folder / self.path.name
        shutil.copyfile(self.path, saved)
        saved.chmod(0o600)
        with saved.open('rb') as f:
            os.fsync(f.fileno())
        self._sync_dir(folder)
        self._sync_dir(folder.parent)
        self._sync_dir(self.path.parent)
        candidate = folder / 'candidate.sqlite3'
        shutil.copyfile(saved, candidate)
        candidate.chmod(0o600)
        with candidate.open('r+b') as f:
            f.write(template)
            f.flush()
            os.fsync(f.fileno())
        # Validation never opens the damaged original and never runs migrations.
        check = sqlite3.connect(candidate.resolve().as_uri() + '?mode=ro&immutable=1', uri=True)
        try:
            if check.execute('PRAGMA integrity_check').fetchall() != [('ok',)]:
                raise sqlite3.DatabaseError('Deferred Questions body damaged; no automatic rollback')
            for table in ('questions', 'answer_actions', 'feedback_edits'):
                check.execute('SELECT * FROM ' + table + ' LIMIT 0')
        finally:
            check.close()
        # All plugin connections have drained before this exclusive store lease.
        # Body/receipts/rowids are byte-identical; only 24 format bytes may change.
        os.replace(candidate, self.path)
        self._sync_dir(self.path.parent)
        logging.getLogger(__name__).warning(
            'Deferred Questions repaired format header; ledger body preserved; original quarantined')

    @staticmethod
    def unpack(row):
        if row is None:
            return None
        data = dict(row)
        for name in ('request', 'snapshot', 'answer'):
            if data[name] is not None:
                data[name] = json.loads(data[name])
        return data

    def create(self, request, platforms):
        r = validate_request(request, platforms)
        self.expire()
        r.update(profile=self.profile, profile_home=self.home)
        qid = uuid.uuid4().hex
        snapshot = render(qid, r)
        encoded = dumps(r)
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            existing = db.execute('SELECT * FROM questions WHERE idem=?', (r['idempotency_key'],)).fetchone()
            if existing:
                if existing['request'] != encoded:
                    raise ValueError('idempotency_key already belongs to a different request')
                return self.unpack(existing)
            tombstone = db.execute('SELECT * FROM tombstones WHERE idem=?', (r['idempotency_key'],)).fetchone()
            if tombstone:
                if tombstone['request_hash'] != hashlib.sha256(encoded.encode()).hexdigest():
                    raise ValueError('idempotency_key already belongs to a different request')
                return self._tombstone(tombstone)
            if r['expires_at'] <= time.time():
                raise ValueError('expires_at must be in the future')
            db.execute('UPDATE delivery_sequence SET value=value+1')
            nonce = db.execute('SELECT value FROM delivery_sequence').fetchone()[0]
            db.execute('INSERT INTO questions(id,idem,request,snapshot,state,created,delivery_nonce) VALUES(?,?,?,?,?,?,?)',
                       (qid, r['idempotency_key'], encoded, dumps(snapshot), 'queued', time.time(), nonce))
        return self.get(qid)

    def expire(self):
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            rows = db.execute("SELECT id FROM questions WHERE state IN ('queued','sent') "
                              "AND json_extract(request,'$.expires_at') <= ?", (time.time(),)).fetchall()
            for row in rows:
                db.execute("UPDATE questions SET state='expired' WHERE id=?", (row['id'],))
                self._feedback_intent(db, row['id'])

    def get(self, qid):
        if not isinstance(qid, str) or not ID_RE.fullmatch(qid):
            raise ValueError('invalid question ID')
        self.expire()
        with self.connect() as db:
            row = db.execute('SELECT *, '
                             '(SELECT state FROM answer_actions WHERE question_id=questions.id) AS continuation_state, '
                             '(SELECT state FROM feedback_edits WHERE question_id=questions.id) AS feedback_state, '
                             '(SELECT version FROM feedback_edits WHERE question_id=questions.id) AS feedback_version '
                             'FROM questions WHERE id=?', (qid,)).fetchone()
            if row:
                return self.unpack(row)
            tombstone = db.execute('SELECT * FROM tombstones WHERE id=?', (qid,)).fetchone()
            return self._tombstone(tombstone) if tombstone else None

    def list(self, limit=50):
        if type(limit) is not int or not 1 <= limit <= 200:
            raise ValueError('limit must be an integer from 1 to 200')
        self.expire()
        with self.connect() as db:
            rows = [self.unpack(r) for r in db.execute('SELECT * FROM questions ORDER BY created DESC,id LIMIT ?', (limit,))]
            rows.extend(self._tombstone(r) for r in db.execute('SELECT * FROM tombstones ORDER BY completed DESC,id LIMIT ?', (limit,)))
            return sorted(rows, key=lambda r: (r.get('created', r.get('completed', 0)), r['id']), reverse=True)[:limit]

    def queued(self, platform):
        self.expire()
        with self.connect() as db:
            rows = db.execute("SELECT * FROM questions WHERE state='queued' ORDER BY created,id").fetchall()
        return [self.unpack(r) for r in rows if json.loads(r['request'])['platform'] == platform]

    def claim(self, qid):
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = self.unpack(db.execute('SELECT * FROM questions WHERE id=?', (qid,)).fetchone())
            if row is None or row['state'] != 'queued':
                return False
            if row['request']['expires_at'] <= time.time():
                db.execute("UPDATE questions SET state='expired' WHERE id=?", (qid,))
                return False
            db.execute("UPDATE questions SET state='sending' WHERE id=?", (qid,))
            return True

    def receipt(self, qid, message_id=None, diagnostic=None, *, confirmed=False):
        with self.connect() as db:
            db.execute("UPDATE questions SET state=?,message_id=?,diagnostic=? WHERE id=? AND state='sending'",
                       ('sent' if message_id or confirmed is True else 'unknown', message_id, diagnostic, qid))

    def cancel(self, qid):
        self.get(qid)  # validate identifier
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            changed = db.execute("UPDATE questions SET state='cancelled' WHERE id=? AND state IN ('queued','sent')",
                                 (qid,)).rowcount == 1
            if changed:
                self._feedback_intent(db, qid)
            return changed

    def answer(self, qid, value, route, *, authorized, message_id=None):
        if authorized is not True:
            return 'unauthorized'
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = self.unpack(db.execute('SELECT * FROM questions WHERE id=?', (qid,)).fetchone())
            if row is None:
                return 'unknown_question'
            r = row['request']
            if r['profile_home'] != self.home or r['profile'] != self.profile:
                return 'wrong_profile'
            if any(route.get(k) != r.get(k) for k in ROUTE_FIELDS + ('profile', 'profile_home')):
                return 'wrong_route'
            if message_id is not None and str(message_id) != row['message_id']:
                return 'wrong_message'
            if row['state'] == 'answered':
                return 'already_answered'
            if row['state'] == 'expired':
                return 'expired'
            if row['state'] != 'sent':
                return 'not_delivered'
            if r['expires_at'] <= time.time():
                db.execute("UPDATE questions SET state='expired' WHERE id=?", (qid,))
                self._feedback_intent(db, qid)
                return 'expired'
            if not isinstance(value, str):
                return 'invalid_answer'
            if re.fullmatch(r'[1-9][0-9]{0,2}', value) and int(value) <= len(r['options']):
                index = int(value) - 1
                answer = {'kind': 'option', 'index': index + 1, 'value': r['options'][index]}
            elif r['allow_text'] and value.startswith('text:') and value[5:].strip() and len(value[5:]) <= 4000:
                answer = {'kind': 'text', 'value': value[5:].strip()}
            else:
                return 'invalid_answer'
            db.execute("UPDATE questions SET state='answered',answer=?,answered=? WHERE id=? AND state='sent'",
                       (dumps(answer), time.time(), qid))
            self._feedback_intent(db, qid)
            if r.get('continue_agent') is True:
                db.execute("INSERT OR IGNORE INTO answer_actions VALUES (?, 'pending')", (qid,))
            return 'answered'

    @staticmethod
    def _feedback_intent(db, qid):
        row = db.execute('SELECT state,message_id FROM questions WHERE id=?', (qid,)).fetchone()
        if not row or not row['message_id'] or row['state'] not in ('answered', 'expired', 'cancelled'):
            return
        db.execute('''INSERT INTO feedback_edits(question_id,version,target_state,message_id,state)
                      VALUES (?,1,?,?,'pending') ON CONFLICT(question_id) DO UPDATE SET
                      version=version+1,target_state=excluded.target_state,message_id=excluded.message_id,
                      state='pending',attempts=0,next_attempt=0,diagnostic=NULL
                      WHERE target_state!=excluded.target_state OR message_id!=excluded.message_id''',
                   (qid, row['state'], row['message_id']))

    def pending_feedback(self, platform):
        self.expire()
        with self.connect() as db:
            rows = db.execute("SELECT q.* FROM questions q JOIN feedback_edits f "
                              "ON f.question_id=q.id WHERE f.state='pending' AND f.next_attempt<=? "
                              "ORDER BY q.created,q.id", (time.time(),)).fetchall()
        return [self.unpack(r) for r in rows if json.loads(r['request'])['platform'] == platform]

    def feedback(self, qid):
        with self.connect() as db:
            row = db.execute('SELECT * FROM feedback_edits WHERE question_id=?', (qid,)).fetchone()
            return dict(row) if row else None

    def finish_feedback(self, qid, version, state, diagnostic=None, *, retry_after=0):
        if state not in ('done', 'deleted', 'held', 'pending'):
            raise ValueError('invalid feedback state')
        with self.connect() as db:
            row = db.execute('SELECT attempts FROM feedback_edits WHERE question_id=? AND version=? '
                             "AND state='pending'", (qid, version)).fetchone()
            if row is None:
                return False
            attempts = row['attempts'] + 1
            # A later explicit wake may revisit safe edits; no job timer is scheduled.
            delay = max(min(300, 2 ** min(attempts, 9)), retry_after) if state == 'pending' else 0
            changed = db.execute('UPDATE feedback_edits SET state=?,attempts=?,next_attempt=?,diagnostic=? '
                                 "WHERE question_id=? AND version=? AND state='pending'",
                                 (state, attempts, time.time() + delay, diagnostic, qid, version)).rowcount == 1
            if changed:
                self._cleanup_completed(db, qid)
            return changed

    def pending_actions(self, platform):
        with self.connect() as db:
            rows = db.execute("SELECT q.* FROM questions q JOIN answer_actions a "
                              "ON a.question_id=q.id WHERE a.state='pending' AND q.state='answered'").fetchall()
        return [self.unpack(row) for row in rows if json.loads(row['request'])['platform'] == platform]

    def claim_continuation(self, qid):
        """Claim and persist correlation atomically, BEFORE any admission effect."""
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            action = db.execute("SELECT a.state FROM answer_actions a JOIN questions q ON q.id=a.question_id "
                                "WHERE q.id=? AND q.state='answered'", (qid,)).fetchone()
            if not action or action['state'] != 'pending':
                return None
            old = db.execute('SELECT token,state FROM completion_attempts WHERE question_id=?', (qid,)).fetchone()
            if old and old['state'] != 'pending':
                return None
            token = old['token'] if old else uuid.uuid4().hex
            if old is None:
                db.execute("INSERT INTO completion_attempts(token,question_id,state) VALUES (?,?,'pending')", (token, qid))
            db.execute("UPDATE answer_actions SET state='dispatching' WHERE question_id=?", (qid,))
            return token

    def claim_action(self, qid):
        with self.connect() as db:
            return db.execute("UPDATE answer_actions SET state='dispatching' WHERE question_id=? AND state='pending'",
                              (qid,)).rowcount == 1

    def finish_action(self, qid, state):
        if state not in ('admitted', 'pending', 'unknown'):
            raise ValueError('invalid action state')
        with self.connect() as db:
            db.execute("UPDATE answer_actions SET state=? WHERE question_id=? AND state='dispatching'", (state, qid))
            if state in ('admitted', 'unknown'):
                db.execute("UPDATE completion_attempts SET state=? WHERE question_id=? AND state='pending'",
                           (state, qid))

    @staticmethod
    def _tombstone(row):
        return dict(row, tombstone=True, compacted=True)

    def prepare_continuation(self, qid):
        """Persist an opaque correlation token before admitting an answered action."""
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            action = db.execute("SELECT a.state FROM answer_actions a JOIN questions q ON q.id=a.question_id "
                                "WHERE q.id=? AND q.state='answered'", (qid,)).fetchone()
            if not action or action['state'] not in ('pending', 'dispatching'):
                return None  # Never retroactively correlate legacy admitted actions.
            old = db.execute('SELECT token,state FROM completion_attempts WHERE question_id=?', (qid,)).fetchone()
            if old:
                return old['token'] if old['state'] == 'pending' else None
            token = uuid.uuid4().hex
            db.execute("INSERT INTO completion_attempts(token,question_id,state) VALUES (?,?,'pending')", (token, qid))
            return token

    def bind_turn(self, token, turn_id):
        """Bind once, with uniqueness in both directions; never guess by question."""
        if not isinstance(token, str) or not isinstance(turn_id, str) or not turn_id.strip():
            return False
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            if db.execute('SELECT 1 FROM completion_attempts WHERE turn_id=?', (turn_id,)).fetchone():
                return False
            return db.execute("UPDATE completion_attempts SET turn_id=? WHERE token=? AND turn_id IS NULL "
                              "AND state IN ('pending','admitted')", (turn_id, token)).rowcount == 1

    def finish_turn(self, turn_id, *, completed=None, failed=None, interrupted=None):
        """Only an explicit successful terminal event is authority to remove data."""
        if completed is not True or failed is not False or interrupted is not False:
            return False
        if not isinstance(turn_id, str) or not turn_id.strip():
            return False
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute("SELECT question_id FROM completion_attempts WHERE turn_id=? "
                             "AND state IN ('pending','admitted')", (turn_id,)).fetchone()
            if not row:
                return False
            db.execute("UPDATE completion_attempts SET state='completed',completed=? WHERE turn_id=?", (time.time(), turn_id))
            self._cleanup_completed(db, row['question_id'])
            return True

    def _cleanup_completed(self, db, qid):
        row = db.execute("SELECT q.* FROM questions q JOIN completion_attempts c ON c.question_id=q.id "
                         "WHERE q.id=? AND q.state='answered' AND c.state='completed' "
                         "AND ((q.message_id IS NULL AND NOT EXISTS (SELECT 1 FROM feedback_edits f WHERE f.question_id=q.id)) "
                         "OR EXISTS (SELECT 1 FROM feedback_edits f WHERE f.question_id=q.id "
                         "AND (f.state IN ('done','deleted') OR (f.state='held' AND f.diagnostic='edit_unsupported'))))", (qid,)).fetchone()
        if row:
            self._retire(db, row)

    def _retire(self, db, row):
        now = time.time()
        attempt = db.execute('SELECT completed FROM completion_attempts WHERE question_id=?', (row['id'],)).fetchone()
        completed = attempt['completed'] if attempt and attempt['completed'] is not None else now
        db.execute('INSERT INTO tombstones VALUES (?,?,?,?,?,?)',
                   (row['id'], row['idem'], hashlib.sha256(row['request'].encode()).hexdigest(),
                    row['state'], completed, max(now + 7 * 86400, json.loads(row['request'])['expires_at'])))
        for table in ('answer_actions', 'feedback_edits'):
            db.execute('DELETE FROM ' + table + ' WHERE question_id=?', (row['id'],))
        db.execute('DELETE FROM questions WHERE id=?', (row['id'],))

    def prune(self):
        """Explicit/opportunistic maintenance only; unresolved receipts are retained."""
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            now = time.time()
            rows = db.execute("SELECT q.* FROM questions q WHERE q.state IN ('expired','cancelled') "
                              "AND q.created<=? AND json_extract(q.request,'$.expires_at')<=? "
                              "AND NOT EXISTS (SELECT 1 FROM answer_actions a WHERE a.question_id=q.id) "
                              "AND NOT EXISTS (SELECT 1 FROM completion_attempts c WHERE c.question_id=q.id) "
                              "AND NOT EXISTS (SELECT 1 FROM feedback_edits f WHERE f.question_id=q.id "
                              "AND f.state NOT IN ('done','deleted'))", (now - 7 * 86400, now)).fetchall()
            for row in rows:
                self._retire(db, row)
            db.execute('DELETE FROM completion_attempts WHERE question_id IN '
                       '(SELECT id FROM tombstones WHERE retain_until<=?)', (now,))
            db.execute('DELETE FROM tombstones WHERE retain_until<=?', (now,))
