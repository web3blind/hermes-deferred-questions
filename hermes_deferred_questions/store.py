"""Durable request/receipt/answer ledger. No gateway imports or scheduler."""
from __future__ import annotations

from contextlib import contextmanager
import json
import math
import re
import sqlite3
import time
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
            # Additive, repeatable migration: old terminal receipts need cleanup too.
            for row in db.execute("SELECT * FROM questions WHERE message_id IS NOT NULL "
                                  "AND state IN ('answered','expired','cancelled')").fetchall():
                self._feedback_intent(db, row['id'])
        self.path.chmod(0o600)

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=5)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

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
            existing = db.execute('SELECT rowid AS delivery_nonce,* FROM questions WHERE idem=?', (r['idempotency_key'],)).fetchone()
            if existing:
                if existing['request'] != encoded:
                    raise ValueError('idempotency_key already belongs to a different request')
                return self.unpack(existing)
            if r['expires_at'] <= time.time():
                raise ValueError('expires_at must be in the future')
            db.execute('INSERT INTO questions(id,idem,request,snapshot,state,created) VALUES(?,?,?,?,?,?)',
                       (qid, r['idempotency_key'], encoded, dumps(snapshot), 'queued', time.time()))
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
            return self.unpack(db.execute('SELECT rowid AS delivery_nonce,*, '
                                          '(SELECT state FROM answer_actions WHERE question_id=questions.id) AS continuation_state, '
                                          '(SELECT state FROM feedback_edits WHERE question_id=questions.id) AS feedback_state, '
                                          '(SELECT version FROM feedback_edits WHERE question_id=questions.id) AS feedback_version '
                                          'FROM questions WHERE id=?', (qid,)).fetchone())

    def list(self, limit=50):
        if type(limit) is not int or not 1 <= limit <= 200:
            raise ValueError('limit must be an integer from 1 to 200')
        self.expire()
        with self.connect() as db:
            return [self.unpack(r) for r in db.execute('SELECT rowid AS delivery_nonce,* FROM questions ORDER BY created DESC,id LIMIT ?', (limit,))]

    def queued(self, platform):
        self.expire()
        with self.connect() as db:
            rows = db.execute("SELECT rowid AS delivery_nonce,* FROM questions WHERE state='queued' ORDER BY created,id").fetchall()
        return [self.unpack(r) for r in rows if json.loads(r['request'])['platform'] == platform]

    def claim(self, qid):
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = self.unpack(db.execute('SELECT rowid AS delivery_nonce,* FROM questions WHERE id=?', (qid,)).fetchone())
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
            row = self.unpack(db.execute('SELECT rowid AS delivery_nonce,* FROM questions WHERE id=?', (qid,)).fetchone())
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
            rows = db.execute("SELECT q.rowid AS delivery_nonce,q.* FROM questions q JOIN feedback_edits f "
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
            # Infinite recovery of safe edits, but capped delay and bounded attempts per pass.
            delay = max(min(300, 2 ** min(attempts, 9)), retry_after) if state == 'pending' else 0
            return db.execute('UPDATE feedback_edits SET state=?,attempts=?,next_attempt=?,diagnostic=? '
                              "WHERE question_id=? AND version=? AND state='pending'",
                              (state, attempts, time.time() + delay, diagnostic, qid, version)).rowcount == 1

    def pending_actions(self, platform):
        with self.connect() as db:
            rows = db.execute("SELECT q.rowid AS delivery_nonce,q.* FROM questions q JOIN answer_actions a "
                              "ON a.question_id=q.id WHERE a.state='pending' AND q.state='answered'").fetchall()
        return [self.unpack(row) for row in rows if json.loads(row['request'])['platform'] == platform]

    def claim_action(self, qid):
        with self.connect() as db:
            return db.execute("UPDATE answer_actions SET state='dispatching' WHERE question_id=? AND state='pending'",
                              (qid,)).rowcount == 1

    def finish_action(self, qid, state):
        if state not in ('admitted', 'pending', 'unknown'):
            raise ValueError('invalid action state')
        with self.connect() as db:
            db.execute("UPDATE answer_actions SET state=? WHERE question_id=? AND state='dispatching'", (state, qid))
