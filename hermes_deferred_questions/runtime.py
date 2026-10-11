"""Gateway delivery worker and namespaced SDK callbacks; no adapter mutation."""
from __future__ import annotations

import asyncio
from contextlib import contextmanager
from dataclasses import dataclass
import json
import logging
import os
from pathlib import Path
import re
import tempfile
import time
import uuid
from types import SimpleNamespace

from gateway.session_identity import identity_of

from .store import ROUTE_FIELDS

log = logging.getLogger(__name__)
TOKEN = re.compile(r'dq:([0-9a-f]{32}):([1-9][0-9]{0,2})\Z')
TEXT = re.compile(r'dq ([0-9a-f]{32}) (.+)\Z', re.DOTALL)


@dataclass(frozen=True)
class Delivery:
    """Transport acknowledged the send; some text-only transports expose no wire ID."""
    message_id: str | None


class Runtime:
    def __init__(self, ctx, store, platforms):
        self.ctx, self.store, self.platforms = ctx, store, platforms
        self.live = True
        self.adapters = {}
        self.workers = {}
        self.feedback_workers = {}
        self.cleanups = []
        self._bindings = {}
        self._pending_bindings = {}
        self._tasks = {}
        self._recovery = set()
        self._desired = {}
        self._drains = {}
        self._reasons = {}
        self.generation = uuid.uuid4().hex
        self._heartbeat_started = False
        self._heartbeat_stage = None
        self._heartbeat_reason = None
        self._lock = __import__('threading').RLock()
        self._unload_handle = ctx.on_unload(self.close)

    def _heartbeat(self, *, unloading=False):
        """Best-effort private Telegram observation, never a delivery decision."""
        with self._lock:
            if self.live or unloading:
                self._write_heartbeat()

    def _write_heartbeat(self):
        if not self._heartbeat_started:
            return
        temporary = None
        try:
            binding = self.adapters.get('telegram')
            connected = bool(binding is not None and binding[1].is_connected)
            state = self.health()['telegram']
            reason = self._heartbeat_reason or state['reason']
            if reason is None and not connected:
                reason = 'not_connected'
            snapshot = dict(pid=os.getpid(), generation=self.generation,
                            timestamp=time.time(), live=self.live,
                            bound=state['bound'], worker_alive=state['worker_alive'],
                            adapter_is_connected=connected, reason=reason,
                            error_stage=self._heartbeat_stage)
            # Canonical Store directory; mkstemp is 0600 before any data is written.
            folder = self.store.path.parent
            fd, temporary = tempfile.mkstemp(prefix='.telegram-runtime-', dir=folder)
            with os.fdopen(fd, 'w', encoding='utf-8') as stream:
                json.dump(snapshot, stream, sort_keys=True)
                stream.write('\n')
            os.replace(temporary, folder / 'telegram-runtime.json')
        except Exception:
            # No exception strings/logs: diagnostics cannot interrupt delivery.
            pass
        finally:
            if temporary is not None:
                try:
                    Path(temporary).unlink(missing_ok=True)
                except OSError:
                    pass

    def _heartbeat_done(self, task):
        with self._lock:
            if self.live:
                self._heartbeat()

    def health(self):
        """Plugin-only, fixed safe reasons: no routes, question IDs or exceptions."""
        result = {}
        with self._lock:
            for p in self.platforms:
                receipt = self._drains.get(p)
                reason = self._reasons.get(p)
                if receipt is not None:
                    reason = ('draining' if not receipt.done() else
                              'drain_failed' if not receipt.result() else reason)
                result[p] = {'bound': p in self.adapters,
                             'worker_alive': p in self.workers and not self.workers[p].done(),
                             'draining': reason == 'draining',
                             'degraded': reason not in (None, 'draining'), 'reason': reason}
        return result

    def _spawn(self, coro, platform):
        with self._lock:
            if not self.live:
                coro.close()
                return None
            try:
                task = asyncio.get_running_loop().create_task(coro)
            except Exception:
                coro.close()
                raise
            owned = self._tasks.setdefault(platform, set())
            owned.add(task)
            task.add_done_callback(owned.discard)
            return task

    def _retire(self, platform):
        from . import lifecycle
        binding = self._bindings.pop(platform, None)
        pending = self._pending_bindings.pop(platform, None)
        self.adapters.pop(platform, None)
        if binding is None:
            if pending is None:
                return
            native, adapter, loop = pending
            cleanups = []
        else:
            native, adapter, loop, cleanups = binding
        previous = self._drains.get(platform) or lifecycle.barrier(self.store.home, platform, adapter)
        receipt = lifecycle.drain(loop, tuple(self._tasks.get(platform, ())), cleanups,
                                  prerequisites=(previous,))
        lifecycle.remember(self.store.home, platform, native, adapter, loop, receipt)
        self._drains[platform] = receipt
        return receipt

    def close(self):
        with self._lock:
            if not self.live:
                return
            self.live = False  # Fence effects synchronously on the discovering thread.
            self.ctx = None
            self._desired.clear()
            for platform in set(self._bindings) | set(self._pending_bindings):
                self._retire(platform)
            if self._heartbeat_started:
                self._heartbeat_reason = 'unloaded'
                self._heartbeat_stage = 'lifecycle'
                self._heartbeat(unloading=True)

    def recover(self):
        from . import lifecycle
        import weakref
        ref = weakref.ref(self)
        for platform, native, adapter, loop, receipt in lifecycle.connections(self.store.home):
            owner = loop()
            if owner is None or owner.is_closed() or not owner.is_running():
                continue
            def schedule(p=platform, n=native, a=adapter, owning_loop=owner):
                rt = ref()
                if rt is not None and rt.live:
                    with rt._lock:
                        if not rt.live or p in rt._desired:
                            return  # A connect-time candidate superseded cached recovery.
                        rt._pending_bindings[p] = (n(), a(), owning_loop)
                        task = rt._spawn(rt._recover(p, n, a), p)
                        rt._recovery.add((owning_loop, task))
                        task.add_done_callback(lambda t: rt._recovery.discard((owning_loop, t)))
            owner.call_soon_threadsafe(schedule)

    async def _recover(self, platform, native_ref, adapter_ref):
        # Registration executes on a loader deadline thread. Await success publication,
        # never start effects on failed/timeout registration.
        for _ in range(100):
            if not self.live or self.ctx is None or not self._unload_handle.active:
                return
            if self.ctx.has_plugin(self.ctx.plugin_id):
                break
            await asyncio.sleep(.01)
        else:
            self._reasons[platform] = 'registration_not_published'
            return
        native, adapter = native_ref(), adapter_ref()
        if native is None and platform in ('telegram', 'discord'):
            return
        if adapter is None:
            return
        if platform == 'telegram':
            # PTB factories run before initialize/start finishes. A force reload
            # can retire that factory's runtime during connect; recovery must not
            # discard its cached candidate merely because readiness is still false.
            # This waiter belongs to the new runtime (and its drain), not the cache.
            while True:
                with self._lock:
                    if (not self.live or self.ctx is None or not self._unload_handle.active
                            or platform in self._desired):
                        return
                    if adapter.is_connected:
                        self.wire(platform, native, adapter)
                        return
                    self._reasons[platform] = 'not_connected'
                await asyncio.sleep(.01)
        if adapter.is_connected:
            with self._lock:
                if platform not in self._desired:
                    self.wire(platform, native, adapter)

    def route(self, source):
        if source is None or getattr(source, 'profile_route_rejected', False):
            return None
        ident = identity_of(source)
        if ident is not None:
            if (ident.transport_profile != self.store.profile
                    or str(ident.authorization_home.resolve()) != self.store.home):
                return None  # Never answer a question through a different profile's bot.
            profile, home = ident.runtime_profile, str(ident.runtime_home.resolve())
        else:
            profile = source.profile or self.store.profile
            home = self.store.home
        result = {k: getattr(source, k, None) for k in ROUTE_FIELDS}
        result['platform'] = getattr(source.platform, 'value', source.platform)
        result.update(profile=profile, profile_home=home)
        return result

    def source(self, adapter, **kwargs):
        # Callback identity is built from the actual SDK event, never the expected DB row.
        source = adapter.build_source(**kwargs)
        canonicalize = getattr(adapter, '_canonicalize', None)
        if callable(canonicalize):
            canonicalize(source)
        # Without canonical identity we can only serve this transport's owning profile.
        if identity_of(source) is None:
            owner = getattr(adapter, '_owner_profile', None) or self.store.profile
            source.profile = source.profile or owner
        return source

    def authorized(self, adapter, source):
        if self.route(source) is None:
            return False
        try:
            if getattr(source.platform, 'value', source.platform) == 'telegram':
                # Reuse the actual intake policy (DM/per-chat/wildcard/global
                # allow_from), AND the gateway grant below. Intake alone may
                # admit unknown DMs for pairing, so it cannot authorize sends.
                # Intake may pass a locally denied DM to pairing/decline.
                # That exception is not permission to send a deferred question.
                from plugins.platforms.telegram.adapter import _coerce_allow_set
                extra = adapter.config.extra
                local_allow = None
                if source.chat_type in ('group', 'forum', 'channel'):
                    groups = extra.get('groups')
                    if isinstance(groups, dict):
                        group = groups.get(str(source.chat_id))
                        if not isinstance(group, dict):
                            group = next((value for key, value in groups.items()
                                          if isinstance(key, str) and key.lower() == str(source.chat_id).lower()
                                          and isinstance(value, dict)), None)
                        if not isinstance(group, dict):
                            group = groups.get('*')
                        if isinstance(group, dict):
                            local_allow = group.get('allow_from') or group.get('allowFrom')
                    if local_allow is None:
                        local_allow = extra.get('group_allow_from')
                else:
                    local_allow = extra.get('allow_from')
                if local_allow is not None:
                    allowed = _coerce_allow_set(local_allow)
                    if source.user_id not in allowed and '*' not in allowed:
                        return False
                local_check = getattr(adapter, '_is_user_authorized_from_message', None)
                if not callable(local_check):
                    return False
                message = SimpleNamespace(
                    from_user=SimpleNamespace(id=source.user_id, is_bot=source.is_bot),
                    chat=SimpleNamespace(id=source.chat_id,
                                         type='private' if source.chat_type == 'dm' else 'supergroup',
                                         is_forum=source.chat_type == 'forum'),
                    message_thread_id=source.thread_id,
                    is_topic_message=source.thread_id is not None)
                if local_check(message) is not True:
                    return False
            return adapter._is_sender_authorized(
                source.user_id, source.chat_type, source.chat_id,
                thread_id=source.thread_id, is_bot=source.is_bot) is True
        except Exception:
            return False

    def pre_dispatch(self, event, gateway, **kwargs):
        if not self.live or event.internal or not event.allow_gateway_control:
            return None
        match = TEXT.fullmatch(event.text or '')
        if getattr(event.source.platform, 'value', None) == 'vk':
            raw = event.raw_message if isinstance(event.raw_message, dict) else {}
            message = (raw.get('object') or {}).get('message') or raw.get('object') or {}
            binding = self.adapters.get('vk')
            if (isinstance(message, dict) and binding
                    and str(message.get('peer_id')) == event.source.chat_id
                    and str(message.get('from_id')) == event.source.user_id):
                # Match only authored raw text, not quoted/reply summaries. Native
                # selectors strip addressing but source always remains intake's source.
                adapter = binding[1]
                authored = adapter._strip_bot_mention_prefix(str(message.get('text') or '').strip())
                _, authored = adapter._one_shot_lane_for_text(event.source.chat_id, authored)
                match = TEXT.fullmatch(authored)
            else:
                match = None
        prompt_message_id = None
        if not match and getattr(event.source.platform, 'value', None) == 'feishu':
            raw = getattr(event.raw_message, 'event', None)
            value = getattr(getattr(raw, 'action', None), 'value', None)
            token = value.get('hermes_deferred_question') if isinstance(value, dict) else None
            button = TOKEN.fullmatch(token or '')
            context = getattr(raw, 'context', None)
            operator = getattr(raw, 'operator', None)
            if (button and str(getattr(context, 'open_chat_id', '')) == event.source.chat_id
                    and str(getattr(operator, 'open_id', '')) == event.source.user_id):
                prompt_message_id = getattr(context, 'open_message_id', None)
                if prompt_message_id:
                    match = TEXT.fullmatch(f'dq {button[1]} {button[2]}')
        if not match and getattr(event.source.platform, 'value', None) == 'whatsapp_cloud':
            raw = event.raw_message if isinstance(event.raw_message, dict) else {}
            interactive = raw.get('interactive') or {}
            inner = interactive.get('button_reply') or interactive.get('list_reply') or {}
            button = TOKEN.fullmatch(inner.get('id') or '')
            prompt_message_id = (raw.get('context') or {}).get('id')
            if (button and prompt_message_id and raw.get('from') == event.source.user_id
                    and raw.get('from') == event.source.chat_id):
                match = TEXT.fullmatch(f'dq {button[1]} {button[2]}')
        if not match and getattr(event.source.platform, 'value', None) == 'vk':
            raw = event.raw_message if isinstance(event.raw_message, dict) else {}
            message = (raw.get('object') or {}).get('message') or {}
            payload = message.get('payload') or {}
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except ValueError:
                    payload = {}
            token = payload.get('hermes_deferred_question') if isinstance(payload, dict) else None
            button = TOKEN.fullmatch(token) if isinstance(token, str) else None
            # Only native intake establishes addressing/lane; payload carries QID/option.
            if (button and str(message.get('peer_id')) == event.source.chat_id
                    and str(message.get('from_id')) == event.source.user_id):
                match = TEXT.fullmatch(f'dq {button[1]} {button[2]}')
        if not match:
            return None
        route = self.route(event.source)
        is_vk = getattr(event.source.platform, 'value', None) == 'vk'
        # Native VK intake already did the parent ACL. This hook is the earliest
        # available plugin veto (AFTER native invocation/project parsing/routing).
        # Never convert a recognized owned token into an ordinary agent request
        # on unknown QID, stale lane or another gateway-authorized actor.
        check = getattr(gateway, '_is_user_authorized_for_source', None)
        vk_authorized = False
        if is_vk and callable(check):
            try:
                vk_authorized = check(event.source) is True
            except Exception:
                pass
        row = self.store.get(match[1])
        mismatch = (route is None or row is None or any(
            route.get(k) != row['request'].get(k)
            for k in ROUTE_FIELDS + ('profile', 'profile_home')))
        if mismatch:
            if is_vk and vk_authorized:
                reason = 'unknown_question' if row is None else 'wrong_route'
                log.warning('Deferred VK input rejected: %s', reason)
                return {'action': 'skip', 'reason': f'deferred-question:{reason}'}
            return None
        assert route is not None  # mismatch above has already rejected missing identity
        # This hook is BEFORE gateway auth. Unauthorised traffic stays with core.
        if not callable(check):
            return None
        try:
            authorized = vk_authorized if is_vk else check(event.source) is True
        except Exception:
            authorized = False
        if not authorized:
            return None
        result = self.store.answer(match[1], match[2], route, authorized=True, message_id=prompt_message_id)
        if result in ('answered', 'already_answered') and route['platform'] in ('telegram', 'vk'):
            self._spawn(self.answer_feedback(match[1]), route['platform'])
        return {'action': 'skip', 'reason': f'deferred-question:{result}'}

    def callback(self, adapter, token, source, message_id):
        match = TOKEN.fullmatch(token or '')
        if not self.live or not match or message_id is None:
            return 'unavailable'
        route = self.route(source)
        if route is None:
            return 'wrong_route'
        return self.store.answer(match[1], match[2], route,
                                 authorized=self.authorized(adapter, source), message_id=str(message_id))

    async def dispatch_feedback_once(self, platform):
        """Independent stage: failure never blocks continuation admission."""
        if not self.live:
            return
        for row in self.store.pending_feedback(platform):
            try:
                await self.answer_feedback(row['id'])
            except asyncio.CancelledError:
                raise
            except Exception:
                log.warning('Deferred feedback iteration failed')

    @staticmethod
    def feedback_text(row):
        if row['state'] == 'answered':
            answer = row['answer']
            label = (f"{answer['index']}. " if answer['kind'] == 'option' else '') + answer['value']
            text = row['request']['question'] + '\n\nAnswer saved: ' + label
        else:
            text = row['request']['question'] + '\n\n' + (
                'Question cancelled.' if row['state'] == 'cancelled' else 'The answer deadline has expired.')
        if row['request']['platform'] == 'telegram' and len(text.encode('utf-16-le')) // 2 > 4096:
            text = text.encode('utf-16-le')[:4095 * 2].decode('utf-16-le', errors='ignore') + '…'
        return text

    @staticmethod
    def vk_feedback_retryable(exc):
        """Recognize native HTTP helper failures without persisting error text.

        Native _http_json wraps URLError and VK API codes in RuntimeError.
        Unknown RuntimeError and non-JSON/permanent failures stay held.
        """
        from urllib.error import HTTPError, URLError
        seen = set()
        current = exc
        for _ in range(4):
            if current is None or id(current) in seen:
                break
            seen.add(id(current))
            if isinstance(current, HTTPError):
                return current.code in (408, 429, 500, 502, 503, 504)
            if isinstance(current, (TimeoutError, ConnectionError, URLError)):
                return True
            if isinstance(current, RuntimeError):
                match = re.match(r'^VK API error ([0-9]+):', str(current))
                if match:
                    return int(match[1]) in (6, 9, 10, 29)
            current = current.__cause__
        return False

    @contextmanager
    def _feedback_lock(self, qid):
        """Fence open/flock/unlink with one stable, never-unlinked admission inode.

        A per-question lock cannot simply be unlinked: another process may have
        opened its old inode while a third opens the replacement. Admission holds
        this guard until the nonblocking question flock succeeds or its FD closes.
        Cleanup holds both locks and runs only after a durable done receipt. The
        guard is never held across transport IO; pending/cancelled attempts keep
        the question inode so retries still serialize with in-flight finalizers.
        """
        import fcntl
        path = self.store.path.with_name(f'feedback-{qid}.lock')
        guard_path = self.store.path.with_name('feedback.lock')
        lock = None
        acquired = False

        def release(fd):
            # Independent attempts: failed unlock must not prevent close, and
            # cleanup OSError must not replace a result or CancelledError.
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            except OSError:
                log.warning('Deferred feedback lock unlock failed')
            try:
                fd.close()
            except OSError:
                log.warning('Deferred feedback lock close failed')

        try:
            guard = guard_path.open('a')
            try:
                try:
                    fcntl.flock(guard, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    pass  # No question FD may be opened while admission is busy.
                else:
                    lock = path.open('a')  # Outer finally owns it immediately.
                    try:
                        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except BlockingIOError:
                        pass
                    else:
                        acquired = True
                    finally:
                        if not acquired:
                            release(lock)  # No old-inode waiter escapes admission.
                            lock = None
            finally:
                release(guard)  # Never retain the admission FD across an await.
            yield acquired
        finally:
            if lock is not None:
                guard = None
                try:
                    guard = guard_path.open('a')
                    try:
                        fcntl.flock(guard, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except BlockingIOError:
                        pass  # Preserve the path; never wait on another process.
                    else:
                        intent = self.store.feedback(qid)
                        if intent is not None and intent['state'] == 'done':
                            path.unlink(missing_ok=True)
                except Exception:
                    # A duplicate callback may retry retained done-path cleanup.
                    log.warning('Deferred feedback lock cleanup failed')
                finally:
                    # On successful unlink, release the old inode before guard.
                    # Also attempt both resources after any read/unlink failure.
                    release(lock)
                    if guard is not None:
                        release(guard)

    async def answer_feedback(self, qid):
        """Versioned edit only. OS lock serializes local generations/processes.

        No DB transaction spans IO; crash releases lock, leaving intent retryable.
        """
        import time
        row = self.store.get(qid)
        if not self.live or not row:
            return False
        with self._feedback_lock(qid) as acquired:
            if not acquired:
                return False
            intent = self.store.feedback(qid)
            if intent is None:
                return False
            if intent['state'] == 'done':
                return True
            if intent['state'] != 'pending' or intent['next_attempt'] > time.time():
                return False
            version = intent['version']
            def finish(state, code=None, retry_after=0.0):
                return self.store.finish_feedback(qid, version, state, code, retry_after=retry_after)
            try:
                row = self.store.get(qid)
                r = row['request']
                if row['state'] != intent['target_state'] or row['message_id'] != intent['message_id']:
                    finish('held', 'stale_intent')
                    return False
                binding = self.adapters.get(r['platform'])
                if binding is None or not self.live:
                    return False
                native, adapter = binding
                if not adapter.is_connected:
                    return False
                source = self.source(adapter, **{k: r[k] for k in ROUTE_FIELDS if k != 'platform'})
                route = self.route(source)
                if (route is None or not self.authorized(adapter, source)
                        or any(route.get(k) != r.get(k) for k in ROUTE_FIELDS + ('profile', 'profile_home'))):
                    finish('pending', 'authorization_or_route_denied')
                    return False
                if not self.live or self.adapters.get(r['platform']) != binding or self.store.feedback(qid) != intent:
                    return False
                text = self.feedback_text(row)
                if r['platform'] == 'telegram':
                    await asyncio.wait_for(native.bot.edit_message_text(
                        chat_id=r['chat_id'], message_id=int(intent['message_id']),
                        text=text, reply_markup=None, parse_mode=None), timeout=30)
                elif r['platform'] == 'vk':
                    params = {'peer_id': r['chat_id'], 'message': text,
                              'keyboard': json.dumps({'inline': True, 'buttons': []})}
                    mid = intent['message_id']
                    params['conversation_message_id' if mid.startswith('cmid:') else 'message_id'] = (
                        mid.split(':', 1)[1] if mid.startswith('cmid:') else mid)
                    result = await asyncio.wait_for(adapter._vk_method('messages.edit', params), timeout=30)
                    if not (type(result) is int and result == 1 or isinstance(result, dict) and 'error' not in result
                            and type(result.get('response')) is int and result['response'] == 1):
                        code = (result.get('error') or {}).get('error_code') if isinstance(result, dict) else None
                        finish('pending' if code in (6, 9, 10, 29) else 'held', 'vk_edit_rejected')
                        return False
                else:
                    finish('held', 'edit_unsupported')
                    return False
                return finish('done')
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if row['request']['platform'] != 'telegram':
                    transient = (self.vk_feedback_retryable(exc) if row['request']['platform'] == 'vk'
                                 else isinstance(exc, (TimeoutError, ConnectionError)))
                    finish('pending' if transient else 'held', 'edit_transient' if transient else 'edit_unknown')
                    return False
                from telegram.error import BadRequest, Forbidden, RetryAfter, NetworkError
                if isinstance(exc, BadRequest) and 'message is not modified' in str(exc).lower():
                    return finish('done', 'already_current')
                if isinstance(exc, BadRequest) and 'message to edit not found' in str(exc).lower():
                    finish('deleted', 'message_deleted')
                elif isinstance(exc, BadRequest):
                    finish('held', 'edit_rejected')
                elif isinstance(exc, RetryAfter):
                    delay = exc.retry_after
                    from datetime import timedelta
                    delay = delay.total_seconds() if isinstance(delay, timedelta) else float(delay)
                    finish('pending', 'edit_transient', retry_after=max(0, delay))
                elif isinstance(exc, (TimeoutError, NetworkError, ConnectionError)):
                    finish('pending', 'edit_timeout' if isinstance(exc, TimeoutError) else 'edit_transient')
                elif isinstance(exc, Forbidden):
                    finish('pending', 'edit_forbidden')
                else:
                    finish('held', 'edit_unknown')
                return False

    def wire(self, platform, native, adapter):
        if platform != 'telegram' or not self.live:
            return self._wire(platform, native, adapter)
        with self._lock:
            if not self.live:
                return
            self._heartbeat_started = True
            self._heartbeat_stage = None
            self._heartbeat_reason = None
            owner = getattr(adapter, '_owner_profile', None) or self.store.profile
            if owner != self.store.profile:
                self._heartbeat_reason = 'route_denied'
                self._heartbeat_stage = 'wire'
            try:
                return self._wire(platform, native, adapter)
            except Exception:
                self._heartbeat_reason = 'wire_failed'
                self._heartbeat_stage = 'wire'
                raise  # Preserve the existing wire failure contract.
            finally:
                self._heartbeat()

    def _wire(self, platform, native, adapter):
        from . import lifecycle
        with self._lock:
            if not self.live:
                return
            owner = getattr(adapter, '_owner_profile', None) or self.store.profile
            if owner != self.store.profile:
                return
            loop = asyncio.get_running_loop()
            self._desired[platform] = (native, adapter)
            current = self.adapters.get(platform)
            receipt = None
            if current is not None:
                if current[0] is native and current[1] is adapter:
                    return
                receipt = self._retire(platform)
            self._pending_bindings[platform] = (native, adapter, loop)
            receipt = receipt or self._drains.get(platform) or lifecycle.barrier(
                self.store.home, platform, adapter)
            if receipt is not None and (not receipt.done() or not receipt.result()):
                lifecycle.remember(self.store.home, platform, native, adapter, loop, receipt)
                if self._reasons.get(platform) == 'draining':
                    return
                self._reasons[platform] = 'draining'
                self._drains[platform] = receipt
                task = self._spawn(self._await_wire(platform, native, adapter, receipt), platform)
                self._recovery.add((loop, task))
                task.add_done_callback(lambda t: self._recovery.discard((loop, t)))
                return
            lifecycle.remember(self.store.home, platform, native, adapter, loop, receipt)
            before = len(self.cleanups)
            self.adapters[platform] = (native, adapter)
            try:
                if platform == 'telegram':
                    self.wire_telegram(native, adapter)
                elif platform == 'discord':
                    self.wire_discord(native, adapter)
                elif platform == 'vk':
                    self.wire_vk(adapter)
                self.workers[platform] = self._spawn(self.worker(platform), platform)
                if platform == 'telegram':
                    self.workers[platform].add_done_callback(self._heartbeat_done)
            except Exception:
                self.adapters.pop(platform, None)
                cleanups = self.cleanups[before:]
                del self.cleanups[before:]
                receipt = lifecycle.drain(loop, tuple(self._tasks.get(platform, ())), cleanups)
                lifecycle.remember(self.store.home, platform, native, adapter, loop, receipt)
                self._reasons[platform] = 'wire_failed'
                raise
            cleanups = self.cleanups[before:]
            del self.cleanups[before:]
            self._bindings[platform] = (native, adapter, loop, cleanups)
            lifecycle.remember(self.store.home, platform, native, adapter, loop)
            self._reasons.pop(platform, None)
            self._drains.pop(platform, None)

    async def _await_wire(self, platform, native, adapter, receipt):
        from . import lifecycle
        # Poll the passive token from this runtime-owned bounded waiter. Attaching
        # Future callbacks would make the cached token retain asyncio tasks/contexts.
        loop = asyncio.get_running_loop()
        deadline = loop.time() + lifecycle.DRAIN_TIMEOUT + .1
        while not receipt.done() and self.live and loop.time() < deadline:
            await asyncio.sleep(.01)
        result = receipt.result() if receipt.done() else False
        if not self.live:
            return
        self._reasons[platform] = None if result else 'drain_failed'
        native, adapter = self._desired.get(platform, (native, adapter))
        if result and adapter.is_connected:
            self.wire(platform, native, adapter)
        if platform == 'telegram' and self.live:
            self._heartbeat()

    async def worker(self, platform):
        while self.live:
            for stage, operation in (('dispatch', self.dispatch_once),
                                     ('continuation', self.dispatch_answers_once)):
                try:
                    await operation(platform)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self._reasons[platform] = stage + '_stage_failed'
                    if platform == 'telegram':
                        self._heartbeat_stage = stage
                    log.warning('Deferred question stage failed: %s', stage)
            try:
                task = self.feedback_workers.get(platform)
                if task is None or task.done():
                    self.feedback_workers[platform] = self._spawn(self.dispatch_feedback_once(platform), platform)
            except Exception:
                log.warning('Deferred feedback stage failed')
            if platform == 'telegram' and self.live:
                self._heartbeat()
            await asyncio.sleep(1)

    async def dispatch_answers_once(self, platform):
        """Opt-in continuation via normal gateway admission, not a separate agent."""
        from gateway.platforms.event import MessageEvent, MessageType
        from gateway.wake import admit_internal_event, WakeNotAccepted
        if not self.live or platform not in self.adapters:
            return
        _, adapter = self.adapters[platform]
        if not adapter.is_connected:
            return
        for row in self.store.pending_actions(platform):
            r = row['request']
            if not self.live or self.adapters.get(platform, (None, None))[1] is not adapter:
                return
            source = self._row_source(adapter, row, 'continuation')
            if source is None:
                continue
            # Pin exact lane; DM topic recovery must not redirect the action.
            try:
                key = adapter._source_session_key(source)
            except Exception:
                self._reasons[platform] = 'continuation_session_failed'
                continue
            if not key or not self.store.claim_action(row['id']):
                continue
            answer = row['answer']
            text = ('User answer to a deferred question.\n'
                    + 'Question: ' + r['question'] + '\n'
                    + 'Answer: ' + answer['value'] + '\n'
                    + 'Continue the current task using this answer and the relevant skill rules. '
                    + 'If another stage is needed, prepare its question with current options. '
                    + 'Do not repeat stages that have already been confirmed.')
            event = MessageEvent(text=text, message_type=MessageType.TEXT, source=source,
                                 internal=True, allow_gateway_control=False,
                                 metadata={'gateway_session_key': key, 'deferred_question_id': row['id']})
            try:
                await asyncio.wait_for(admit_internal_event(adapter, event), timeout=30)
                self.store.finish_action(row['id'], 'admitted')
            except WakeNotAccepted:
                self.store.finish_action(row['id'], 'pending')
            except asyncio.CancelledError:
                raise  # Leave dispatching held: no blind duplicate agent turn.
            except Exception:
                self.store.finish_action(row['id'], 'unknown')
                log.warning('Deferred answer continuation admission is uncertain')

    def _row_source(self, adapter, row, stage):
        r = row['request']
        try:
            source = self.source(adapter, **{k: r[k] for k in ROUTE_FIELDS if k != 'platform'})
            route = self.route(source)
            if route is None or any(route.get(k) != r.get(k) for k in ROUTE_FIELDS + ('profile', 'profile_home')):
                self._reasons[r['platform']] = 'route_denied'
                if r['platform'] == 'telegram':
                    self._heartbeat_stage = stage
                return None
            if not self.authorized(adapter, source):
                self._reasons[r['platform']] = 'authorization_denied'
                if r['platform'] == 'telegram':
                    self._heartbeat_stage = stage
                return None
            return source
        except Exception:
            self._reasons[r['platform']] = stage + '_source_failed'
            if r['platform'] == 'telegram':
                self._heartbeat_stage = stage
            return None

    async def dispatch_once(self, platform):
        if not self.live or platform not in self.adapters:
            return
        native, adapter = self.adapters[platform]
        if not adapter.is_connected:
            return
        for row in self.store.queued(platform):
            if not self.live or self.adapters.get(platform) != (native, adapter):
                return
            if self._row_source(adapter, row, 'dispatch') is None:
                continue
            if not self.store.claim(row['id']):
                continue
            attempt = self._spawn(self._deliver(platform, native, adapter, row), platform)
            if attempt is None:
                return
            try:
                # Cancellation of the worker must not swallow an acknowledged result
                # from a cancellation-resistant transport finalizer. The separately
                # owned attempt records its own receipt and participates in the drain.
                await asyncio.wait_for(asyncio.shield(attempt), timeout=30)
            except TimeoutError:
                attempt.cancel()
                self._reasons[platform] = 'send_timeout_held'
            except asyncio.CancelledError:
                raise

    async def _deliver(self, platform, native, adapter, row):
        if not self.live or self.adapters.get(platform) != (native, adapter):
            return
        try:
            delivery = await self.send(platform, native, adapter, row)
            self.store.receipt(row['id'], delivery.message_id if delivery else None,
                               None if delivery else 'no_confirmed_receipt', confirmed=delivery is not None)
        except asyncio.CancelledError:
            raise  # Durable sending remains held; never infer that no effect happened.
        except Exception:
            self.store.receipt(row['id'], diagnostic='transport_exception')

    def wire_vk(self, adapter):
        """Consume the generic namespace registrar; SDK acknowledgment stays in VK."""
        register = getattr(adapter, 'register_callback_handler', None)
        if not callable(register):
            return  # Old adapters may still offer honest explicitly addressed text.

        async def handler(obj):
            if not self.live or self.adapters.get('vk', (None, None))[1] is not adapter:
                return None
            task = self._spawn(self.vk_callback(adapter, obj), 'vk')
            return await task if task is not None else None

        cleanup = register('deferred_questions', handler)
        if not callable(cleanup):
            raise RuntimeError('VK callback registrar must return a cleanup')
        self.cleanups.append(cleanup)

    async def vk_callback(self, adapter, obj):
        """Fence actual event identity/receipt; never use payload or active lane."""
        if not self.live or self.adapters.get('vk', (None, None))[1] is not adapter:
            return None
        if not isinstance(obj, dict):
            return None

        def data(value, envelope=False):
            if isinstance(value, str):
                try:
                    value = json.loads(value)
                except ValueError:
                    return None
            if not isinstance(value, dict):
                return None
            if envelope or 'hermes_callback' in value:
                if (set(value) != {'hermes_callback', 'data'}
                        or value.get('hermes_callback') != 'deferred_questions'):
                    return None
                value = value.get('data')
            if not isinstance(value, dict) or set(value) != {'token'}:
                return None
            token = value.get('token')
            return token if isinstance(token, str) and TOKEN.fullmatch(token) else None

        supplied = [data(obj[key], envelope=key == 'payload')
                    for key in ('payload', 'data') if key in obj]
        if not supplied or any(token is None or token != supplied[0] for token in supplied):
            return None
        # bool, zero, signed/padded IDs are not native observed VK identities.
        ids = [obj.get(key) for key in ('peer_id', 'user_id', 'conversation_message_id')]
        if any(type(value) not in (int, str) or not re.fullmatch(r'[1-9][0-9]*', str(value))
               for value in ids) or not isinstance(obj.get('event_id'), str) or not obj['event_id']:
            return None
        peer, actor, cmid = map(str, ids)
        reply_lane = getattr(adapter, '_reply_lane', None)
        group_peer = getattr(adapter, '_is_group_peer', None)
        if not callable(reply_lane) or not callable(group_peer):
            return None
        try:
            lane = reply_lane(peer, {'reply_message': {'conversation_message_id': cmid}})
            if lane is not None and (not isinstance(lane, dict) or not isinstance(lane.get('id'), str)
                                     or not lane['id']):
                return None
            source = self.source(adapter, chat_id=peer, user_id=actor,
                                 chat_type='thread' if lane else ('group' if group_peer(peer) else 'dm'),
                                 thread_id=f"lane:{lane['id']}" if lane else None)
            result = self.callback(adapter, supplied[0], source, f'cmid:{cmid}')
        except Exception:
            log.warning('Deferred VK callback rejected: invalid native identity')
            return None
        if result in ('answered', 'already_answered'):
            self._spawn(self.answer_feedback(TOKEN.fullmatch(supplied[0])[1]), 'vk')
            return 'Answer saved'
        return 'Answer not accepted'

    def vk_callback_capability(self, adapter, row, buttons):
        if not callable(getattr(adapter, 'register_callback_handler', None)):
            return [], 'callback-registration-unavailable'
        if not all(callable(getattr(adapter, name, None)) for name in ('_reply_lane', '_is_group_peer')):
            return [], 'callback-route-unavailable'
        r = row['request']
        if r['thread_id']:
            resolve = getattr(adapter, '_resolve_lane', None)
            if not r['thread_id'].startswith('lane:') or r['chat_type'] != 'thread' or not callable(resolve):
                return [], 'unsupported-thread'
            lane_id = r['thread_id'].removeprefix('lane:')
            lane = resolve(r['chat_id'], lane_id)
            if not lane or lane.get('id') != lane_id:
                return [], 'lane-address-unavailable'
        elif r['chat_type'] != ('group' if adapter._is_group_peer(r['chat_id']) else 'dm'):
            return [], 'native-route-mismatch'
        if not buttons or len(buttons) > 10:
            return [], 'option-count'
        labels = [b['label'] for b in buttons]
        if any('\n' in label or '\r' in label or len(label.encode('utf-16-le')) // 2 > 40 for label in labels):
            return [], 'label-budget'
        return labels, 'callback'

    def vk_text_address(self, adapter, row):
        """Read-only explicit text addressing; never emits text-action buttons."""
        r = row['request']
        required = ('_is_group_peer', '_peer_lanes', '_resolve_lane',
                    '_pending_create', '_pending_edit', '_select_message_lane',
                    '_strip_bot_mention_prefix', '_passes_invocation_gate')
        if not all(callable(getattr(adapter, name, None)) for name in required):
            return [], 'native-intake-unavailable', None
        peer, actor = r['chat_id'], r['user_id']
        prefix = ''
        group = adapter._is_group_peer(peer)
        requires = adapter.require_mention_by_peer.get(peer, adapter.require_mention)
        if group and requires:
            community = str(adapter.group_id or '')
            if not re.fullmatch(r'[1-9][0-9]*', community):
                return [], 'community-address-unavailable', None
            prefix = f'@club{community} '
        thread = r['thread_id']
        if thread:
            if r['chat_type'] != 'thread' or not thread.startswith('lane:'):
                return [], 'unsupported-thread', None
            lane_id = thread.removeprefix('lane:')
            lane = adapter._resolve_lane(peer, lane_id)
            if (not lane or lane['id'] != lane_id
                    or not re.fullmatch(r'[a-z0-9_-]+', lane_id)):
                return [], 'lane-address-unavailable', None
            prefix += f'@{lane_id} '
        elif (adapter._peer_lanes(peer) or {}).get('lanes'):
            return [], 'root-with-projects', None
        elif r['chat_type'] != ('group' if group else 'dm'):
            return [], 'native-route-mismatch', None
        if adapter._pending_create(peer, actor, prune=False) or adapter._pending_edit(peer, actor, prune=False):
            return [], 'pending-project-input', prefix
        return [], 'explicit-text', prefix

    async def send(self, platform, native, adapter, row):
        r, snapshot = row['request'], row['snapshot']
        buttons = snapshot['buttons'] if len(snapshot['buttons']) <= 24 else []
        if platform == 'telegram':
            from telegram import InlineKeyboardButton, InlineKeyboardMarkup
            args = {'chat_id': r['chat_id'], 'text': snapshot['text'], 'parse_mode': None}
            if r['thread_id']:
                # A bare thread is Hermes' message_thread_id even in private
                # chats. Native direct topics need an explicit discriminator;
                # never infer that different lane from chat_type == dm.
                args['message_thread_id'] = int(r['thread_id'])
            if buttons:
                args['reply_markup'] = InlineKeyboardMarkup([
                    [InlineKeyboardButton(b['label'][:75], callback_data=b['token'])] for b in buttons])
            # Same live PTB Bot, one SDK call, no topic-deleting fallback or clarify state.
            message = await native.bot.send_message(**args)
            if str(message.chat.id) != r['chat_id']:
                return None
            actual_thread = getattr(message, 'message_thread_id', None)
            dm_topic = getattr(getattr(message, 'direct_messages_topic', None), 'topic_id', None)
            if r['thread_id'] and (dm_topic is not None or str(actual_thread) != r['thread_id']):
                return None
            return Delivery(str(message.message_id))
        if platform == 'slack':
            # Workspace MUST be known; _get_client may otherwise fall back to primary.
            client = adapter._team_clients.get(r['scope_id'])
            if client is None:
                raise RuntimeError('workspace unavailable')
            args = {'channel': r['chat_id'], 'text': snapshot['text'], 'unfurl_links': False, 'unfurl_media': False}
            if r['thread_id']:
                args['thread_ts'] = r['thread_id']
            if buttons:
                blocks = [{'type': 'section', 'text': {'type': 'plain_text', 'text': snapshot['text']}}]
                elements = [{'type': 'button', 'text': {'type': 'plain_text', 'text': b['label'][:75]},
                             'action_id': f'hermes_dq_{i}', 'value': b['token']} for i, b in enumerate(buttons)]
                blocks += [{'type': 'actions', 'elements': elements[i:i + 5]} for i in range(0, len(elements), 5)]
                args['blocks'] = blocks
            result = await client.chat_postMessage(**args)
            if not result.get('ok') or result.get('channel') != r['chat_id']:
                return None
            return Delivery(str(result['ts'])) if result.get('ts') else None
        if platform == 'discord':
            import discord
            # Discord chat_id is the actual channel/thread ID; no inferred auto-thread.
            target = r['thread_id'] or r['chat_id']
            channel = native.get_channel(int(target)) or await native.fetch_channel(int(target))
            # Lookup can suppress cancellation and return after unload/replacement.
            # Fence the NEW send/View, not an ACK from a send already in flight.
            if not self.live or self.adapters.get(platform) != (native, adapter):
                return None
            guild = getattr(channel, 'guild', None)
            if (str(guild.id) if guild else None) != r['scope_id']:
                raise RuntimeError('guild mismatch')
            args = {'content': snapshot['text'], 'allowed_mentions': discord.AllowedMentions.none()}
            if buttons:
                view = discord.ui.View(timeout=None)
                for b in buttons:
                    view.add_item(discord.ui.Button(label=b['label'][:80], custom_id=b['token']))
                args['view'] = view
                self._bindings[platform][3].append(view.stop)
            message = await channel.send(**args)
            if str(message.channel.id) != target:
                return None
            return Delivery(str(message.id))
        if platform == 'line' and buttons and len(buttons) <= 13:
            # Message actions enter the adapter's ordinary authorized text-event path.
            items = [{'type': 'action', 'action': {'type': 'message', 'label': b['label'][:20],
                                                 'text': f"dq {row['id']} {i}"}}
                     for i, b in enumerate(buttons, 1)]
            await adapter._client.push(r['chat_id'], [{'type': 'text', 'text': snapshot['text'],
                                                       'quickReply': {'items': items}}])
            return Delivery(None)  # LINE client acknowledges push but exposes no wire ID.
        if platform == 'vk':
            labels, reason = self.vk_callback_capability(adapter, row, buttons)
            _, text_reason, prefix = self.vk_text_address(adapter, row)
            text = '\n'.join([r['question'], '', *[
                f'{i}. {option}' for i, option in enumerate(r['options'], 1)]])
            if labels:
                text += '\n\nChoose an option using a button below.'
                if prefix is not None:
                    text += f'\nText answer: {prefix}dq {row["id"]} NUMBER'
                    if r['allow_text']:
                        text += f'\nCustom answer: {prefix}dq {row["id"]} text: YOUR ANSWER'
            args = {'peer_ids': r['chat_id'], 'message': text, 'random_id': -row['delivery_nonce']}
            if not 0 < row['delivery_nonce'] <= 2_147_483_647:
                raise RuntimeError('VK delivery sequence exhausted')
            if labels:
                elements = [{'action': {'type': 'callback', 'label': label,
                                         'payload': json.dumps({'hermes_callback': 'deferred_questions',
                                                                'data': {'token': b['token']}})},
                             'color': 'secondary'} for b, label in zip(buttons, labels)]
                args['keyboard'] = json.dumps({'inline': True, 'buttons': [
                    elements[i:i + 2] for i in range(0, len(elements), 2)]}, ensure_ascii=False)
            else:
                log.warning('Deferred VK buttons unavailable: %s', reason)
                # Do not advertise bare dq as addressed or root replies as lane overrides.
                instruction = (f'Answer without buttons ({reason}; text: {text_reason}). '
                               'Finish creating/editing the project before answering.\n')
                answer_form = 'NUMBER' if r['options'] else 'text: YOUR ANSWER'
                if r['thread_id'] and prefix:
                    instruction += (f'Send: {prefix}dq {row["id"]} {answer_form}\n'
                                    'Or reply to this exact message: '
                                    f'dq {row["id"]} {answer_form} (the project binding must be preserved).')
                elif text_reason == 'root-with-projects':
                    instruction += ('Automatically addressed replies to the root chat are unavailable '
                                    '(root-with-projects) '
                                    'with projects: the host does not support explicit root selection. '
                                    'Do not answer from another project; an operator must verify the route.')
                elif prefix is not None:
                    instruction += f'Send: {prefix}dq {row["id"]} {answer_form}'
                else:
                    instruction += ('No verified addressing: an operator must correct the '
                                    'community/project configuration; do not answer from another project.')
                if prefix is not None and r['options'] and r['allow_text']:
                    instruction += f'\nCustom answer: {prefix}dq {row["id"]} text: YOUR ANSWER'
                args['message'] = text + '\n\n' + instruction
            result = await adapter._vk_method('messages.send', args)
            message_id = adapter._sent_message_id(result)
            if message_id:
                # Persist the known receipt BEFORE any await in optional local
                # bookkeeping, including timeout/cancellation/unload boundaries.
                # dispatch_once's later receipt is an idempotent no-op for sent.
                self.store.receipt(row['id'], str(message_id), confirmed=True)
            if message_id and r['thread_id']:
                try:
                    await adapter._remember_message_lane(r['chat_id'], message_id, r['thread_id'])
                except Exception:
                    # Network ACK is already known. Never turn a local failure into
                    # an uncertain send or discard the receipt (and never retry send).
                    log.warning('Deferred VK send acknowledged; local lane bookkeeping failed')
            return Delivery(str(message_id)) if message_id else None
        if (platform == 'whatsapp_cloud' and buttons and len(buttons) <= 10
                and len(snapshot['text']) <= 1024 and r['chat_type'] == 'dm' and not r['thread_id']):
            if len(buttons) <= 3:
                interactive = {'type': 'button', 'body': {'text': snapshot['text']},
                               'action': {'buttons': [{'type': 'reply', 'reply': {'id': b['token'],
                                                                               'title': b['label'][:20]}} for b in buttons]}}
            else:
                interactive = {'type': 'list', 'body': {'text': snapshot['text']},
                               'action': {'button': 'Choose', 'sections': [{'rows': [
                                   {'id': b['token'], 'title': b['label'][:24]} for b in buttons]}]}}
            result = await adapter._post_message_result(
                adapter._outbound_payload(r['chat_id'], 'interactive', interactive, None),
                fail_log='deferred question failed', reject_log='deferred question rejected: %d %s')
            return Delivery(str(result.message_id)) if result.success and result.message_id else None
        if (platform == 'feishu' and buttons and not r['thread_id']
                and r['chat_type'] == 'group' and r['user_id'].startswith('ou_')):
            # The existing generic card path emits /card events with the untouched SDK payload.
            # No replacement of the SDK's singleton callback dispatcher or process-memory clarify.
            actions = [{'tag': 'button', 'text': {'tag': 'plain_text', 'content': b['label'][:75]},
                        'type': 'default', 'value': {'hermes_deferred_question': b['token']}} for b in buttons]
            card = {'config': {'wide_screen_mode': True},
                    'elements': [{'tag': 'div', 'text': {'tag': 'plain_text', 'content': snapshot['text']}}] +
                                [{'tag': 'action', 'actions': actions[i:i + 5]} for i in range(0, len(actions), 5)]}
            response = await adapter._feishu_send_with_retry(
                chat_id=r['chat_id'], msg_type='interactive', payload=json.dumps(card, ensure_ascii=False),
                reply_to=None, metadata=snapshot['metadata'])
            result = adapter._finalize_send_result(response, 'deferred question failed')
            return Delivery(str(result.message_id)) if result.success and result.message_id else None
        result = await adapter.send(r['chat_id'], snapshot['text'], metadata=snapshot['metadata'])
        raw = result.raw_response or {}
        if not result.success or (isinstance(raw, dict) and raw.get('thread_fallback')):
            return None
        return Delivery(str(result.message_id) if result.message_id is not None else None)

    def wire_telegram(self, native, adapter):
        from telegram.ext import CallbackQueryHandler

        async def handle(update, context):
            query = update.callback_query
            message = query.message
            if message is None:
                await query.answer('unavailable')
                return
            if getattr(message, 'direct_messages_topic', None) is not None:
                await query.answer('wrong_route')
                return
            # The same decoder as actual intake preserves forum and private
            # topic semantics. The callback actor is NOT message.from_user
            # (that is our bot); only chat/topic facts come from the message.
            decoded = adapter._source_from_message_for_auth(message)
            source = self.source(adapter, chat_id=str(message.chat.id), user_id=str(query.from_user.id),
                                 chat_type=decoded.chat_type, thread_id=decoded.thread_id,
                                 is_bot=bool(query.from_user.is_bot))
            result = self.callback(adapter, query.data, source, message.message_id)
            await query.answer('Answer saved' if result in ('answered', 'already_answered') else result)
            if result in ('answered', 'already_answered'):
                match = TOKEN.fullmatch(query.data)
                if match is not None:
                    await self.answer_feedback(match[1])

        async def owned(update, context):
            if not self.live or self.adapters.get('telegram') != (native, adapter):
                return
            task = self._spawn(handle(update, context), 'telegram')
            if task is not None:
                await task
        handler = CallbackQueryHandler(owned, pattern=TOKEN)
        self.cleanups.append(lambda: native.remove_handler(handler, group=-20))
        native.add_handler(handler, group=-20)

    def wire_discord(self, native, adapter):
        async def handle(interaction):
            token = (interaction.data or {}).get('custom_id', '')
            if not TOKEN.fullmatch(token):
                return
            channel, guild = interaction.channel, interaction.guild
            # Match the adapter's native slash/message source representation.
            is_thread = bool(getattr(channel, 'parent_id', None))
            source = self.source(adapter, chat_id=str(interaction.channel_id), user_id=str(interaction.user.id),
                                 chat_type='dm' if guild is None else ('thread' if is_thread else 'channel'),
                                 thread_id=str(interaction.channel_id) if is_thread else None,
                                 parent_chat_id=str(channel.parent_id) if is_thread else None,
                                 scope_id=str(guild.id) if guild else None,
                                 is_bot=bool(getattr(interaction.user, 'bot', False)))
            message = getattr(interaction, 'message', None)
            result = self.callback(adapter, token, source, getattr(message, 'id', None))
            if not interaction.response.is_done():
                await interaction.response.send_message(result, ephemeral=True)

        async def owned(interaction):
            if not self.live or self.adapters.get('discord') != (native, adapter):
                return
            task = self._spawn(handle(interaction), 'discord')
            if task is not None:
                await task
        self.cleanups.append(lambda: native.remove_listener(owned, 'on_interaction'))
        native.add_listener(owned, 'on_interaction')

    async def slack_action(self, ack, body, action):
        await ack()
        binding = self.adapters.get('slack')
        if not self.live or binding is None:
            return
        _, adapter = binding
        message = body.get('message') or {}
        channel = body.get('channel') or {}
        chat_id = channel.get('id', '')
        chat_type = 'dm' if chat_id.startswith('D') else 'group'
        thread = message.get('thread_ts')
        source = self.source(adapter, chat_id=chat_id, user_id=(body.get('user') or {}).get('id', ''),
                             chat_type=chat_type, thread_id=str(thread) if thread else None,
                             scope_id=(body.get('team') or {}).get('id'))
        self.callback(adapter, action.get('value'), source, message.get('ts'))
