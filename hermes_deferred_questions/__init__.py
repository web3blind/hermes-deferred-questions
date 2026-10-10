"""Durable questions: conventional entrypoint and legacy directory loading."""
from __future__ import annotations

import json
import re
from hermes_constants import get_hermes_home
from .store import Store
from .runtime import Runtime

# Built-in and bundled messenger names with a connect-time platform-handler seam.
# A new third-party platform can be explicitly opted into via settings.extra_platforms.
MESSENGERS = ('telegram', 'discord', 'slack', 'whatsapp', 'whatsapp_cloud', 'signal',
              'mattermost', 'matrix', 'homeassistant', 'email', 'sms', 'dingtalk', 'feishu',
              'wecom', 'weixin', 'bluebubbles', 'qqbot', 'yuanbao', 'vk', 'photon', 'google_chat',
              'irc', 'line', 'teams', 'simplex', 'ntfy', 'a2a', 'buzz', 'raft')


def configure_parser(parser):
    sub = parser.add_subparsers(dest='dq_action', required=True)
    request = sub.add_parser('request', help='Enqueue a JSON request from a file (or - for stdin)')
    request.add_argument('--json-file', required=True)
    get = sub.add_parser('get', help='Read a question and durable answer/receipt')
    get.add_argument('id')
    listing = sub.add_parser('list', help='Read newest questions (default 50, max 200)')
    listing.add_argument('--limit', type=int, default=50)
    cancel = sub.add_parser('cancel', help='Cancel a queued/delivered question')
    cancel.add_argument('id')


def register(ctx):
    platforms = set(MESSENGERS)
    extra = ctx.get_config('extra_platforms', [])
    if not isinstance(extra, list) or any(not isinstance(p, str) or not re.fullmatch('[a-z][a-z0-9_]*', p) for p in extra):
        raise ValueError('extra_platforms must be a list of platform names')
    platforms.update(extra)
    store = Store(get_hermes_home(), ctx.profile_name)
    runtime = Runtime(ctx, store, platforms)

    def operate(action, request=None, qid=None, limit=50):
        if not runtime.live:
            raise ValueError('plugin unloaded')
        if action == 'request':
            return store.create(request, platforms)
        if action == 'get':
            return store.get(qid)
        if action == 'list':
            return store.list(limit)
        if action == 'cancel':
            return {'cancelled': store.cancel(qid)}
        raise ValueError('unknown action')

    def cli(args):
        import sys
        try:
            request = None
            if args.dq_action == 'request':
                from pathlib import Path
                raw = sys.stdin.read() if args.json_file == '-' else Path(args.json_file).read_text()
                request = json.loads(raw)
            result = operate(args.dq_action, request=request, qid=getattr(args, 'id', None),
                             limit=getattr(args, 'limit', 50))
            print(json.dumps({'ok': True, 'result': result}, ensure_ascii=False))
        except (ValueError, TypeError, OSError) as error:
            print(json.dumps({'ok': False, 'error': str(error)}, ensure_ascii=False))
            raise SystemExit(2)

    def tool(args, **kwargs):
        try:
            if ctx.get_config('enable_tool', False) is not True:
                raise ValueError('tool not enabled')
            result = operate(args.get('action'), request=args.get('request'), qid=args.get('id'),
                             limit=args.get('limit', 50))
            return json.dumps({'ok': True, 'result': result}, ensure_ascii=False)
        except (ValueError, TypeError) as error:
            return json.dumps({'ok': False, 'error': str(error)}, ensure_ascii=False)

    request_properties = {k: {'type': 'string'} for k in ('platform', 'chat_id', 'user_id', 'chat_type',
                                                          'question', 'idempotency_key')}
    request_properties.update({k: {'type': ['string', 'null']} for k in ('thread_id', 'scope_id', 'parent_chat_id')})
    request_properties.update(options={'type': 'array', 'items': {'type': 'string'}, 'maxItems': 100},
                              allow_text={'type': 'boolean'}, continue_agent={'type': 'boolean'},
                              expires_at={'type': 'number'})
    schema = {'name': 'deferred_questions',
              'description': 'Enqueue a dynamic question without waiting for its answer, or read durable questions/answers. '
                             'Explicit recipient, exact platform route and absolute Unix expiry required. '
                             'Set request.continue_agent=true only when the answer should resume the existing chat task.',
              'parameters': {'type': 'object', 'additionalProperties': False,
                             'properties': {'action': {'type': 'string', 'enum': ['request', 'get', 'list', 'cancel']},
                                            'id': {'type': 'string'}, 'limit': {'type': 'integer', 'minimum': 1, 'maximum': 200},
                                            'request': {'type': 'object', 'additionalProperties': False,
                                                        'properties': request_properties,
                                                        'required': ['platform', 'chat_id', 'user_id', 'chat_type', 'question',
                                                                     'idempotency_key', 'expires_at']}},
                             'required': ['action']}}
    ctx.register_tool('deferred_questions', 'deferred_questions', schema, tool,
                      check_fn=lambda: runtime.live and ctx.get_config('enable_tool', False) is True)
    ctx.register_cli_command('deferred-questions', 'Durable non-blocking questions', configure_parser, cli)
    ctx.register_hook('pre_gateway_dispatch', runtime.pre_dispatch)
    ctx.register_slack_action_handler(re.compile(r'^hermes_dq_[0-9]+$'), runtime.slack_action)

    # Factory qualnames must differ per platform, not the usual loop-lambda default argument trap.
    # Each registration owns fresh listeners/workers. A force-discovery disposes
    # the old runtime; its stable factory name must not suppress this new one.
    generation = runtime.generation

    import weakref
    runtime_ref = weakref.ref(runtime)
    def make_factory(platform):
        def factory(native, adapter):
            current = runtime_ref()
            if current is not None and current.live:
                current.wire(platform, native, adapter)
        factory.__qualname__ = f'deferred_questions.factory_{platform}_{generation}'
        return factory

    for platform in sorted(platforms):
        ctx.register_platform_handler(platform, make_factory(platform))
    runtime.recover()
