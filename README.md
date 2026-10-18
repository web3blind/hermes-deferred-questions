# Hermes Deferred Questions

Durable, non-blocking questions through an **already-connected Hermes gateway**.
The CLI or optional tool enqueues a question and returns immediately. Gateway-owned
workers deliver it, validate the answer and save it in profile-local SQLite.
No second bot connection, blocking answer waiter, cron patch or core patch is added.

## Requirements and verification scope

- Python 3.11+ and **Hermes >=0.21.6**, which introduced the gateway APIs used
  here: platform handler registration, unload callbacks, internal-event admission
  and canonical routing identity.
- Supported operating systems: **Linux and macOS** (`platforms: [linux, macos]`).
  Feedback serialization requires POSIX `fcntl.flock`; **Windows is unsupported**.
  Verification reported here runs on Linux, not macOS or Windows.
- The selected platform's normal Hermes dependencies and credentials. This plugin
  adds no third-party Python runtime dependencies; `dependencies` and manifest
  `python_dependencies` are empty. Hermes's Git installer resolves declared
  dependencies, **not the plugin's own project distribution**.
- For VK native callbacks, **hermes-vk-platform 0.2.6+**, with
  `register_callback_handler(namespace, async_handler)` and listener removal.
  Older adapters fall back to explicitly addressed text, never fake buttons.

The regression suite exercises real PluginManager discovery, CLI/tool registry,
SQLite, wheel/sdist, unload/force reload, native Telegram adapter and public VK
callback decoder. Network/model effects are controlled at transport boundaries.
Telegram and VK have been exercised in the originating deployment; this is not
certification of other platforms or an automatic production health check.

## Event-driven processing and retention (0.3.0)

The plugin does not poll SQLite while idle and has no per-question retry/expiry
scheduler. Producers commit first, then issue one profile-local Unix-socket wake.
The gateway recovers queued work once on binding/reconnect and otherwise waits
for notices. Button callbacks save the answer and wake continuation directly.
A notice arriving during an in-flight feedback edit schedules one further pass;
store contention requests a lease-release notice, not a retry timer. A bounded
native connection-readiness wait performs no database reads and stops after ten
seconds. If native readiness never arrives, the runtime reports degraded state
and waits for a new connection/wake event, rather than polling indefinitely.

`request` returns `result.notification_sent`: it confirms the wake receipt, not
message delivery or model completion. If it is false, the question is still
persisted. Binding/reconnect recovery or `hermes deferred-questions wake` can
wake it later. A producer crash between commit and notice likewise leaves durable
work; there is no hidden retry daemon promising eventual delivery without a new
event. Expiry is checked on access; there is no timed removal of old buttons.

For `continue_agent: true`, an atomic claim saves a random attempt token before
admission. A technical marker in the current internal input correlates that token
with the opaque `pre_llm_call.turn_id`. Only explicit
`on_session_end(completed=True, failed=False, interrupted=False)` permits content
retirement, after applicable feedback removal is finished. Missing fields,
interruption, error and ambiguous admission keep the record. This is technical
turn completion, **not proof of outbound delivery or of the skill's business
result**; the plugin never judges whether an agent scheduled the correct cron.

Retirement removes the question, options, snapshot, answer, action and feedback
payload. A text-free receipt (ID, idempotency key, request hash and terminal state)
is retained for seven days, or through the original answer deadline if later.
Receipt pruning is opportunistic on opening the store, not a background timer.
SQLite reuses cleared pages; `secure_delete` clears retired payload bytes in the
live ledger, but external backups/quarantines are separate operator-owned data.
VK delivery nonces now have a persistent monotonic sequence and are not reused
when rows are deleted. Existing nonce values are preserved during migration.

Uncertain old `admitted` records without a completion token cannot be certified
retroactively and are not silently erased. Storage-only answers without
`continue_agent` have no agent-completion receipt and remain available to their
consumer. Unsupported message-edit surfaces cannot guarantee visual button
removal, but retired IDs never trigger another continuation. Stop old plugin
processes and back up the ledger before upgrading this additive schema/protocol.

## Built-in store protection (0.2.5)

No cron or external watchdog is required. Every CLI and gateway database operation
uses one persistent, nonblocking cross-process store lock. Contention fails with a
retryable busy error rather than blocking the gateway event loop.

On the first healthy open, the plugin stores a private, path-bound template of
SQLite's first 24 format bytes. On subsequent opens, it checks those bytes before
opening SQLite. If only that header is damaged and no nonempty WAL/journal exists,
it keeps the original in `deferred-questions/quarantine/`, prepares a candidate,
and requires a full SQLite integrity check and the plugin tables before publishing
it. All ledger bytes after offset 23 are preserved. Delivered receipts, answers,
rowids and continuation states are never restored from an older snapshot.

This is damage containment and recovery, **not a fix for an unrelated host/TLS FD
bug**. Body damage, live journals, missing initialized ledgers, invalid metadata,
and corrupt legacy stores without a trusted template fail closed. They require
explicit offline recovery; the plugin never silently creates an empty replacement
or rolls back to an old ledger (which could replay sends).

**Upgrade boundary:** stop all gateway/CLI processes running older plugin versions
and disable external scripts that replace this database before activating 0.2.5.
Older versions and direct third-party SQLite writers do not honor the store lock.
Do not move the database without its recovery metadata; paths are profile-bound.
The persistent `store.lock` must not be removed while any process is running.

The bundled catalog example pins the published 0.2.5 source commit. Catalog
admission still requires an upstream maintainer review.

## Install

This repository is prepared for catalog review; it is **not yet an approved
Hermes catalog entry**. Review an exact commit before installing custom code.

From a published Git repository, using the intended profile's normal Hermes CLI:

```sh
hermes plugins install https://github.com/web3blind/hermes-deferred-questions --ref REVIEWED_40_HEX_COMMIT_SHA
hermes plugins enable deferred-questions
```

Replace the SHA placeholder with the actual reviewed full commit. The root
`plugin.yaml` and `__init__.py` are the directory-plugin entry. The bridge makes
its bundled `hermes_deferred_questions` package available to ordinary Python
imports; core force reload evicts the directory wrapper, not that package.
This preserves the passive connection/drain cache needed for native recovery,
including force reload before Telegram readiness. No dynamic downloader or
self-updater is included.

Alternatively, build a wheel and install it into the **actual gateway Python
environment**, with that environment owner's permission and package manager.
The distribution exposes `hermes_agent.plugins` → `hermes_deferred_questions`.
Do not retain a conflicting same-name directory installation during an entrypoint
migration: Hermes prefers directory plugins over distribution entrypoints.
Use a backed-up, operator-managed gateway lifecycle; this repository does not
restart gateways or modify live environments as part of verification.

The CLI works independently of tool exposure. The optional `deferred_questions`
tool is off by default:

```sh
hermes config set plugins.entries.deferred-questions.settings.enable_tool true
```

Expose its `deferred_questions` toolset only where needed using normal Hermes
toolset configuration. Per-call checks enforce opt-in. A recipient ID in a
request does not authorize sending; current gateway and adapter policy must grant it.

## Request and read answers

Generate fresh JSON with your adapter's **canonical IDs**, not display names.
`expires_at` must be an absolute future Unix timestamp. The following helper
writes a request to stdout; `123` and `456` are synthetic example IDs to replace:

```sh
python3 -c 'import json,time; print(json.dumps({"platform":"telegram","chat_id":"123","user_id":"456","chat_type":"dm","question":"Which route today?","options":["Walk","Bus","Stay home"],"allow_text":True,"expires_at":time.time()+3600,"idempotency_key":"unique-occurrence"}))' > request.json
hermes deferred-questions request --json-file request.json
hermes deferred-questions request --json-file - < request.json
hermes deferred-questions list --limit 50
hermes deferred-questions get QUESTION_ID
hermes deferred-questions cancel QUESTION_ID
```

`request` returns JSON with `state: queued`: **not a delivery acknowledgment**.
Successful commands print an `ok` result; invalid requests return exit code 2.
`get` includes the original request, immutable rendered snapshot, transport
receipt, answer and continuation state. `list` defaults to 50, maximum 200.

Answer using native options where supported, or explicit addressed text:

```text
dq QUESTION_ID 2
dq QUESTION_ID text: My own answer
```

Bare numbers, ordinary prose and arbitrary replies never select a pending
question. Custom text requires `text:`. Up to 100 unique, nonempty option labels
and an 1800 UTF-16-unit rendered question budget are enforced. Zero options with
`allow_text: true` is supported. Oversized native option sets use numbered text;
the complete list and original labels remain in the stored snapshot.

## Safety and durability

- Exact platform, chat type/ID, responder, thread, workspace/guild scope, parent,
  profile name and canonical home must match. Supply `thread_id`, `scope_id` and
  `parent_chat_id` when the real adapter provides them. IDs are not normalized or
  guessed. Missing or throwing authorization fails closed; a grant must be literal
  `True`. Adapter-local Telegram intake restrictions remain independently enforced.
- The DB is `$HERMES_HOME/deferred-questions/questions.sqlite3`; profile directory
  creation uses 0700 and DB uses 0600 on POSIX. SQLite WAL transactions serialize
  claims and first-answer wins. It is sensitive data, not encrypted storage.
- States are `queued`, `sending`, `sent`, `answered`, `expired`, `cancelled`,
  `unknown`. A claim records `sending` before the transport effect. A confirmed
  acknowledgment records `sent`; missing native IDs are never fabricated.
- Uncertain effects, timeouts, crashes and interrupted continuations are held,
  **not blindly replayed**. Inspect the real platform before creating a new
  occurrence. The plugin has no unsafe reset-and-retry command.
- Identical requests with the same idempotency key return the existing row;
  different content under the same key is rejected. Reuse the exact JSON,
  including expiry, for retries; use a new key for each scheduled occurrence.
- Expiry is materialized during queue/read/answer access. Late answers cannot win.
  Cancellation does not retract a platform message and cannot overwrite an answer.
- Telegram and VK answer/expiry/cancellation feedback uses a durable edit outbox.
  Failed edits do not erase accepted answers. Transient edits may be revisited on a later explicit wake;
  uncertain/deleted/permanent outcomes are held or marked deleted, not resent.
  Per-question feedback locks are removed only after durable `done`, while still
  locked. A single persistent `feedback.lock` admission guard serializes lock
  opening and terminal unlinking across processes, preventing split-inode races.
  Retryable, interrupted and held feedback retains its per-question lock.
  Stop old gateway processes before upgrading: versions without this admission
  guard must not share the feedback ledger concurrently with the new lock protocol.
- Gateway absent/disconnected/unauthorized: questions remain queued or expire.
  Gateway and sender must share the same profile home. Borrowing another profile's
  bot is not silently supported.

For cron, use an existing `no_agent` script to generate an occurrence's JSON and
invoke the CLI once, with normal `deliver: local` to avoid a second ordinary
message containing CLI output. Cron only schedules; SQLite owns answers.

Set request `continue_agent: true` only to resume that chat task. Accepted answer
and pending continuation are persisted atomically, then revalidated and admitted
through the existing adapter. `continuation_state: admitted` means scheduled,
not a completed model turn. Without the flag, answers are storage-only.

## Transport support and limits

- **Telegram:** inline callback buttons for 1–24 options, `dq:` namespace, removable
  handlers, exact receipt/topic binding. Forum questions use `chat_type: forum`;
  private-chat topics use `dm` with `thread_id`. These are Hermes topics via
  `message_thread_id`, not the separate Bot API `direct_messages_topic_id` lane.
- **VK 0.2.6+:** 1–10 inline callbacks, two per row, original labels within 40
  UTF-16 units. The generic `deferred_questions` namespace carries a `dq:` token.
  Actual peer/user/conversation-message receipt and persisted reply-lane mapping
  determine identity; payload/current-project guesses cannot certify a lane.
  Callbacks preserve pending project edits. Older adapters and oversized labels
  use addressed text. Mention-gated groups may require `@club99 @a dq ID 2`
  (synthetic addressing); pending project edits can outrank text routing.
- **Slack:** Block Kit 1–24 buttons through the existing workspace client and
  plugin action registry. Requires real conversation/workspace IDs; no primary
  workspace fallback. Controlled contract coverage, not live workspace E2E.
- **Discord:** 1–24 components with namespaced listeners and no memory waiter;
  exact channel/thread/guild/parent binding. Controlled contract coverage, not
  live Discord or installed-SDK certification.
- **Feishu/Lark:** safe unthreaded group card subset addressed by `ou_` open ID;
  unsupported callback identities use text. **WhatsApp Cloud:** 1–3 buttons or
  4–10 list rows, DM only, normal Meta free-form window restrictions apply.
  **LINE:** 1–13 quick-reply message actions with explicit `dq` text; null wire
  message IDs are allowed where the adapter cannot expose one.
- Other registered messaging transports use explicit numbered/text fallback over
  their existing send/MessageEvent seam. This is not live-account E2E certification.
  Notification-only deployments cannot collect replies. Native callback surfaces
  without safe receipt/route dispatch are deliberately not replaced or patched.

Force reload/unload owns cancellation and completion barriers, including pending
feedback and callbacks. Replacement waits for prior owned work; slow or failed
drains degrade closed instead of starting overlapping workers. The passive cache
is bounded to 32 connections and retains weak native/adapter/loop references, not
an active runtime or supervisor. Extra connections fail rather than evict barriers.

## Runtime diagnostics

`$HERMES_HOME/deferred-questions/telegram-runtime.json` is a private, read-only
operator observation, **not a delivery receipt**. Existing wire/worker/unload paths
write it atomically with 0600 permissions; there is no extra timer/client. CLI-only
operations do not create it. It contains PID, generation, timestamp, live/bound/
worker/adapter state and fixed reason/error-stage codes—not tokens, question text,
recipient IDs, profile IDs or raw exceptions. Write failures never affect delivery.

Snapshots describe the most recent processed event, not periodic liveness. An
old timestamp is normal while idle; missing, future-dated or mismatched-PID
snapshots cannot prove transport failure or health. PID reuse is possible;
`worker_alive` can briefly remain true during unload drain. Retired generations
must not overwrite their replacement. Read without modifying the file.

## Testing and catalog review

```sh
export HERMES_CORE_ROOT=/path/to/hermes-agent
export HERMES_VK_PLUGIN_ROOT=/path/to/hermes-vk-platform
export TMPDIR=/path/to/writable/scratch
scripts/run_tests.sh
python3 scripts/verify.py
```

Each invocation uses scratch HOME/HERMES_HOME, unique basetemp and the core's
canonical serial per-file runner. Explicit path options survive its `env -i`;
credentials do not. Without a public VK checkout (or canonical installed `vk`
plugin under the scratch home), cross-repository VK tests skip explicitly.
See [CONTRIBUTING.md](CONTRIBUTING.md) and [SECURITY.md](SECURITY.md).

The original regressions retain both legacy evicted-directory and conventional
entrypoint fixtures. The actual public bridge has a separate real install smoke:
commit the reviewed source, then run `scripts/verify_install.py` with a disposable
venv **under TMPDIR**, never the live core Python. It clones the exact commit
through loopback HTTPS using Git's smart HTTP backend and a temporary trusted CA,
passes the real catalog/dependency/scanner/metadata path, builds and installs a
wheel, compares installed bytes, and exercises two force reloads before Telegram
readiness on both directory and entrypoint paths. Native sends are not invoked;
only the adapter's manager-selection accessor is scoped to the test manager.

```sh
python3 -m venv "$TMPDIR/dq-install-venv"
"$TMPDIR/dq-install-venv/bin/python" -m pip install \
  'setuptools>=77,<84' 'wheel>=0.45,<0.47' \
  'PyYAML>=6,<7' 'ruamel.yaml>=0.18,<0.19' 'packaging>=24,<27' \
  'rich>=13,<15' 'python-dotenv>=1,<2' 'requests>=2,<3' \
  'httpx>=0.28,<0.29' 'python-telegram-bot>=22,<23'
"$TMPDIR/dq-install-venv/bin/python" scripts/verify_install.py
```

Git and `openssl` must be available. Scratch artifacts include a private test CA
key, test config/database, cloned tree, wheel and external catalog metadata;
keep them private and remove the disposable scratch environment when finished.
This does not demonstrate public GitHub reachability before publication. No CI
workflow is shipped without a real exercised CI result.

Catalog admission is a separate human-reviewed upstream process. A ready-to-copy
entry is provided in [catalog/deferred-questions.yaml](catalog/deferred-questions.yaml).
It pins the tested plugin-source commit preceding that metadata-only addition,
so the pin is not self-referential. Copy it into the upstream `plugin-catalog/`
in a reviewed PR; publication of this repository does not imply admission.
For future releases, obtain the full reviewed SHA and update the entry following the current [catalog schema](https://github.com/NousResearch/hermes-agent/blob/main/plugin-catalog/README.md)
and [submission guide](https://hermes-agent.nousresearch.com/docs/developer-guide/plugins/catalog-submission).
Use `name: deferred-questions`, community tier, automation category, this public
repository URL, that exact SHA, version `0.2.4`, maintainer `web3blind`, and declare
`provides_tools: [deferred_questions]`, `provides_hooks: [pre_gateway_dispatch]`,
empty `provides_middleware` and `requires_env`, `requires_hermes: ">=0.21.6"` and
`platforms: [linux, macos]`. Do not put a self-referential SHA pin inside the pinned
tree. The current catalog SHA still identifies the previously reviewed source,
not these uncommitted changes; update it only after a new commit is reviewed. Catalog install
must prove real clone/discovery/force-reload readiness; catalog visibility alone
is not dependency or lifecycle verification.

MIT © Denis Skripnik (web3blind).
