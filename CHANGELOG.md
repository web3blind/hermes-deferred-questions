# Changelog

## 0.3.0

- Replace the idle database polling loop with profile-local wake notices and
  once-on-bind recovery. No per-question retry or expiry timers.
- Persist exact continuation correlation before admission and consume explicit
  technical turn-completion receipts without inspecting business outcomes.
- Remove completed payloads while retaining bounded text-free idempotency receipts;
  preserve unknown/interrupted/legacy work conservatively.
- Preserve monotonic VK send nonces across deletion and additive migration.
- Keep acknowledged sends and admission receipts through store contention and
  lifecycle cancellation without replaying transport or model effects.
- Document cold-upgrade requirements, event-only diagnostics and delivery limits.

## 0.2.5

- Protect every Store connection with a persistent nonblocking inter-process lock.
- Enroll trusted private format-header metadata on a healthy open and detect header
  damage both on startup and in a running worker, without an external cron.
- Quarantine and validate header-only repairs; preserve ledger body and delivery,
  answer and continuation receipts. Never roll back an old ledger or rebuild an
  initialized missing store. Live journals and non-header corruption fail closed.
- Stop all older plugin processes and external store-replacement watchdogs before
  upgrading; direct third-party SQLite writers are outside the lock protocol.

## 0.2.4

- Require Hermes >=0.21.6 and declare Linux/macOS support.
- Use English feedback, callback instructions and continuation messages while
  preserving the user's original question, labels and answer text.
- Remove completed per-question feedback locks through a stable, nonblocking
  admission guard; preserve retry/cancellation safety across processes.
- Append the directory-plugin path without overriding existing import priority.
- Add real cross-process contention, cancellation, unload and cleanup-failure
  regressions. Stop older gateway processes before upgrading the lock protocol.

## 0.2.3

- Public distribution with a conventional Python package, wheel entrypoint and
  directory-plugin bridge that retains package identity across core force reload.
- Durable queued questions, exact-route answer capture, optional continuation,
  feedback outbox and lifecycle-owned gateway workers.
- Telegram readiness/recovery and private, non-content runtime diagnostics.
- VK generic callback integration (requires hermes-vk-platform 0.2.6+), native
  feedback error classification and addressed-text fallback for older adapters.
- Portable isolated regression suite and catalog submission guidance.

The public tree begins with clean history. Private deployment records, profiles,
verification artifacts and credentials are not part of this distribution.
