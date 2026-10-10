# Security policy

Do not include credentials, recipient IDs, question contents, databases, profile
configuration or raw private logs in public issues. Report vulnerabilities via
GitHub private vulnerability reporting when enabled; otherwise ask the maintainer
for a private reporting channel without disclosing exploit details publicly.

This plugin runs with the gateway process's authority. Review the exact commit
before installation. It does not open a second bot connection or download updates.
Requests must specify an exact recipient and route; current adapter/gateway
permission checks are enforced again before sending and accepting answers.
SQLite contains sensitive questions and answers: retain and back it up as private
profile data. POSIX modes are defense in depth, not encryption.

Uncertain sends and continuations are held rather than blindly retried. A queued
request is not proof of delivery, and a runtime heartbeat is not a delivery receipt.
No live production state should be used for tests. Only the latest released version
receives fixes; no guaranteed response SLA is provided.
