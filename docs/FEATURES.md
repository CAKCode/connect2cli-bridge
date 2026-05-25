# Feature Guide

This document describes what the current bridge implements, what the main feature areas are, and where the boundaries are.

## Core Positioning

This bridge is a headless runtime that connects WeCom bot conversations to the `codex` CLI.

It is designed around:

- local deployment
- persistent session state
- workspace-aware Codex execution
- operational control through files, local commands, and JSON APIs

It is not a web application and does not provide a browser UI.

## Feature Areas

### 1. WeCom connectivity

Implemented:

- long-lived WeCom WebSocket connection per active bot
- reconnect loop and bot runtime status tracking
- inbound text, image, file, and mixed-message handling
- multi-instance bot runtime locking so only one instance actively owns a bot connection

Not implemented:

- OAuth login flow
- browser-based bot management UI

### 2. Multi-bot configuration and persistence

Implemented:

- multiple bot definitions
- startup bootstrap from environment variables or a JSON file
- persisted bot config file via `.bots.json`
- restart, stop, delete, and standby behavior across instances

Important constraint:

- plaintext `secret` persistence is intentionally disabled
- bot secrets must come from `secretFile` or in-memory bootstrap input

### 3. Session lifecycle and resume

Implemented:

- session persistence with `sessionId`
- Codex thread resume with `threadId`
- chat queue management
- session lease model to coordinate work across processes

Session addressing:

- `single:USER_ID`
- `group-user:CHAT_ID:USER_ID`
- `group:CHAT_ID`

Recommended usage:

- prefer `chatKey` for external integrations
- treat `sessionId` as a durable fallback identifier

### 4. Group session models

Implemented:

- `per-user`
  each member in a group gets a separate logical session
- `shared`
  the whole group shares one session

This allows the bridge to support both collaborative room workflows and isolated per-member workflows without changing the bot integration model.

### 5. Codex execution model

Implemented:

- `codex exec`
- `codex exec resume`
- execution mode switch through `CODEX_EXEC_MODE`
- global concurrency limit through `MAX_CONCURRENT_CODEX_RUNS`

Modes:

- `sandboxed`
  bridge starts Codex with `--full-auto`
- `host`
  bridge starts Codex with `--dangerously-bypass-approvals-and-sandbox`

Default: `host`

Important boundary:

- Codex auth is inherited from the runtime user's `CODEX_HOME`
- the bridge does not manage Codex login for you

### 6. Workspace and file layout

Implemented:

- per-user persistent workspace
- per-room shared workspace
- per-session `chatfile` exchange directory
- workspace-local Codex skills
- project-shared skills under `relate-skills/`

Effective runtime layout centers around:

- `workspace/<bot>/users/<user>/workfile`
- `workspace/<bot>/rooms/<room>/roomfile`
- `workspace/<bot>/sessions/<chat-key>/chatfile`

Behavior:

- Codex usually runs with `workfile` as `cwd`
- pure room-shared sessions run with `roomfile` as `cwd`
- temporary directories are redirected to the current session `chatfile`

### 7. File ingestion and outbound file delivery

Implemented:

- inbound WeCom media download to local files
- outbound file delivery via `send_file.py`
- outbound file delivery via `POST /api/send-file`
- queue-based local file-send workflow

Security boundary:

- the bridge does not allow arbitrary outbound file access by default
- extra file roots must be explicitly allowlisted with `FILE_SEND_ROOTS`

### 8. Built-in session control commands

Implemented:

- `/bridge-status`
- `/bridge-interrupt`
- `/bridge-reset`

Meaning:

- `status` reports current runtime state and identifiers
- `interrupt` stops the current run without resetting the conversation state
- `reset` clears the active session context and starts fresh

API equivalents also exist for interrupt and reset.

### 9. Scheduling

Implemented:

- one-shot scheduled messages
- cron-based recurring schedules
- local command entrypoint through `schedule_message.py`
- JSON APIs for create, inspect, pause, resume, and delete

Current scheduling model:

- all scheduling is normalized into cron definitions
- one-shot requests are internally converted into `cron + maxRuns=1`
- trigger precision is minute-level

Current persistence model:

- definitions under `.scheduled-messages/definitions/`
- trigger instances under `.scheduled-messages/pending|processing|done|failed/`

Current concurrency behavior:

- default schedule concurrency policy is `skip_if_running`

### 10. JSON APIs

Implemented:

- root health endpoint `/`
- bot management endpoints
- session chat fetch endpoint
- session interrupt/reset endpoints
- send-file endpoint
- schedule management endpoints
- schedule-message endpoint

Authentication behavior:

- if `BRIDGE_TOKEN` or `BRIDGE_BASIC_AUTH` is configured, `/api/*` requires auth
- otherwise `/api/*` is restricted to localhost callers

### 11. Operations and observability

Implemented:

- `check_bridge_health.sh`
- `check_restart_noise.sh`
- `smoke_bridge.sh`
- `test.sh`
- structured status and runtime logs in `bridge.log`

Operationally important persisted state includes:

- `.bots.json`
- `.session-registry/`
- `.session-locks/`
- `.scheduled-messages/`
- `workspace/`

### 12. Test coverage

Implemented:

- smoke coverage for critical bridge flows
- full pytest coverage for scheduling, file send, runtime, workspace, upload, and session behavior

This repo is already structured as a testable runtime, not just a prototype shell.

## Capability Boundaries

The current bridge does not try to be:

- a browser application
- a general-purpose task orchestration platform
- a secret vault
- a replacement for Codex login management
- a second scheduler model beyond cron-definition scheduling

That boundary is intentional. The project is optimized for a pragmatic bridge runtime, not for being a full platform layer.

## Recommended Reading Order

- Start with [README.md](../README.md) for the Chinese quick start
- Use [README.en.md](../README.en.md) for the English quick start
- Use [使用手册](使用手册.md) for deployment and operational details
- Use [cron-periodic-scheduler-design.md](cron-periodic-scheduler-design.md) for scheduling internals
