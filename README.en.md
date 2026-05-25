# WeCom Codex Bridge Python

Pure Python bridge service between WeCom bots and the `codex` CLI.

This project is a headless backend. It does not provide a web UI. The root path `/` returns JSON status only.

Chinese docs:

- Main README: [README.md](README.md)
- Operations manual: [docs/使用手册.md](docs/使用手册.md)
- Minimal Codex deployment flow: [README.codex-deploy.md](README.codex-deploy.md)

## What It Does

- Maintains long-lived WeCom WebSocket connections
- Manages multiple bot configurations and persistence
- Starts `codex exec` and `codex exec resume` per session
- Supports built-in session control commands:
  `/bridge-status`, `/bridge-interrupt`, `/bridge-reset`
- Downloads inbound WeCom images and files into local workspaces
- Sends local files back to WeCom through a local command or JSON API
- Supports one-shot scheduled messages and cron-based recurring schedules
- Exposes Bot / Session / Schedule JSON APIs

Default bind address:

```text
http://127.0.0.1:9299
```

## Quick Start

### 1. Prerequisites

- `python3` is installed
- `codex` is installed and executable
- the bridge runtime user has already run `codex login`
- you have a WeCom `botId`
- you have a readable secret file

Recommended checks:

```bash
which python3
which codex
codex login status
echo "${CODEX_HOME:-$HOME/.codex}"
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

For tests:

```bash
pip install -r requirements-dev.txt
```

### 3. Prepare `.env`

```bash
cp .env.example .env
```

Minimal example:

```bash
BRIDGE_BIND=127.0.0.1:9299
WORK_DIR=/home/jenkins
BRIDGE_BASIC_AUTH=bridge:change-me
CODEX_EXEC_MODE=host

WECOM_BOT_NAME=default
WECOM_BOT_ID=YOUR_BOT_ID
WECOM_BOT_SECRET_FILE=/run/secrets/wecom_default_secret
WECOM_BOT_WORK_DIR=/home/jenkins
WECOM_BOT_GROUP_SESSION_MODE=per-user
WECOM_BOT_ENABLED=true
```

Notes:

- Use either `BRIDGE_BASIC_AUTH` or `BRIDGE_TOKEN`
- if neither is configured, `/api/*` is limited to localhost
- plaintext bot `secret` is no longer supported; use `secretFile`
- `WECOM_BOT_WORK_DIR` is the bot's shared project root, not the exact Codex `cwd`

### 4. Start the service

```bash
sh ./start.sh
```

Then inspect logs:

```bash
tail -f ./bridge.log
```

Health check:

```bash
curl -s http://127.0.0.1:9299/
```

## Runtime Model

### Workspace layout

Important runtime paths:

- `workspace/<bot>/users/<user>/workfile`
  long-lived per-user workspace
- `workspace/<bot>/rooms/<room>/roomfile`
  shared room workspace
- `workspace/<bot>/sessions/<chat-key>/chatfile`
  session-level file exchange area
- `relate-skills/<skill>/SKILL.md`
  project-shared skills
- `<workfile or roomfile>/.codex/skills/<skill>/SKILL.md`
  workspace-local skills

When the bridge starts Codex:

- default `cwd` is `workfile`
- room-shared sessions use `roomfile`
- both `sandboxed` and `host` keep this `cwd` selection and do not force a fallback to `workDir`
- `TMPDIR`, `TMP`, and `TEMP` point to the current session `chatfile`
- auth state still comes from the runtime user's `CODEX_HOME`

### Session modes

`groupSessionMode` supports:

- `per-user`
  each group member gets an isolated session, replies still go to the same group
- `shared`
  the whole group shares one session

For external integrations, prefer storing and using `chatKey`. `sessionId` should be treated as a fallback identifier.

### Codex execution mode

`CODEX_EXEC_MODE` supports:

- `sandboxed`
  runs `codex exec --full-auto`
- `host`
  runs `codex exec --dangerously-bypass-approvals-and-sandbox`

Default: `host`.

`host` mode is intended for trusted internal environments.

## Bot Management

### Bootstrap bots on startup

The recommended model is bootstrap by environment variables or a JSON file so bots survive process or container restarts.

Single bot:

```bash
WECOM_BOT_NAME=default
WECOM_BOT_ID=YOUR_BOT_ID
WECOM_BOT_SECRET_FILE=/run/secrets/wecom_default_secret
WECOM_BOT_WORK_DIR=/home/jenkins
WECOM_BOT_GROUP_SESSION_MODE=per-user
```

Multiple bots:

```bash
WECOM_BOOTSTRAP_BOTS_JSON_FILE=/run/secrets/wecom_bots.json
```

Example:

```json
[
  {
    "id": "bot-a",
    "name": "bot-a",
    "botId": "BOT_A",
    "secretFile": "/run/secrets/bot_a.secret",
    "workDir": "/home/jenkins",
    "groupSessionMode": "per-user",
    "enabled": true
  }
]
```

### JSON APIs

Main endpoints:

- `GET /api/bots`
- `POST /api/bots`
- `POST /api/bots/{bot_id}/restart`
- `POST /api/bots/{bot_id}/stop`
- `DELETE /api/bots/{bot_id}`
- `GET /api/bots/{bot_id}/sessions/{chat_key}/chat`
- `POST /api/bots/{bot_id}/sessions/{chat_key}/interrupt`
- `POST /api/bots/{bot_id}/sessions/{chat_key}/reset`
- `POST /api/send-file`
- `GET /api/schedules`
- `POST /api/schedules`
- `GET /api/schedules/{schedule_id}`
- `POST /api/schedules/{schedule_id}/pause`
- `POST /api/schedules/{schedule_id}/resume`
- `DELETE /api/schedules/{schedule_id}`
- `POST /api/schedule-message`

Example:

```bash
curl -s -X POST http://127.0.0.1:9299/api/bots \
  -u 'bridge:change-me' \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "codex1",
    "botId": "YOUR_BOT_ID",
    "secretFile": "/run/secrets/wecom_bot_secret",
    "workDir": "/home/jenkins",
    "groupSessionMode": "per-user"
  }'
```

## Local Commands

### Send a file back to WeCom

```bash
python3 ./send_file.py \
  --chat-key "CURRENT_CHAT_KEY" \
  --bot-config-id "BOT_CONFIG_ID" \
  --file-path "/path/to/file"
```

For `workspace_bridge.service`, common send-file API outcomes are:

- `400` for invalid JSON or missing `chatKey` / `filePath`
- `403` for files outside `chatfile` or explicit `FILE_SEND_ROOTS`
- `404` for missing files
- `413` for files larger than `MAX_UPLOAD_SIZE`
- `503` when the bot is not connected
- `504` when file delivery times out
- `502` for upload or transport-layer failures

Note:

- The main `bridge.py` service uses a queue-backed `/api/send-file` flow and returns queued-style results.
- `workspace_bridge.service` uses an immediate upload/send flow for the same path and returns final delivery results such as `mediaId` and `sent ...`.

### Schedule a one-shot message

```bash
python3 ./schedule_message.py \
  --chat-key "CURRENT_CHAT_KEY" \
  --bot-config-id "BOT_CONFIG_ID" \
  --run-at "2026-05-09T15:00:00+08:00" \
  --message "Remind me to check the report at 3 PM"
```

### Create a recurring cron schedule

```bash
python3 ./schedule_message.py \
  --chat-key "CURRENT_CHAT_KEY" \
  --cron "0 9 * * *" \
  --timezone "Asia/Shanghai" \
  --message "Summarize yesterday's alerts every day at 9 AM"
```

Scheduling notes:

- recurring scheduling is cron-definition based
- scheduling precision is minute-level
- one-shot `runAt` and `delaySeconds` are normalized into the same minute-level model

## Built-in Session Control Commands

These commands are intercepted by the bridge and are not forwarded to Codex:

- `/bridge-status`
- `/bridge-interrupt`
- `/bridge-reset`

Meaning:

- `/bridge-status`
  show session state, queue size, scheduled count, `sessionId`, and `threadId`
- `/bridge-interrupt`
  stop the current run but keep thread and chat context
- `/bridge-reset`
  stop the current run and clear session context

The same controls are also exposed by API:

- `POST /api/bots/{bot_id}/sessions/{chat_key}/interrupt`
- `POST /api/bots/{bot_id}/sessions/{chat_key}/reset`

## Feature Guide

See [docs/FEATURES.md](docs/FEATURES.md) for a capability-oriented feature summary.

## Validation

Quick health check:

```bash
sh ./check_bridge_health.sh
```

Smoke tests:

```bash
sh ./smoke_bridge.sh
```

Full test suite:

```bash
sh ./test.sh
```
