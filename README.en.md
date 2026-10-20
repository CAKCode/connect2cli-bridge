# WeCom Workspace Bridge（codex & claude code）

Pure Python bridge service between WeCom bots and the `codex` CLI.

This project is a headless backend. It does not provide a web UI. The root path `/` returns JSON status only.

Chinese docs:

- Main README: [README.md](README.md)
- Operations manual: [docs/使用手册.md](docs/使用手册.md)
- Minimal Codex deployment flow: [README.codex-deploy.md](README.codex-deploy.md)

## What It Does

- Maintains long-lived WeCom WebSocket connections
- Manages multiple bot configurations and persistence
- Starts `codex exec` per session and attempts `codex exec resume` only when a runtime thread is still available
- Supports built-in session control commands:
  `/bridge-status`, `/bridge-interrupt`, `/bridge-reset`, `/new`, `/bridge-resume`, `/local-resume`, `/local-detach`, `/cwd`
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
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
```

For tests:

```bash
python -m pip install -r requirements-dev.txt
```

### 3. Prepare `.env`

```bash
cp .env.example .env
```

Minimal example:

```bash
BRIDGE_BIND=127.0.0.1:9299
BRIDGE_PYTHON=/path/to/connect2cli-bridge/.venv/bin/python
WORK_DIR=/home/jenkins
BRIDGE_BASIC_AUTH=bridge:change-me
BRIDGE_RUNTIME_ROOT=/var/tmp/wecom-bridge-runtime
BRIDGE_CONTROL_ROOT=/var/tmp/wecom-bridge-control
CODEX_EXEC_MODE=host

WECOM_BOT_NAME=default
WECOM_BOT_ID=YOUR_BOT_ID
WECOM_BOT_SECRET_FILE=/run/secrets/wecom_default_secret
WECOM_BOT_WORK_DIR=/home/jenkins
WECOM_BOT_WORKSPACE_MODE=team
WECOM_BOT_GROUP_SESSION_MODE=per-user
WECOM_BOT_ENABLED=true
```

Notes:

- Use either `BRIDGE_BASIC_AUTH` or `BRIDGE_TOKEN`
- if neither is configured, `/healthz` and `/api/*` are limited to localhost
- `/livez` remains public for process liveness probes; detailed `/healthz` output follows the API authentication policy
- `BRIDGE_PYTHON` selects the interpreter that launches `bridge.py`; pin it to the project virtual environment
- plaintext bot `secret` is no longer supported; use `secretFile`
- `WECOM_BOT_WORK_DIR` is the bot's shared project root, not the exact Codex `cwd`
- `WECOM_BOT_WORKSPACE_MODE` supports `team` and `personal`
  - `team`: default for new `codex` bots; user chats use `workfile`, room-shared chats use `roomfile`
  - `personal`: default for new `claude` bots; all chats run directly in `workDir`
- If an env value contains spaces, such as `WECOM_BOT_AGENT_COMMAND=claude --model sonnet`, quote it in `.env`
- `BRIDGE_SHARED_RUNTIME_ROOT` stores shared coordination state, `BRIDGE_RUNTIME_ROOT` stores instance-local state, and `BRIDGE_CONTROL_ROOT` stores PID files, watchdog state, and logs
- runtime roots may be shared by multiple system users; put those users in one group and use a setgid group-writable root such as `2770` or `2775`
- state directories and files inherit the parent collaboration policy, so instances sharing one root do not need to use the same UID
- each instance sharing one source tree needs a distinct `BRIDGE_CONTROL_ROOT`
- set `BRIDGE_ENV_FILE=/path/to/instance.env` in the launcher environment to use an isolated config file instead of the repository `.env`; startup fails when an explicitly selected file is missing

### 4. Start the service

```bash
sh ./start.sh
```

`bridge.py` is the only production entry point started by this launcher. `workspace_bridge.service` remains a compatibility library/API path with its own regression gate.

Then inspect logs:

```bash
tail -f /var/tmp/wecom-bridge-control/bridge.log
```

Health check:

```bash
curl -s http://127.0.0.1:9299/livez
```

Run an isolated launcher, watchdog recovery, HTTP load, and resource gate against the current code:

```bash
python3 ./check_production_readiness.py --duration-sec 60 --concurrency 20
```

The probe uses a temporary port and isolated runtime roots without production bot credentials. It does not replace live WeCom subscription, reconnect, or multi-chat acceptance checks.

## Runtime Model

### Workspace layout

Important runtime paths:

- `workspace/<workspace-namespace>/users/<user>/workfile`
  long-lived per-user workspace
- `workspace/<workspace-namespace>/rooms/<room>/roomfile`
  shared room workspace
- `chatfile/<session-id>/`
  session-level file exchange area
- `~/.codex/skills/<skill>/SKILL.md`
  user-global skills
- `<workfile or roomfile>/.codex/skills/<skill>/SKILL.md`
  workspace-local skills

When the bridge starts an agent:

- `workspaceMode=team` uses `workfile` as the default `cwd`
- room-shared `workspaceMode=team` sessions use `roomfile`
- `workspaceMode=personal` runs every session directly in `workDir`
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

### Agent execution mode

`CODEX_EXEC_MODE` supports:

- `sandboxed`
  runs `codex exec --full-auto`
- `host`
  runs `codex exec --dangerously-bypass-approvals-and-sandbox`

Default: `host`.

Claude uses the same Bridge process identity and supplementary groups without `setpriv` or an isolated run-as
directory. In `host` mode, Bridge adds `--permission-mode bypassPermissions` to Claude so non-interactive runs
are not blocked by Claude CLI tool permission prompts. Legacy run-as fields are accepted but ignored.

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
    "workspaceMode": "team",
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
- `POST /api/bots/{bot_id}/sessions/{chat_key}/new`
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
    "workspaceMode": "team",
    "groupSessionMode": "per-user"
  }'
```

If the submitted `botId` already exists, `POST /api/bots` updates that bot in place and preserves any fields you omit from the request body.

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
- `workspace_bridge.service` uses an immediate upload/send flow for the same path and returns final delivery results including `sessionId`, `chatKey`, `mediaId`, and `sent ...`.

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
- `/new`
- `/bridge-resume`
- `/local-resume`
- `/local-resume <sessionId>`
- `/local-detach`
- `/cwd`

In group chats, mention the bot before the command, for example `@Bot /new` or `@Bot /bridge-interrupt`.
With `groupSessionMode=per-user`, commands affect the sender's session in that group; with `shared`, they affect the group's shared session. Confirmations are sent to the original group chat.

Meaning:

- `/bridge-status`
  show Bridge session state, queue size, scheduled count, `sessionId`, and `threadId`, followed by native Codex `/status` model, permissions, token, and context details
- `/bridge-interrupt`
  stop the current run but keep thread and chat context
- `/bridge-reset`
  wait for the current run to exit, then clear the thread, chat, queue, attachments, and Codex state in the current `sessionId`
- `/new`
  wait for the current run to exit, preserve the old session and its Codex state, then create and bind a new `sessionId`; select an older session later with `/bridge-resume`
- `/bridge-resume`
  list resumable sessions visible to the current chat; reply with the index or send `/bridge-resume <sessionId>`
- `/local-resume`
  list local Codex sessions under `$CODEX_HOME/sessions` whose latest rollout `cwd` matches the current directory; reply with an index to select one
- `/local-resume <sessionId>`
  bind the specified local session for the current directory and import its recent chat history
- `/local-detach`
  release the local-session binding and restore the previous Bridge thread and chat history, when available
- `/cwd`
  available only in `workspaceMode=personal`; `/cwd` shows the current directory, `/cwd <path>` switches to a subdirectory inside `workDir`, and switching is blocked while a run is active

Local-session commands require a bot with `workspaceMode=personal` and `agentBackend=codex`. Set the bot's
`localResumeOwnerUserId` to the authorized WeCom user ID. For single-bot bootstrap, use
`WECOM_BOT_LOCAL_RESUME_OWNER_USER_ID`; `LOCAL_RESUME_OWNER_USER_ID` is accepted as a fallback. Shared group
sessions cannot use this capability, and one local Codex session can be bound to only one valid WeCom session
at a time. The current session must have no running, queued, or pending-delivery work before switching context.

The same controls are also exposed by API:

- `POST /api/bots/{bot_id}/sessions/{chat_key}/interrupt`
- `POST /api/bots/{bot_id}/sessions/{chat_key}/new`
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

Static and runtime release gates:

```bash
sh ./check_quality.sh
sh ./check_production_runtime.sh
sh ./check_compatibility_runtime.sh
```

`check_multi_user_readiness.sh` runs both runtime gates in sequence. CI invokes the same commands.
