# WeCom Workspace Bridge（codex & claude code）

企业微信智能机器人与 `codex` CLI 之间的纯 Python 会话桥接服务，默认对接 `codex`，也支持兼容 `claude` backend。

English overview: [README.en.md](README.en.md)

如果你要给其他人一个“只提供 `botId`、`secret`、GitHub 地址，Codex 就能帮部署”的最简流程，直接看：[README.codex-deploy.md](README.codex-deploy.md)

这个项目的主要定位不是面向公网的通用聊天机器人，而是面向组内协作的多人工作空间桥接层：

- 每个成员有各自独立的会话工作区和文件目录
- 同一个 Bot 下多人同时拉代码、改文件、跑命令时互不影响
- 通过企业微信作为入口，把 `codex` 变成组内可复用的协作开发工具

这是一个无前端的 headless 服务，负责：

- 维护企业微信 WebSocket 长连
- 管理多 Bot 配置与持久化
- 为会话启动可选 agent backend（`codex` / `claude`），并在当前运行态可恢复时尝试原生 resume
- 为组内多人协作提供隔离 workspace，保证每人文件和代码操作互不影响
- 提供全局共享 skill 与个人私有 skill 的分层注入和隔离
- 提供会话控制命令 `/bridge-status`、`/bridge-interrupt`、`/bridge-reset`、`/new`、`/bridge-resume`、`/local-resume`、`/local-detach`、`/cwd`
- 下载企微图片/文件到本地 workspace
- 对外暴露运行状态和最新摘要，便于前端轮询刷新
- 对超长运行会话支持企微 10 分钟后的分段回复与后续续传
- 通过本地命令或 API 回传文件到企微
- 支持一次性定时消息和 cron 周期调度
- 暴露 Bot / Session / Schedule JSON API

默认监听地址是 `http://127.0.0.1:9299`。根路径 `/` 只返回 JSON 状态，不提供网页 UI。

## 快速开始

### 1. 前置条件

- 已安装 `python3`
- 已安装并可直接执行 `codex`
- bridge 运行用户已经执行过 `codex login`
- 已拿到企业微信智能机器人的 `botId`
- 已准备好可读的 secret 文件

推荐先确认：

```bash
which python3
which codex
codex login status
echo "${CODEX_HOME:-$HOME/.codex}"
```

### 2. 安装依赖

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
```

如需跑测试：

```bash
python -m pip install -r requirements-dev.txt
```

### 3. 准备配置

先复制模板：

```bash
cp .env.example .env
```

一个最小可启动示例：

```bash
BRIDGE_BIND=127.0.0.1:9299
BRIDGE_PYTHON=/path/to/connect2cli-bridge/.venv/bin/python
WORK_DIR=/home/jenkins
BRIDGE_BASIC_AUTH=bridge:change-me
BRIDGE_SHARED_RUNTIME_ROOT=/srv/wecom-bridge-shared
BRIDGE_RUNTIME_ROOT=/var/tmp/wecom-bridge-runtime
BRIDGE_CONTROL_ROOT=/var/tmp/wecom-bridge-control
CODEX_EXEC_MODE=host
WECOM_BOT_AGENT_BACKEND=codex

WECOM_BOT_NAME=default
WECOM_BOT_ID=YOUR_BOT_ID
WECOM_BOT_SECRET_FILE=/run/secrets/wecom_default_secret
WECOM_BOT_WORK_DIR=/home/jenkins
WECOM_BOT_WORKSPACE_MODE=team
WECOM_BOT_GROUP_SESSION_MODE=per-user
WECOM_BOT_ENABLED=true
```

说明：

- `BRIDGE_BASIC_AUTH` 和 `BRIDGE_TOKEN` 二选一即可；如果都不配，`/healthz` 和 `/api/*` 只允许 localhost 访问
- `/livez` 只暴露进程存活状态，保持免鉴权以供 watchdog 使用；包含 Bot 明细的 `/healthz` 受上述鉴权规则保护
- `BRIDGE_PYTHON` 是拉起 `bridge.py` 的解释器；推荐固定到项目 `.venv/bin/python`
- `WECOM_BOT_SECRET_FILE` 必须指向 secret 文件；当前版本不再支持明文 `secret`
- 模板卡片原位更新严格走企业微信 `101031`：只在模板卡片点击回调当次即时更新
- 如果要延迟推进状态，应通过 `101032` 的 `response_url` 主动补发一条新消息或新卡，而不是继续原位更新旧卡
- `WECOM_BOT_WORK_DIR` 是 Bot 的共享项目根，不等于实际会话 `cwd`
- `WECOM_BOT_WORKSPACE_MODE` 可选 `team` 或 `personal`
  - `team`：默认给新建 `codex` bot；单聊 / 群内个人会话用 `workfile`，纯群共享会话用 `roomfile`
  - `personal`：默认给新建 `claude` bot；所有会话直接使用 `workDir`
- `BRIDGE_SHARED_RUNTIME_ROOT` 用于 Bot 锁、session 注册表、schedule、用户别名等共享协调状态；多实例部署时应指向共享且持久的目录
- `BRIDGE_RUNTIME_ROOT` 用于实例本地 workspace、chatfile、per-session `CODEX_HOME` 等高频 I/O 目录；建议放在本地快盘
- `BRIDGE_CONTROL_ROOT` 用于 PID、watchdog 状态和日志；目录必须可创建、可写，同一代码目录并行运行多个实例时必须分别配置
- 运行目录可以由多个系统用户共享；推荐让这些用户加入同一组，并把共享根设为 setgid 的 `2770` 或 `2775`
- Bridge 会继承父目录策略：组可写父目录下的新状态目录保持组协作，状态文件和锁文件允许同组读写；不要求所有实例使用同一 UID
- 启动隔离实例时，可在命令环境中设置 `BRIDGE_ENV_FILE=/path/to/instance.env`；默认仍读取仓库根 `.env`，显式指定的文件不存在时启动会失败
- `WECOM_BOT_AGENT_BACKEND` 默认是 `codex`；切到 `claude` 时，bridge 会改用 Claude CLI 的流式 JSON 输出协议

### 4. 启动服务

```bash
sh ./start.sh
```

说明：

- `start.sh` 默认会先启动仓库内置 watchdog，再由 watchdog 拉起 `bridge.py`
- `bridge.py` 是唯一生产服务入口；`workspace_bridge.service` 仅保留为兼容库 API 和独立回归路径，不由 `start.sh` 启动
- watchdog 会定期检查 Bridge 进程和 `GET /livez` 存活状态
- 连续失败达到阈值后会自动重启 Bridge，避免单次异常退出后服务一直挂着
- 如果你明确不需要这层保护，可在 `.env` 中设置 `BRIDGE_WATCHDOG_ENABLED=false`

启动成功后会输出：

- 进程 PID
- API 地址
- 当前鉴权模式
- `bridge.log` 的最后几行

查看日志：

```bash
tail -f /var/tmp/wecom-bridge-control/bridge.log
```

快速健康检查：

```bash
curl -s http://127.0.0.1:9299/livez
```

对当前代码执行隔离的 launcher、watchdog 恢复、HTTP 负载和资源门禁：

```bash
python3 ./check_production_readiness.py --duration-sec 60 --concurrency 20
```

探针使用临时端口和独立运行目录，不读取生产 Bot 凭据；它不能替代真实企微订阅、重连和多聊天验收。

## 运行模型

### Workspace 布局

当前实现将运行态分成两类目录：

- 共享协调状态，默认在 bridge 项目根目录下，或由 `BRIDGE_SHARED_RUNTIME_ROOT` 指定
  - `.bot-runtime-locks/`
  - `.session-registry/`
  - `.session-locks/`
  - `.scheduled-messages/`
  - `.user-aliases/`
- 实例本地运行态，默认也在 bridge 项目根目录下，或由 `BRIDGE_RUNTIME_ROOT` 指定
  - `workspace/<workspace-namespace>/users/<user>/workfile`
    用户级长期工作区
  - `workspace/<workspace-namespace>/rooms/<room>/roomfile`
    群共享工作区
  - `chatfile/<session-id>/`
    当前会话的文件交换区
  - `.bridge-codex-home/sessions/<session-id>/`
    当前会话隔离出来的 `CODEX_HOME`

- `~/.codex/skills/<skill>/SKILL.md`
  用户级全局 skills
- `<workfile 或 roomfile>/.codex/skills/<skill>/SKILL.md`
  当前 workspace 私有 skills

这套布局的核心目标是把 bridge 当作多人协作工作空间来用，而不是单用户临时会话：

- 单聊或 `groupSessionMode=per-user` 下，每个成员都有自己的 `workfile`
- 同一项目根下，多个人同时 `git pull`、改代码、生成文件时不会互相覆盖
- 会话级 `chatfile/<session-id>/` 负责当前轮次文件交换，用户长期文件放在 `workfile` 或 `roomfile`
- `CODEX_HOME` 也按 session 隔离，避免不同成员的会话状态、个人 skill、临时运行态互相污染

Bridge 运行 agent 时：

- `workspaceMode=team` 时，默认 `cwd` 是 `workfile`
- `workspaceMode=team` 且纯群共享会话时，会使用 `roomfile`
- `workspaceMode=personal` 时，所有会话直接使用 `workDir`
- `sandboxed` 与 `host` 模式都会沿用这套 `cwd` 选择规则，不会强制退回到 `workDir`
- `TMPDIR` / `TMP` / `TEMP` 会指向当前会话 `chatfile`
- 登录态继承 bridge 运行用户的 `CODEX_HOME`

如果 Bot 配置切到 `claude` backend：

- 默认执行命令是 `claude -p --verbose --permission-mode bypassPermissions --output-format stream-json`
- 有可用 `threadId` 时会尝试 `claude --resume <threadId>`
- 如果原生 resume 状态缺失，bridge 会回退到 fresh run，并把最近本地聊天历史拼进 prompt 做连续性兜底
- Claude 与 Codex 一样直接继承 Bridge 进程的系统用户、补充组和文件访问能力，不使用 `setpriv`、`chown` 或独立降权目录
- 默认 `CODEX_EXEC_MODE=host` 时，Claude 使用 `bypassPermissions`，不再弹出或卡在 Claude CLI 自身的工具授权检查
- Claude 的真实 `cwd` 由 `workspaceMode` 决定，`TMPDIR` 仍使用当前会话的 `chatfile`

用户目录命名说明：

- `chatKey` 里的用户部分可能仍然是企业微信原始 `USER_ID`
- 如果 `.user-aliases/<workspaceNamespace>/` 下存在映射，最终个人 `workfile` 会按 alias 后的用户名目录落盘
- 因此日志里的 `single:RAW_USER_ID` 与磁盘上的 `users/<alias>/workfile` 目录名可以不同，这是预期行为

### Skill 分层

Skill 目前固定分成两层：

- 用户全局层
  `~/.codex/skills/<skill>/SKILL.md`
- 个人/工作区私有层
  `<project>/.codex/skills/<skill>/SKILL.md`

- 工作区私有 skill 与用户全局同名时，优先使用私有层
- 每个 workspace 自己的 `.codex/skills` 彼此隔离，不会串到其他会话
- 群共享与单用户 workspace 都使用各自 `workfile/.codex/skills` 或 `roomfile/.codex/skills` 作为私有 skill 空间

### Session 模式

`groupSessionMode` 支持两种值：

- `per-user`
  群里不同成员 `@robot` 时，各自隔离会话；回复仍发回原群，且主动回传的 markdown 消息会自动 `@` 对应触发成员
- `shared`
  整个群共用一个会话

对外部调用方，建议优先保存并使用 `chatKey`；`sessionId` 只作为补充兜底。

### 前端状态与摘要

Bridge 自身不内置网页 UI，但会暴露足够的运行态和会话数据，方便外部前端或控制台持续刷新：

- Bot 连接状态
- 当前会话是否运行中
- 排队长度
- `sessionId` / 运行态 `threadId`
- 最新一次可见摘要或最终回复

适合做一个轻前端面板，轮询展示：

- 当前谁在跑
- 每个会话的最新摘要
- 运行状态是否仍在推进
- 是否已经进入超时后的分段回复

### Codex 执行模式

`CODEX_EXEC_MODE` 支持：

- `sandboxed`
  使用 `codex exec --full-auto`
- `host`
  使用 `codex exec --dangerously-bypass-approvals-and-sandbox`

默认值是 `host`。

`host` 模式适合可信内部环境，不适合暴露给不受控用户。

### Agent Backend 配置

每个 Bot 都可以独立选择执行后端：

- `agentBackend`
  当前支持 `codex`、`claude`
- `agentCommand`
  覆盖该 Bot 的实际可执行命令；不配时，`codex` 走 `CODEX_COMMAND`/`codex`，`claude` 走 `CLAUDE_COMMAND`/`claude`
- `workspaceMode`
  可选；`team` 或 `personal`。未显式配置时，新建 `codex` bot 默认 `team`，新建 `claude` bot 默认 `personal`；历史缺字段配置保持 `team`
- `workspaceNamespace`
  可选；用于把多个 Bot 收敛到同一套个人/群工作空间。相同 `workspaceNamespace` 的 Bot 会共享 `workfile` / `roomfile`

推荐做法：

- 默认继续用 `codex` backend，最贴合当前 bridge 的原生 resume 和 `CODEX_HOME` 模型
- 只有在你明确需要 Claude CLI 时再切换到 `claude`
- 历史配置中的 `agentRunAsUser`、`agentRunAsGroup`、`agentRuntimeRoot` 会继续被读取以兼容旧 Bot 配置，但执行时不再生效

## Bot 管理

### 启动时自动恢复 Bot

最推荐的方式是通过环境变量或 JSON 文件做 bootstrap，这样容器或进程重启后不需要手工重新建 Bot。

单 Bot：

```bash
WECOM_BOT_NAME=default
WECOM_BOT_ID=YOUR_BOT_ID
WECOM_BOT_SECRET_FILE=/run/secrets/wecom_default_secret
WECOM_BOT_WORK_DIR=/home/jenkins
WECOM_BOT_GROUP_SESSION_MODE=per-user
```

多 Bot：

```bash
WECOM_BOOTSTRAP_BOTS_JSON_FILE=/run/secrets/wecom_bots.json
```

`wecom_bots.json` 示例：

```json
[
  {
    "id": "bot-a",
    "name": "bot-a",
    "botId": "BOT_A",
    "secretFile": "/run/secrets/bot_a.secret",
    "workDir": "/home/jenkins",
    "workspaceMode": "personal",
    "workspaceNamespace": "personal-main",
    "groupSessionMode": "per-user",
    "agentBackend": "claude",
    "agentCommand": "/usr/local/bin/claude",
    "enabled": true
  }
]
```

### JSON API

如果配置了 `BRIDGE_BASIC_AUTH`：

```bash
curl -s http://127.0.0.1:9299/api/bots -u 'bridge:change-me'
```

如果配置了 `BRIDGE_TOKEN`：

```bash
curl -s http://127.0.0.1:9299/api/bots \
  -H 'Authorization: Bearer YOUR_BRIDGE_TOKEN'
```

主要接口：

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

新增 Bot 示例：

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
    "groupSessionMode": "per-user",
    "agentBackend": "codex"
  }'
```

说明：

- `secretFile` 必填
- 明文 `secret` 已不再支持
- `chat_key` 放到 URL 前需要先做 URL 编码
- 如果传入的 `botId` 已存在，`POST /api/bots` 会按现有配置 ID 更新；请求里未显式传入的字段会沿用旧配置
- Bot JSON/API 支持 `agentCommand`、`workspaceNamespace`、`workspaceMode`；旧的 `agentRunAsUser`、`agentRunAsGroup`、`agentRuntimeRoot` 字段仅做配置兼容，不改变执行身份

## 本地命令

### 回传文件

Bridge 内运行的 Codex 推荐直接调用本地命令，而不是本地 HTTP：

```bash
python3 ./send_file.py \
  --chat-key "CURRENT_CHAT_KEY" \
  --bot-config-id "BOT_CONFIG_ID" \
  --file-path "/path/to/file"
```

如果只有 `sessionId`，也可以改用 `--session-id`。

相关环境变量：

- `LOCAL_FILE_SEND_QUEUE_ROOT`
- `LOCAL_FILE_SEND_RESULT_TIMEOUT_MS`
- `LOCAL_FILE_SEND_RESULT_RETENTION_MS`
- `FILE_SEND_ROOTS`

### 创建一次性定时消息

```bash
python3 ./schedule_message.py \
  --chat-key "CURRENT_CHAT_KEY" \
  --bot-config-id "BOT_CONFIG_ID" \
  --run-at "2026-05-09T15:00:00+08:00" \
  --message "下午三点提醒我检查报告"
```

也支持：

- `--delay-seconds`
- `--session-id`

### 创建 cron 周期调度

```bash
python3 ./schedule_message.py \
  --chat-key "CURRENT_CHAT_KEY" \
  --cron "0 9 * * *" \
  --timezone "Asia/Shanghai" \
  --message "每天 9 点汇总昨日报警"
```

说明：

- 当前周期调度统一走 cron definition
- 调度精度是分钟级
- 一次性 `runAt` / `delaySeconds` 最终也会落到分钟级执行

## 会话控制命令

下面这些文本命令会被 bridge 拦截，不会继续转发给 `codex`：

- `/bridge-status`
- `/bridge-interrupt`
- `/bridge-reset`
- `/new`
- `/bridge-resume`
- `/local-resume`
- `/local-resume <sessionId>`
- `/local-detach`
- `/cwd`

群聊中先 @机器人再输入命令，例如 `@机器人 /new` 或 `@机器人 /bridge-interrupt`。
`groupSessionMode=per-user` 时只控制发送者在当前群的会话；`shared` 时控制当前群的共享会话。命令确认回复发送到原群聊。

语义：

- `/bridge-status`
  查看当前 Bridge 会话状态、排队数、定时任务数、`sessionId`、`threadId`，并附加 Codex 原生 `/status` 的模型、权限、token/context 等信息
- `/bridge-interrupt`
  中断当前任务，但保留当前 thread 和聊天上下文
- `/bridge-reset`
  等待当前任务退出，在当前 `sessionId` 内清除 thread、聊天记录、队列、附件上下文和该 session 的 Codex 状态；下一条消息绑定新 thread
- `/new`
  等待当前任务退出，保留旧 session 的 thread、聊天记录和 Codex 状态，创建并绑定新的 `sessionId`；旧会话仍可通过 `/bridge-resume` 选择
- `/bridge-resume`
  列出当前用户在当前运行态内可恢复的会话；回复编号可选择，或直接发送 `/bridge-resume <sessionId>`
- `/local-resume`
  列出 `$CODEX_HOME/sessions` 中最新 rollout 的 `cwd` 与当前目录一致的本地 Codex 会话；回复编号可选择
- `/local-resume <sessionId>`
  直接绑定指定的当前目录本地会话，并导入最近聊天记录
- `/local-detach`
  解除本地会话绑定，并恢复绑定前的 Bridge thread 和聊天记录（如有）
- `/cwd`
  仅 `workspaceMode=personal` 可用；`/cwd` 查看当前目录，`/cwd <path>` 切换到 `workDir` 内的子目录；任务运行中不允许切换

本地会话命令仅适用于 `workspaceMode=personal` 且 `agentBackend=codex` 的 Bot。需要将 Bot 的
`localResumeOwnerUserId` 配置为获准使用该能力的企业微信用户 ID；单 Bot 环境可通过
`WECOM_BOT_LOCAL_RESUME_OWNER_USER_ID` 配置，未设置时兼容读取 `LOCAL_RESUME_OWNER_USER_ID`。
shared 群会话不支持该能力，且同一个本地 Codex 会话同一时间只能绑定到一个有效的企微会话。
切换上下文前，当前会话必须没有运行中、排队中或待发送的任务。

## 长时运行回复策略

企业微信单次会话回包存在时效限制。对运行时间很长的任务，Bridge 会在原始响应窗口内仅在进展变化时更新；超过窗口后，会切换到主动消息续传模式。

这意味着：

- 运行早期走同一条回复流，只在正文或阶段变化时更新；等待时使用企微原生流式指示，不刷新运行秒数或动画点
- 超过较长时长后，会转为分段主动回复，而不是整段结果丢失
- 在群 `per-user` 会话里，主动续传消息会继续 `@` 原触发成员，避免消息混淆
- 前端或调用方可根据运行状态和最近摘要判断是否仍在继续输出

同样的控制能力也可通过 session API 完成：

- `POST /api/bots/{bot_id}/sessions/{chat_key}/interrupt`
- `POST /api/bots/{bot_id}/sessions/{chat_key}/new`
- `POST /api/bots/{bot_id}/sessions/{chat_key}/reset`

## 常用环境变量

- `BRIDGE_BIND`
  推荐的统一监听配置，格式如 `127.0.0.1:9299`
- `BRIDGE_HOST` / `BRIDGE_PORT`
  兼容拆分写法
- `BRIDGE_BASIC_AUTH`
  HTTP Basic 鉴权，格式 `user:password`
- `BRIDGE_TOKEN`
  Bearer token 鉴权
- `WORK_DIR`
  默认工作根目录
- `CODEX_EXEC_MODE`
  `sandboxed` 或 `host`
- `CODEX_COMMAND`
  全局默认 `codex` 可执行命令
- `CLAUDE_COMMAND`
  全局默认 `claude` 可执行命令
- `MAX_CONCURRENT_CODEX_RUNS`
  并发 `codex exec` 上限
- `SESSION_TTL`
  Session Codex Home 的空闲保留时长，单位为秒，默认 30 天，最短 30 分钟
- `WECOM_BOT_AGENT_BACKEND`
  单 Bot backend，默认 `codex`
- `WECOM_BOT_AGENT_COMMAND`
  单 Bot backend 命令覆盖
  如果值里包含空格，例如 `claude --model sonnet`，放进 `.env` 时请写成带引号的 shell 赋值
- `AGENT_BACKEND` / `AGENT_COMMAND`
  `WECOM_BOT_AGENT_*` 未配置时的兜底默认值
- `FILE_SEND_ROOTS`
  允许回传文件的额外目录白名单
- `LOCAL_FILE_SEND_QUEUE_ROOT`
  本地文件回传队列目录
- `WECOM_BOOTSTRAP_BOTS_JSON`
  直接通过环境变量注入多 Bot JSON
- `WECOM_BOOTSTRAP_BOTS_JSON_FILE`
  通过文件注入多 Bot JSON

完整变量列表见 [.env.example](.env.example)。

## 运维与验证

快速健康检查：

```bash
sh ./check_bridge_health.sh
```

### 进程保护

默认启用仓库内置 watchdog，无需额外部署 systemd/supervisor。

关键配置：

- `BRIDGE_WATCHDOG_ENABLED`
  说明：是否启用 watchdog，默认 `true`
- `BRIDGE_WATCHDOG_POLL_SEC`
  说明：watchdog 检查 Bridge 存活和健康状态的周期，默认 `5`
- `BRIDGE_WATCHDOG_HEALTH_TIMEOUT_SEC`
  说明：单次健康检查 HTTP 超时时间，默认 `5`
- `BRIDGE_WATCHDOG_STARTUP_GRACE_SEC`
  说明：Bridge 启动后的健康检查宽限期，默认 `20`
- `BRIDGE_WATCHDOG_FAIL_THRESHOLD`
  说明：连续失败多少次后触发重启，默认 `3`
- `BRIDGE_WATCHDOG_RESTART_BACKOFF_SEC`
  说明：重启前的退避等待时间，默认 `3`
- `BRIDGE_WATCHDOG_RESTART_WINDOW_SEC`
  说明：统计重启频率的时间窗口，默认 `300`
- `BRIDGE_WATCHDOG_MAX_RESTART_STREAK`
  说明：窗口内允许的连续重启上限，默认 `8`
- `BRIDGE_WATCHDOG_COOLDOWN_SEC`
  说明：超过重启上限后的冷却时间，默认 `60`

重启噪音检查：

```bash
sh ./check_restart_noise.sh
```

本地 smoke：

```bash
sh ./smoke_bridge.sh
```

模板卡片双人群聊 smoke：

```bash
sh ./smoke_template_card.sh "<chat-key>" "<bot-config-id>" "<bot-name>"
```

模板卡片 JSON 示例：

- [docs/template-card-examples/text_notice_basic.json](/home/jenkins/connect2cli-bridge/docs/template-card-examples/text_notice_basic.json)
- [docs/template-card-examples/button_progress_0_100.json](/home/jenkins/connect2cli-bridge/docs/template-card-examples/button_progress_0_100.json)
- [docs/template-card-examples/button_confirm_execute.json](/home/jenkins/connect2cli-bridge/docs/template-card-examples/button_confirm_execute.json)

基于 `101032 response_url` 延迟补发新卡：

```bash
python3 ./schedule_message.py \
  --reply-req-id "REQ_ID" \
  --delay-seconds "600" \
  --msgtype "template_card" \
  --template-card-file "/home/jenkins/connect2cli-bridge/docs/template-card-examples/button_progress_0_100.json"
```

说明：
- `REQ_ID` 来自之前那次消息/事件回调的 `req_id`
- 该方式会在 1 小时有效期内，通过企业微信 `101032` 的 `response_url` 补发一张新的状态卡
- 它不会修改旧卡，只会补发新卡

完整测试：

```bash
sh ./test.sh
```

提交前质量与 runtime 门禁：

```bash
sh ./check_quality.sh
sh ./check_production_runtime.sh
sh ./check_compatibility_runtime.sh
```

`check_multi_user_readiness.sh` 会依次执行后两套门禁。GitHub Actions 使用相同命令，避免 CI 与本地验收标准分叉。

## 相关文档

- [企业微信消息能力对齐](docs/wecom-protocol-alignment.md)：智能助理输入聚合、回复状态、限流、原生媒体和反馈查询。

- [README.en.md](README.en.md)
  英文版入口文档
- [使用手册](docs/使用手册.md)
  更完整的部署、API、排障说明
- [Feature Guide](docs/FEATURES.md)
  特性说明与能力边界
- [cron 周期调度设计](docs/cron-periodic-scheduler-design.md)
  当前周期调度实现与后续演进设计

## 说明

- 项目默认端口是 `9299`
- `.bots.json` 不再持久化明文 secret
- 如果旧版本 `.bots.json` 里还残留明文 `secret`，新版本会拒绝加载，需要先改成 `secretFile`

 意见反馈：chenzhak@sues.edu.cn
