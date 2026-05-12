# WeCom Codex Bridge Python

企业微信智能机器人与 `codex` CLI 之间的纯 Python 会话桥接服务。

English overview: [README.en.md](README.en.md)

这是一个无前端的 headless 服务，负责：

- 维护企业微信 WebSocket 长连
- 管理多 Bot 配置与持久化
- 为会话启动 `codex exec` / `codex exec resume`
- 提供会话控制命令 `/bridge-status`、`/bridge-interrupt`、`/bridge-reset`
- 下载企微图片/文件到本地 workspace
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
pip install -r requirements.txt
```

如需跑测试：

```bash
pip install -r requirements-dev.txt
```

### 3. 准备配置

先复制模板：

```bash
cp .env.example .env
```

一个最小可启动示例：

```bash
BRIDGE_BIND=127.0.0.1:9299
WORK_DIR=/home/jenkins
BRIDGE_BASIC_AUTH=bridge:change-me
BRIDGE_SHARED_RUNTIME_ROOT=/srv/wecom-bridge-shared
BRIDGE_RUNTIME_ROOT=/var/tmp/wecom-bridge-runtime
CODEX_EXEC_MODE=sandboxed

WECOM_BOT_NAME=default
WECOM_BOT_ID=YOUR_BOT_ID
WECOM_BOT_SECRET_FILE=/run/secrets/wecom_default_secret
WECOM_BOT_WORK_DIR=/home/jenkins
WECOM_BOT_GROUP_SESSION_MODE=per-user
WECOM_BOT_ENABLED=true
```

说明：

- `BRIDGE_BASIC_AUTH` 和 `BRIDGE_TOKEN` 二选一即可；如果都不配，API 只允许 localhost 访问
- `WECOM_BOT_SECRET_FILE` 必须指向 secret 文件；当前版本不再支持明文 `secret`
- `WECOM_BOT_WORK_DIR` 是 Bot 的共享项目根，不等于实际会话 `cwd`
- `BRIDGE_SHARED_RUNTIME_ROOT` 用于 Bot 锁、session 注册表、schedule、用户别名等共享协调状态；多实例部署时应指向共享且持久的目录
- `BRIDGE_RUNTIME_ROOT` 用于实例本地 workspace、chatfile、per-session `CODEX_HOME` 等高频 I/O 目录；建议放在本地快盘

### 4. 启动服务

```bash
sh ./start.sh
```

启动成功后会输出：

- 进程 PID
- API 地址
- 当前鉴权模式
- `bridge.log` 的最后几行

查看日志：

```bash
tail -f ./bridge.log
```

快速健康检查：

```bash
curl -s http://127.0.0.1:9299/
```

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
  - `workspace/<bot>/users/<user>/workfile`
    用户级长期工作区
  - `workspace/<bot>/rooms/<room>/roomfile`
    群共享工作区
  - `workspace/<bot>/sessions/<chat-key>/chatfile`
    当前会话的文件交换区
  - `.bridge-codex-home/sessions/<session-id>/`
    当前会话隔离出来的 `CODEX_HOME`

- `relate-skills/<skill>/SKILL.md`
  项目级共享 skills
- `<workfile 或 roomfile>/.codex/skills/<skill>/SKILL.md`
  当前 workspace 私有 skills

Bridge 运行 `codex` 时：

- 默认 `cwd` 是 `workfile`
- 纯群共享会话会使用 `roomfile`
- `TMPDIR` / `TMP` / `TEMP` 会指向当前会话 `chatfile`
- 登录态继承 bridge 运行用户的 `CODEX_HOME`

### Session 模式

`groupSessionMode` 支持两种值：

- `per-user`
  群里不同成员 `@robot` 时，各自隔离会话；回复仍发回原群
- `shared`
  整个群共用一个会话

对外部调用方，建议优先保存并使用 `chatKey`；`sessionId` 只作为补充兜底。

### Codex 执行模式

`CODEX_EXEC_MODE` 支持：

- `sandboxed`
  使用 `codex exec --full-auto`
- `host`
  使用 `codex exec --dangerously-bypass-approvals-and-sandbox`

`host` 模式适合可信内部环境，不适合暴露给不受控用户。

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
    "groupSessionMode": "per-user",
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
    "groupSessionMode": "per-user"
  }'
```

说明：

- `secretFile` 必填
- 明文 `secret` 已不再支持
- `chat_key` 放到 URL 前需要先做 URL 编码

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

语义：

- `/bridge-status`
  查看当前会话状态、排队数、定时任务数、`sessionId`、`threadId`
- `/bridge-interrupt`
  中断当前任务，但保留当前 thread 和聊天上下文
- `/bridge-reset`
  中断当前任务，并清空当前会话上下文

同样的控制能力也可通过 session API 完成：

- `POST /api/bots/{bot_id}/sessions/{chat_key}/interrupt`
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
- `MAX_CONCURRENT_CODEX_RUNS`
  并发 `codex exec` 上限
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

重启噪音检查：

```bash
sh ./check_restart_noise.sh
```

本地 smoke：

```bash
sh ./smoke_bridge.sh
```

完整测试：

```bash
sh ./test.sh
```

## 相关文档

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
