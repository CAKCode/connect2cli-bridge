# Cron / 周期调度设计

状态：当前实现已统一为 cron 调度模型。

## 当前实现

当前 bridge 只有一种调度模型：cron definition。

它覆盖两类能力：

- 一次性任务
  说明：`runAt / delaySeconds` 会在内部转换成 `cron + maxRuns=1 + startAt/endAt`
- 周期任务
  说明：直接使用 cron 定义

当前实现特征：

- 调度精度为分钟级
- 一次性任务如果不是整分钟，会向上取整到下一分钟
- 周期任务定义持久化在 `.scheduled-messages/definitions/`
- 每次具体触发持久化在 `.scheduled-messages/pending|processing|done|failed/`
- planner 负责从 definition 生成一次具体触发
- dispatcher 负责把具体触发投递到 session

## 目录模型

### 1. 调度定义层

```text
.scheduled-messages/definitions/
.scheduled-messages/definition-locks/
```

一个 definition 代表“这条任务以后还会不会继续触发”。

示例：

```json
{
  "scheduleId": "sch_19f0...",
  "botId": "bot-config-id",
  "sessionId": "session-id-or-null",
  "chatKey": "group-user:CHAT_ID:USER_ID",
  "message": "每天 9 点检查昨天的告警并总结",
  "mode": "cron",
  "cron": "0 9 * * *",
  "timezone": "Asia/Shanghai",
  "startAt": 1776387600000,
  "endAt": null,
  "maxRuns": null,
  "runCount": 0,
  "enabled": true,
  "nextRunAt": 1776387600000,
  "lastPlannedAt": null,
  "lastTriggeredAt": null,
  "lastFinishedAt": null,
  "misfirePolicy": "fire_once_now",
  "concurrencyPolicy": "skip_if_running",
  "autoDeleteOnDone": false,
  "createdAt": 1776301200000,
  "updatedAt": 1776301200000
}
```

关键字段：

- `cron`
- `timezone`
- `startAt`
- `endAt`
- `maxRuns`
- `runCount`
- `enabled`
- `nextRunAt`
- `misfirePolicy`
- `concurrencyPolicy`
- `autoDeleteOnDone`

### 2. 触发实例层

```text
.scheduled-messages/pending/
.scheduled-messages/processing/
.scheduled-messages/done/
.scheduled-messages/failed/
```

一个实例文件代表“一次具体要投递的消息”。

示例：

```json
{
  "requestId": "fire_19f0...",
  "scheduleId": "sch_19f0...",
  "botId": "bot-config-id",
  "sessionId": "session-id-or-null",
  "chatKey": "group-user:CHAT_ID:USER_ID",
  "message": "每天 9 点检查昨天的告警并总结",
  "runAt": 1776387600000,
  "createdAt": 1776387600000,
  "enqueuedAt": null,
  "enqueuedByInstance": null
}
```

## 调度流程

### 1. 创建 definition

- 一次性 `POST /api/schedule-message`
  说明：内部转成 one-shot cron definition
- 周期 `POST /api/schedules`
  说明：直接创建 cron definition

### 2. planner 循环

`schedule_definition_loop()` 会：

- 扫描 `definitions/`
- 找到 `enabled=true` 且 `nextRunAt <= now` 的 definition
- 获取 definition lock
- 生成一条具体触发到 `pending/`
- 推进下一次 `nextRunAt`
- 更新 `runCount`、`lastPlannedAt`

### 3. dispatcher 循环

`scheduled_message_loop()` 会：

- 扫描 `pending/`
- 到期后移到 `processing/`
- 尝试投递到 session
- 成功时移到 `done/`
- 失败时留在 `processing/`，等待重试窗口

## 锁与并发

### definition 锁

每个 `scheduleId` 对应一把 definition lock。

用途：

- 防止多个实例同时推进同一条 definition
- 防止 API 管理动作和 planner 并发修改同一条 definition

### session 运行冲突

当前默认 `concurrencyPolicy` 是：

- `skip_if_running`

含义：

- 如果这个 schedule 已经有 pending/processing 任务
- 或当前 session 正在跑这个 schedule

则本次 planner 会跳过，不再重复入队。

## API

### 一次性定时任务

```text
POST /api/schedule-message
```

示例：

```json
{
  "sessionId": "SESSION_ID",
  "runAt": "2026-04-17T15:00:00+08:00",
  "message": "查看 183.131.178.189 的报告，并发给我"
}
```

### 周期任务

```text
GET /api/schedules
POST /api/schedules
GET /api/schedules/{schedule_id}
POST /api/schedules/{schedule_id}/pause
POST /api/schedules/{schedule_id}/resume
DELETE /api/schedules/{schedule_id}
```

创建周期任务示例：

```json
{
  "sessionId": "SESSION_ID",
  "mode": "cron",
  "cron": "0 9 * * *",
  "timezone": "Asia/Shanghai",
  "message": "每天 9 点汇总昨日报警",
  "misfirePolicy": "fire_once_now",
  "concurrencyPolicy": "skip_if_running"
}
```

### 当前语义

- `pause`
  说明：停止未来规划，并清掉还没开始执行的该 schedule 任务；默认不打断当前已经在跑的这一轮
- `resume`
  说明：重新计算 `nextRunAt`，恢复未来触发
- `delete`
  说明：删除 definition，清掉待执行项，并中断当前正在运行的该 schedule

## 本地命令

### 一次性调用

```bash
python3 schedule_message.py \
  --session-id SESSION_ID \
  --run-at 2026-04-17T15:00:00+08:00 \
  --message "稍后提醒我"
```

### cron 调用

```bash
python3 schedule_message.py \
  --session-id SESSION_ID \
  --cron "0 9 * * *" \
  --timezone Asia/Shanghai \
  --message "每天 9 点汇总昨日报警"
```

参数约束：

- `--run-at` / `--delay-seconds` 与 `--cron` 互斥
- `--timezone` 仅对 `--cron` 有意义

## 和会话控制命令的关系

- `/bridge-interrupt`
  说明：中断当前执行，但不删除 schedule definition
- `/bridge-reset`
  说明：清空当前 session 上下文；未开始执行的 scheduled 实例会回退到 pending

## 后续可扩展方向

如果后续需要更强能力，可以继续考虑：

- 更丰富的 misfire policy
- 更丰富的 concurrency policy
- `run-now`
- 更强的可观测性和管理视图
- 更复杂的 cron 语法支持
