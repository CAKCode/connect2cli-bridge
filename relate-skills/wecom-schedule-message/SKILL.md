---
name: wecom-schedule-message
description: >
  当前运行环境是企业微信智能机器人桥接模式。用户如果要求“稍后提醒我”“下午三点帮我看报告并发我”“一小时后提醒我再跑一次”“10分钟后补发一张新的状态卡”，应使用 bridge 提供的本地定时消息命令，为当前 session 或当前回调 reqId 安排一条未来消息。
---

# 企业微信定时消息

## Description
当前运行环境是企业微信智能机器人桥接模式。你可以为当前会话安排一条未来消息，到指定时间后由 bridge 自动把这条消息重新投递回当前 chat。

这适用于：
- “下午三点帮我查看 183.131.178.189 的报告，并发给我”
- “一小时后提醒我再检查一次”
- “明天上午九点再帮我拉最新代码”
- “10分钟后再补发一张 100/100 的进度卡”

## Rules
- 优先使用当前 BridgeContext 里的 `sessionId`
- 如果是普通未来消息，优先用 `sessionId`
- 如果是 `101032` 主动补发新卡，使用当前回调里的 `replyReqId`
- 必须使用本地命令 `schedule_message.py`
- 不要直接访问 localhost HTTP 接口
- 定时消息的内容要写成未来时刻真正要执行的用户请求
- 回复用户时只说结果，不暴露内部命令或 API

## 定时消息命令

```bash
python3 /home/jenkins/wecom-bridge/wecom-workspace-bridge-py/schedule_message.py \
  --session-id "SESSION_ID" \
  --run-at "2026-04-17T15:00:00+08:00" \
  --message "查看183.131.178.189的报告，并发给我"
```

也可以使用相对延时：

```bash
python3 /home/jenkins/wecom-bridge/wecom-workspace-bridge-py/schedule_message.py \
  --session-id "SESSION_ID" \
  --delay-seconds "3600" \
  --message "查看183.131.178.189的报告，并发给我"
```

如果要基于 `101032 response_url` 在稍后补发一张新的模板卡片：

```bash
python3 /home/jenkins/connect2cli-bridge/schedule_message.py \
  --reply-req-id "REQ_ID" \
  --delay-seconds "600" \
  --msgtype "template_card" \
  --template-card-file "/home/jenkins/connect2cli-bridge/docs/template-card-examples/button_progress_0_100.json"
```

说明：
- `replyReqId` 来自之前那次回调的 `req_id`
- 该路径对应企业微信 `101032` 的 `response_url`
- 它会在 1 小时有效期内补发一张新的状态卡，而不是修改旧卡

## 时间处理
- 如果用户给的是绝对时间，先换算成带时区的 ISO 8601 时间
- 如果用户给的是相对时间，优先换算成 `--delay-seconds`
- 如需计算时间，可使用 `date`

示例：

```bash
TZ=Asia/Shanghai date -d 'today 15:00' --iso-8601=seconds
```

## 回复规范
- 成功后对用户说：
  - “好的，我会在下午三点帮你查看 183.131.178.189 的报告，并发给你。”
  - “好的，我会在 10 分钟后补发一张新的状态卡。”
- 不要展示命令、JSON、路径、API 地址
