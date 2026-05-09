---
name: wecom-session-control
description: >
  当前运行环境是企业微信智能机器人桥接模式。用户如果要求“停止当前任务”“中断这次执行”“清掉当前会话”“重置上下文”，应调用 bridge 提供的 session 管理 API，只操作当前 chat 对应的 session。
---

# 企业微信会话控制

## Description
当前运行环境是企业微信智能机器人桥接模式。你可以控制当前会话的运行状态，包括：
- 中断当前正在跑的任务，但保留上下文
- 重置当前会话，清空上下文

这类操作只影响当前 `chatKey` 对应的 session，不影响其他人的会话。

## Rules
- 只能操作当前 BridgeContext 里的 `chatKey` / `sessionId`
- 不要去中断别人的会话
- 优先使用当前 BridgeContext 里的 `sessionId`
- reset 会同时取消当前 chat 对应的本地文件回传请求；取消结果以当前会话为范围
- 如果当前 Codex 运行在 sandbox 中，不要直接访问 localhost API
- 只有在宿主机模式或外部控制器环境下，才使用 bridge API
- 只在用户明确表达“停止、取消、重置、清空当前会话”时使用
- 回复用户时只说结果，不暴露 URL、curl、JSON 或内部实现

## 调用方式

当前技能默认是“会话控制意图说明”技能，不强制在 sandbox 内直接调用 API。

如果当前运行环境允许直接访问 bridge API，例如：

- `CODEX_EXEC_MODE=host`
- 或者当前动作由外部控制器执行

才使用下面的 API 示例。

## API 示例

中断当前任务，但保留 thread 上下文：

```bash
python3 - <<'PY'
import json, urllib.parse, urllib.request
bots = json.loads(urllib.request.urlopen('http://127.0.0.1:9299/api/bots').read().decode())
chat_key = "single:userid"
bot = next((b for b in bots if any(s.get("key") == chat_key for s in b.get("sessions", []))), None)
if not bot:
    raise SystemExit("bot/session not found")
chat_key_encoded = urllib.parse.quote(chat_key, safe='')
url = f"http://127.0.0.1:9299/api/bots/{bot['id']}/sessions/{chat_key_encoded}/interrupt"
req = urllib.request.Request(url, data=b"", method="POST")
print(urllib.request.urlopen(req).read().decode())
PY
```

重置当前会话，清掉 thread 和聊天上下文：

```bash
python3 - <<'PY'
import json, urllib.parse, urllib.request
bots = json.loads(urllib.request.urlopen('http://127.0.0.1:9299/api/bots').read().decode())
chat_key = "single:userid"
bot = next((b for b in bots if any(s.get("key") == chat_key for s in b.get("sessions", []))), None)
if not bot:
    raise SystemExit("bot/session not found")
chat_key_encoded = urllib.parse.quote(chat_key, safe='')
url = f"http://127.0.0.1:9299/api/bots/{bot['id']}/sessions/{chat_key_encoded}/reset"
req = urllib.request.Request(url, data=b"", method="POST")
print(urllib.request.urlopen(req).read().decode())
PY
```

## 参数来源
- `chat_key`：来自当前 BridgeContext 的 `chatKey`
- `bot.id`：通过 `GET /api/bots` 找到持有当前会话的 bot

## 何时用 interrupt
- 用户说“停止当前任务”
- 用户说“不要继续跑了”
- 用户说“把这次卡住的停掉”

## 何时用 reset
- 用户说“清掉当前上下文”
- 用户说“从头开始”
- 用户说“重置当前会话”

## 回复规范
- interrupt 成功：
  - “当前任务已停止。”
- reset 成功：
  - “当前会话已重置，我们可以重新开始。”
