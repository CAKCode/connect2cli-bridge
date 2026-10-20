# 企业微信消息能力对齐

依据 2026-10-14 核对的[官方更新日志](https://developer.work.weixin.qq.com/document/path/93221)、[长连接文档](https://developer.work.weixin.qq.com/document/path/101463)、[智能助理调用](https://developer.work.weixin.qq.com/document/path/101903)及[频道概述](https://developer.work.weixin.qq.com/document/path/101906)。更新日志的最新记录为 2026/10/12；接口页面更新时间与能力首次发布时间分别记录。

## 已对齐的行为

| 能力 | 行为 |
| --- | --- |
| 会话频率 | 同一 Bot、同一真实会话共用 30 条/分钟、1000 条/小时额度，群里不同成员不能各自获得一份额度；上传初始化另按 Bot 计算额度 |
| 流式显示 | 正文变化时才合并更新，重复摘要不发送；不刷新计时或动画点；单条按 UTF-8 20,480 字节上限处理 |
| 卡片和欢迎语 | 按接收时间计算 5 秒发送窗口；卡片先更新，再执行业务；接收循环保持运行以处理发送确认 |
| 语音输入 | 使用企业微信转写文本（当前为 `voice.content`，兼容 `recognition`、`transcript`、`text` 别名），复用文字入口 |
| 视频输入 | 下载并保留在当前会话的附件上下文中；入站视频和文件的大小限制与出站限制分别管理 |
| 引用消息 | 引用文字作为上下文，不能触发控制命令；可下载的引用附件加入本次上下文，失败时说明需重新发送 |
| 反馈 | 记录准确、不准确及取消反馈，保留负反馈原因；不向反馈事件发送普通消息；取消反馈替换旧 verdict |
| 原生媒体发送 | 文件、图片、语音和视频使用对应消息类型；媒体大小、格式、文件名和分片数在上传前检查 |
| 断线续传 | 成功确认的分片保存在私有状态文件；不确定是否成功的分片可幂等重传；超过 30 分钟的上传会话重新初始化 |
| 智能助理 | 按 `task_id/sub_id/total_sub_count` 聚合文本、图片和文件，收齐后处理一次；不同用户或会话的分片不能混合，5 分钟缺片时显式收尾 |
| 助理回复 | 过程、附件和中间分段标为 `waiting`，当前执行最后一条结果才标为 `finished`；主动追加消息携带 `task_id` |
| 频道兼容 | 识别 `compat_info.source=channel`，保留现有群通道转发，不启用开发中的原生长连接或停止群通道同步 |

原生频道长连接在当前官方文档中仍标为“开发中”。频道在客户端 5.0.12 灰度开放；频道的三种权限由企业微信侧控制，不能由本地代码自行授权。企业微信在线文档 MCP 和 CLI PPT 属于独立授权的扩展，不会因为此次消息协议更新自动发布文档或创建 PPT。

## 媒体限制

| 类型 | 大小上限 | 格式 |
| --- | --- | --- |
| `file` | 20 MB | 普通文件 |
| `image` | 10 MB | PNG、JPEG、GIF |
| `voice` | 2 MB | AMR |
| `video` | 10 MB | MP4 |

媒体至少 5 字节，文件名最多 256 个 UTF-8 字节；单片不超过 512 KB，最多 100 片。当前使用 400 KB 分片与有限并发。视频标题和描述分别按 64、512 个 UTF-8 字节处理。图片、文件、视频可回复给智能助理；助理场景不支持语音和模板卡片回复。

企业微信长连接入站语音只提供平台转写文本，当前协议不提供原始音频下载地址。因此转写结果（例如“哈喽”被平台识别为“哈哈”）只能原样交给业务会话，Bridge 无法在本地对原始语音重新识别；语音消息也仅支持单聊。

## 本地发送

默认发送普通文件，原生媒体通过 `--media-type` 明确选择：

```bash
python3 send_file.py --chat-key single:USER --bot-config-id BOT_CONFIG_ID \
  --file-path /absolute/path/screenshot.png --media-type image
```

文件 API 使用 `mediaType`、`title`、`description`；`/api/send-message` 可发送已上传的 `mediaId`，支持相同原生类型。

智能助理执行中，模型子进程获得本次 `assistantTaskId`，文件/消息发送命令默认通过 `WECOM_BRIDGE_ASSISTANT_TASK_ID` 关联该调用，并将追加消息声明为 `waiting`。标准执行的最后一条正文结果会声明 `finished`。高级调用方也可明确设置：

```bash
python3 send_message.py --chat-key group:ASSISTANT_CHAT --bot-config-id BOT_CONFIG_ID \
  --msgtype markdown --content '最终结果' \
  --assistant-task-id CALL_ID --reply-status finished
```

`finished` 后再到达的消息不会计入本次助理结果；只有确实没有后续消息时才提前声明。更长的独立后台业务应由调用方明确管理追加回复状态，不能把“启动成功”描述为“测试通过”。

## 查询

`GET /api/bots` 包含当前能力边界及 `callbackBusy`。`GET /api/bots/{bot_id}/feedback` 返回最新反馈统计和最多 100 条明细，继承原有 API 访问控制。反馈、上传状态使用私有文件保存；聊天仍只输出可读摘要，不将 YAML/JSON 元信息作为文本转发。
