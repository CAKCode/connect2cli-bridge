---
name: wecom-file-reply
description: >
  当前运行环境是企业微信智能机器人桥接模式。用户通过企业微信与你对话，文本回复会自动发送到企微。 如果需要发送文件给用户，必须优先使用当前 BridgeContext 提供的本地发送命令。
---

# 企业微信文件发送

## Description
当前运行环境是企业微信智能机器人桥接模式。用户通过企业微信与你对话，文本回复会自动发送到企微。
如果需要发送文件给用户，必须优先使用当前 BridgeContext 提供的本地发送命令。
企业微信文件消息默认按 `20 MB` 上限处理；超限时先尝试压缩成 `zip`，压缩后仍超限再回退为下载链接。

## Rules
- 当用户要求发送文件、下载文件、获取文件时，必须请求 bridge 发文件
- 如果 BridgeContext 提供了 `localSendFileCommand`，必须直接使用它
- 只有当前 BridgeContext 没有本地发送命令时，才允许退回到 `sendFileEndpoint`
- 文件必须存在于本地文件系统中
- 支持的文件类型：mp4、avi、mov、pdf、doc、docx、xls、xlsx、csv、zip、tar、gz、png、jpg、jpeg、gif、html、log、md、xmind、txt、py、json、xml
- 一次最多发送 3 个文件
- 不要告诉用户“我无法发送文件”
- 如果 BridgeContext 明确提示本地网络受限，禁止探测 `127.0.0.1`，禁止自行写 socket/HTTP 探活代码
- 回复必须只保留用户能理解的自然语言结果，不能暴露接口地址、命令、JSON、tool 输出
- 发送前先用本 skill 自带的 helper 判断是否可直发，不要自行猜测大小限制
- 只要文件不在 `CHATFILE_DIR` 或 `EXPORT_DIR` 这类可发送目录下，就先复制或生成到可发送目录
- 文件超过企业微信默认上限时，先尝试生成 `zip`
- 如果 `zip` 仍超过上限，优先生成下载链接，不要继续尝试直发超限文件
- 如果当前环境未配置下载链接发布能力，要明确说明“需要先配置下载链接发布命令或公网基地址”，不要退回去修改 bridge

## Preferred Command

如果 BridgeContext 里有：

```text
localSendFileCommand: ...
```

直接执行这个命令模板，把其中的文件路径替换成真实绝对路径。

示例：

```bash
python3 /path/to/send_file.py --session-id "SESSION_ID" --file-path "/path/to/file"
```

## Smart Fallback Workflow

优先使用本 skill 附带的 helper：

```bash
python3 /home/jenkins/.codex/skills/wecom-file-reply/scripts/prepare_wecom_file.py \
  --export-dir "$EXPORT_DIR" \
  "/absolute/path/to/file"
```

helper 会输出 JSON，按以下规则处理每个文件：

1. `action = send`
   说明文件已经被放到可发送目录，且大小在上限内。
   这时再调用 `localSendFileCommand` 发送 `path` 字段对应的文件。

2. `action = link`
   说明文件或压缩包仍超过上限，helper 已经生成可发送给用户的下载链接。
   这时不要调用发文件命令，直接给用户回复链接。

3. `action = error`
   说明准备失败，例如文件不存在，或未配置下载链接发布能力。
   这时只向用户说明结果，不暴露内部命令和 JSON。

## Download Link Configuration

如果要支持“压缩后仍超限则发下载链接”，使用以下任一配置：

- `WECOM_FILE_PUBLISH_CMD`
  一个本地发布命令。helper 会把待发布文件路径作为最后一个参数传入。
  命令需要在标准输出打印最终下载链接。
  也支持在命令模板里使用 `{file}` 占位符。

- `WECOM_FILE_PUBLIC_BASE_URL`
  一个公网可访问的基础 URL。helper 会把文件放到 `EXPORT_DIR`，然后按文件名拼出下载链接。
  只有当 `EXPORT_DIR` 对外可访问时才能使用这个方式。

可选配置：

- `WECOM_FILE_MAX_MB`
  覆盖默认的 `20 MB` 上限。

- `WECOM_FILE_MAX_BYTES`
  如果设置，优先级高于 `WECOM_FILE_MAX_MB`。

本 skill 自带一个通用发布脚本：

```bash
python3 /home/jenkins/.codex/skills/wecom-file-reply/scripts/publish_download_link.py /path/to/file
```

推荐把它配置成：

```bash
export WECOM_FILE_PUBLISH_CMD='python3 /home/jenkins/.codex/skills/wecom-file-reply/scripts/publish_download_link.py {file}'
```

该脚本默认走 S3 兼容对象存储，支持两种返回方式：

- 如果设置了 `WECOM_FILE_PUBLIC_BASE_URL`，上传后返回公网直链
- 如果没设置 `WECOM_FILE_PUBLIC_BASE_URL`，返回预签名下载链接

发布脚本需要的环境变量：

- `WECOM_FILE_S3_BUCKET`
- `WECOM_FILE_S3_REGION`
  说明：部分兼容存储可留空
- `WECOM_FILE_S3_ENDPOINT_URL`
  说明：S3 兼容存储时通常需要，例如 MinIO / COS / OSS 的兼容端点
- `WECOM_FILE_S3_KEY_PREFIX`
  说明：可选，默认 `wecom`
- `WECOM_FILE_LINK_EXPIRES_IN`
  说明：预签名链接有效期，默认 `604800` 秒，也就是 7 天

脚本支持 `--dry-run`，可先验证链接格式，不实际上传。

## Personal Computer Fallback

如果是个人电脑、没有 CDN、也没有对象存储，本 skill 默认会优先启用“常驻 Cloudflare 分享”回退。

默认命令：

```bash
python3 /home/jenkins/.codex/skills/wecom-file-reply/scripts/share_via_cloudflared.py {file}
```

这个脚本会：

- 把目标文件复制到当前会话的可发送目录
- 在后台启动本地只读 HTTP 服务
- 在后台启动 `cloudflared` 临时隧道
- 复用已有常驻进程，避免链接刚发出去就失效
- 返回手机可访问的 `trycloudflare.com` 链接
- 对同一条分享链接设置默认有效期，过期后自动换新隧道

默认会从当前环境自动读取：

- `EXPORT_DIR`
- `WECOM_BRIDGE_EXPORT_DIR`
- `CHATFILE_DIR`
- `WECOM_BRIDGE_CHATFILE_DIR`

如果这些变量都不存在，再手动传 `--export-dir`。

默认有效期：

- `24h`

超过有效期后，脚本会自动关闭旧分享并生成新的 `trycloudflare.com` 链接。

可选环境变量：

- `WECOM_FILE_ENABLE_CLOUDFLARED_SHARE`
  说明：默认 `1`。设为 `0` / `false` / `no` 可关闭这条默认回退。

- `WECOM_FILE_CLOUDFLARED_SHARE_CMD`
  说明：覆盖默认的 Cloudflare 分享命令。

- `WECOM_FILE_CLOUDFLARED_MAX_AGE_SECONDS`
  说明：覆盖默认的 Cloudflare 分享有效期。默认 `86400` 秒，也就是 `24h`。

- `WECOM_FILE_ENABLE_TEMP_PUBLISH`
  说明：只有在 Cloudflare 分享被关闭或失败、且没有别的发布配置时，才回退到临时文件托管服务。

如果 Cloudflare 分享不可用，再回退到临时文件托管服务。

临时服务默认命令：

```bash
python3 /home/jenkins/.codex/skills/wecom-file-reply/scripts/publish_temp_link.py {file}
```

当前脚本默认有效期是 `24h`。

可选环境变量：

- `WECOM_FILE_TEMP_PUBLISH_CMD`
  说明：覆盖默认的临时分享命令。

如果你已经配置了 `WECOM_FILE_PUBLISH_CMD` 或 `WECOM_FILE_PUBLIC_BASE_URL`，优先走你自己的配置，不会走这些个人电脑回退。

## Proven Strategy

这次会话已经验证通过的优先级顺序：

1. 直发企微文件
2. 直发压缩包
3. 自定义对象存储 / 预签名链接
4. 常驻 Cloudflare 分享
5. 临时文件托管服务

其中第 4 步比第 5 步更适合手机和企业微信内置浏览器，因为它不依赖第三方临时文件站对 HTML 的处理策略。
同时第 4 步默认带 `24h` 有效期，避免链接长期暴露。

## Expected User-Facing Behavior

- 小文件：直接发送文件
- 超限但压缩后可发送：发送压缩包
- 压缩后仍超限：发送下载链接
- 未配置下载链接发布能力且压缩仍超限：说明当前缺少下载链接发布配置

## Fallback API

只有在当前 BridgeContext 没有 `localSendFileCommand`，但给了 `sendFileEndpoint` 时，才允许用 API。

优先级规则：

1. 优先使用当前 BridgeContext 的 `localSendFileCommand`
2. 次选当前 BridgeContext 的 `sessionId`
3. 再次选当前 BridgeContext 的 `chatKey`
4. 不要自行切换群聊/单聊

## Response Style

- 成功时只回复类似“文件已发送：sample.log”
- 发送压缩包时只回复类似“文件超过企微上限，已发送压缩包：sample.zip”
- 发送下载链接时只回复类似“文件超过企微上限，下载链接：<url>”
- 失败时只回复类似“文件发送失败，我重新试一下”
