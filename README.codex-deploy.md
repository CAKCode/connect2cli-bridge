# Codex 最简部署说明

这份说明的目标只有一个：

- 后续其他人只要在 Codex 里提供 `botId`、`secret`、GitHub 地址
- Codex 就可以按固定流程把这套 bridge 部署起来

## 最小输入

给 Codex 的最小信息只有 3 个：

```text
botId: YOUR_BOT_ID
secret: YOUR_SECRET
github: git@github.com:your-org/your-repo.git
```

说明：

- `botId`
  企业微信智能机器人的 `botId`
- `secret`
  企业微信智能机器人的 `secret`
- `github`
  需要让 Codex 后续工作的代码仓库地址，不是本 bridge 仓库地址

## 给 Codex 的固定指令

以后可以直接把下面这段发给 Codex：

```text
请在 wecom-workspace-bridge-py 仓库根目录执行部署脚本：

sh ./deploy_from_codex.sh \
  --bot-id "YOUR_BOT_ID" \
  --bot-secret "YOUR_SECRET" \
  --github-repo "git@github.com:your-org/your-repo.git"
```

如果你只想先看会做什么，不真正执行：

```text
sh ./deploy_from_codex.sh \
  --bot-id "YOUR_BOT_ID" \
  --bot-secret "YOUR_SECRET" \
  --github-repo "git@github.com:your-org/your-repo.git" \
  --dry-run
```

## 脚本会做什么

`deploy_from_codex.sh` 会自动完成这些动作：

1. 检查 `git`、`python3` 是否存在
2. 把目标 GitHub 仓库 clone 到：
   `./bot-workdirs/<repo-name>`
3. 安装 Python 依赖：
   `python3 -m pip install -r requirements.txt`
4. 把 secret 写到：
   `./.secrets/<bot-name>.secret`
5. 生成最小可用 `.env`，默认 `CODEX_EXEC_MODE=host`
6. 执行：
   `sh ./start.sh`

## 生成的关键配置

脚本会把 `.env` 写成最小可运行版本，核心变量包括：

```text
BRIDGE_BIND=127.0.0.1:9299
WORK_DIR=<克隆下来的目标仓库目录>
CODEX_EXEC_MODE=host

WECOM_BOT_CONFIG_ID=<bot-name>
WECOM_BOT_NAME=<bot-name>
WECOM_BOT_ID=<botId>
WECOM_BOT_SECRET_FILE=./.secrets/<bot-name>.secret
WECOM_BOT_WORK_DIR=<克隆下来的目标仓库目录>
WECOM_BOT_GROUP_SESSION_MODE=per-user
WECOM_BOT_ENABLED=true
```

默认行为：

- `bot-name` 默认取 GitHub 仓库名
- 监听地址默认是 `127.0.0.1:9299`
- 会覆盖当前 `.env`，但如果原来已有 `.env`，会先备份成 `.env.backup.<timestamp>`

## 部署后的检查

部署完成后，建议按顺序检查：

```bash
codex login status
sh ./check_bridge_health.sh
tail -f ./bridge.log
```

如果健康检查正常，再去企业微信里 `@bot` 发送消息。

## 常用附加参数

如果不是默认值，可以额外传：

```bash
--bot-name custom-bot
--bridge-bind 127.0.0.1:9399
--work-root /srv/wecom-bot-workdirs
--skip-install
--skip-start
```

## 注意事项

- 运行这个脚本前，当前 Linux 用户必须已经执行过 `codex login`
- 如果 GitHub 仓库是私有仓库，当前机器必须已经具备对应的 Git 拉取权限
- 这份脚本面向“单 Bot + 单代码仓库”的快速部署
- 如果后续要做多 Bot、共享状态目录、反向代理、鉴权、容器化，请回到主 README 和使用手册配置
