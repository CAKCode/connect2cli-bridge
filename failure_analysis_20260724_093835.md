# 失败用例分析报告

日志: 当前 pytest 完整套件输出
时间: 2026-07-24 09:38:35 UTC
总失败: 1 | BUG: 0 | ENV_ISSUE: 0 | CASE_ISSUE: 1

---

## CASE_ISSUE (1个)

### 1. test_check_bridge_health_uses_shared_runtime_schedule_dirs

- **is_bug**: NO
- **分类**: CASE_ISSUE
- **NodeID**: `tests/test_bridge_core.py::test_check_bridge_health_uses_shared_runtime_schedule_dirs`
- **错误**: `subprocess.CalledProcessError`: 健康检查脚本退出码为 1，测试桩报告 `unexpected url: http://127.0.0.1:19399/livez`
- **位置**: `tests/test_bridge_core.py:1835`
- **ChannelName**: -
- **AgentID**: -
- **TaskID**: -
- **操作步骤与关键参数**:
  1. 将健康检查脚本及运行时配置复制到临时目录。
  2. 在共享运行时目录创建 1 个 processing 和 2 个 pending 调度任务。
  3. 通过 `sitecustomize.py` 替换 `urllib.request.urlopen`，模拟健康检查和 bot 列表响应。
  4. 使用 `BRIDGE_BIND=127.0.0.1:19399` 执行 `check_bridge_health.sh`。
  5. 校验脚本输出包含 1 个 bot、1 个 processing 和 2 个 pending 任务。
- **不符合预期的点**: 脚本按当前约定请求 `/livez`，但该 case 的 HTTP 桩只接受旧的 `/healthz`，导致桩主动抛错。仓库中的专门测试明确要求健康检查脚本使用 `/livez`，因此应更新 case，而不是回退脚本行为。
- **Jira**: 不生成；该失败为测试桩过时，不是产品 bug。
