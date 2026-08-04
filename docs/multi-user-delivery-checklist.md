# Multi-User Delivery Checklist

This checklist is the minimum evidence bar for treating the bridge as ready for multi-user team delivery.

## 1. Required Regression Gates

These test suites must pass together:

```bash
sh ./check_multi_user_readiness.sh
```

This command runs two explicit gates in order:

- `check_production_runtime.sh` covers the deployed `bridge.py` path.
- `check_compatibility_runtime.sh` covers the retained `workspace_bridge.service` library/API path.

Static checks are a separate required gate:

```bash
sh ./check_quality.sh
```

Optional local concurrency probe:

```bash
python3 ./check_multi_user_concurrency.py --chat-count 10 --message "readiness check" --mode mock --rounds 5
```

If you want to exercise the real Codex backend instead of the built-in mock mode:

```bash
python3 ./check_multi_user_concurrency.py --env-file .env --chat-count 10 --message "readiness check" --mode real --rounds 2
```

Minimum real-mode preconditions:

- `codex` is executable in `PATH`
- `WECOM_BOT_ID` is set
- `WECOM_BOT_SOURCE_DIR` exists
- `WECOM_BOT_SECRET_FILE` exists

Notes:

- the default `mock` mode is self-contained: it uses a temporary secret and runtime directory, does not connect to WeCom, and does not write into a configured production runtime
- the `real` mode performs preflight checks and fails fast with structured errors if required paths or settings are missing
- generated runtime directories should not be treated as delivery artifacts

Optional runtime hygiene probe:

```bash
python3 ./cleanup_recursive_runtime.py --runtime-root .runtime
```

Required isolated launcher/watchdog/resource probe:

```bash
python3 ./check_production_readiness.py --duration-sec 60 --concurrency 20
```

This starts the current worktree through the real `start.sh` and watchdog on a temporary port, verifies recovery after `SIGKILL`, applies HTTP latency/error thresholds, samples RSS/CPU/descendants/queue depth, and cleans up only processes carrying the temporary control-root identity.

At minimum, the regression evidence must cover:

- same-chat concurrency rejection
- different-chat concurrent progress
- idle recycle then message recovery
- session runtime cleanup after recycle/reset
- stale session runtime cleanup by retention policy
- orphan session runtime cleanup on startup
- session runtime bootstrap avoids copying heavy base CODEX_HOME state into every session
- disconnect event driven reconnection path
- stale thread fallback to fresh exec
- schedule execution and retry behavior

## 2. Manual Runtime Checks

Before calling the service ready for team use, verify all of the following against a running instance:

1. Start the service with `sh ./start.sh`.
2. Confirm public `/livez` returns `200`, then confirm authenticated `/healthz` returns `200` and `ok: true`.
3. Confirm the bot logs show `subscribed`.
4. Send two messages from different chats at nearly the same time and verify both progress.
5. Send two overlapping messages from the same chat and verify the second receives a busy/queued response.
6. Leave a chat idle for more than 30 minutes, then send a message and verify the session resumes with the same stable `sessionId`.
7. Force a WeCom disconnect and verify the runtime reconnects automatically.
8. Confirm the short reconnect window does not trigger unnecessary watchdog restarts.

## 3. Production Activation Evidence Still Required

The isolated probe covers launcher/watchdog behavior and HTTP resource evidence. True multi-user delivery still requires deployment-specific evidence:

- concurrent chat load test with at least 10 active chats
- soak run over an extended period
- resource observations during the real workload, in addition to the isolated probe
- watchdog recovery verification on the deployment host, in addition to the isolated probe

These checks require real credentials and a production-like host. They are an activation gate, not a claim the repository-only test suite can establish. Without their artifacts, the code may be released as a candidate, but the specific deployment must not be described as production-proven.
