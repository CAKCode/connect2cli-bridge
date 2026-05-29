# Multi-User Delivery Checklist

This checklist is the minimum evidence bar for treating the bridge as ready for multi-user team delivery.

## 1. Required Regression Gates

These test suites must pass together:

```bash
sh ./check_multi_user_readiness.sh
```

Optional local concurrency probe:

```bash
python3 ./check_multi_user_concurrency.py --env-file .env --chat-count 10 --message "readiness check" --mode mock --rounds 5
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

- the default `mock` mode is intended for repeatable local validation without depending on a live Codex backend
- the `real` mode performs preflight checks and fails fast with structured errors if required paths or settings are missing
- prefer pointing `RUNTIME_ROOT` at a disposable path when running probes outside the test suite
- generated runtime directories should not be treated as delivery artifacts

Optional runtime hygiene probe:

```bash
python3 ./cleanup_recursive_runtime.py --runtime-root .runtime
```

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
2. Confirm `/healthz` returns `200` and `ok: true`.
3. Confirm the bot logs show `subscribed`.
4. Send two messages from different chats at nearly the same time and verify both progress.
5. Send two overlapping messages from the same chat and verify the second receives a busy/queued response.
6. Leave a chat idle for more than 30 minutes, then send a message and verify the session resumes with the same stable `sessionId`.
7. Force a WeCom disconnect and verify the runtime reconnects automatically.
8. Confirm the short reconnect window does not trigger unnecessary watchdog restarts.

## 3. Stress / Soak Evidence Still Required

The current repository now contains stronger correctness checks, but true multi-user delivery still requires system-level evidence:

- concurrent chat load test with at least 10 active chats
- soak run over an extended period
- resource observations for CPU, memory, subprocess count, and queue depth
- watchdog recovery verification under restart conditions

Without those artifacts, the code can be called improved and better verified, but not fully proven for multi-user delivery.
