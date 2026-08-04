# Completion-Prep Audit

## Goal

Assess the current worktree against the objective:

> review整体代码，找出设计问题，并修改，然后再review，直到质量可以达到上线的标准

This document records the current completion audit evidence for both:

- the production `bridge.py` runtime path launched by `start.sh`
- the retained `workspace_bridge.service` compatibility library/API path

## Current High-Level Assessment

- The current `workspace_bridge` implementation now has strong execution, schedule, control, replay, file-send, and readiness proof in the current environment.
- The highest-risk quality gap that remained in the previous audit was closed: execution/service runtime proof is now whole-file green instead of only subset green.
- A real implementation issue was found and fixed during this audit cycle:
  - the runtime depended on default `asyncio.to_thread()` behavior that is unreliable in this environment and caused blocking test/runtime shutdown behavior
  - blocking bridge operations now use `workspace_bridge.async_utils.run_blocking()` instead
- After that fix, the current service path no longer shows the earlier “slow proof but unclear bug” pattern in the key `workspace_bridge` suites.
- The same blocking-offload fix was also applied to the production `bridge.py` path where equivalent instability existed.
- Production and compatibility evidence now use separate named runtime gates, while `check_multi_user_readiness.sh` runs both.
- API authorization is centralized and enforced by middleware for `/healthz` and `/api/*`; `/livez` remains a minimal public liveness endpoint.
- Deployment installs dependencies into the project `.venv`, and the launcher/watchdog share one explicit `BRIDGE_PYTHON` interpreter contract.
- GitHub Actions runs the same static, production-runtime, and compatibility-runtime gates available locally.

## Strong-Proof Areas

### Schedule definition layer

Evidence:

- `tests/test_schedule.py` -> `11 passed`

Covered behaviors:

- create / pause / resume / delete
- one-shot vs cron base semantics
- misfire and supported concurrency policy validation
- pause/delete cleanup of already materialized pending/processing work

### Schedule runtime / service

Evidence:

- `tests/test_schedule_runtime_and_service.py` -> `40 passed`
- combined run with schedule/reliability/execution/config/subscribe stack -> part of `167 passed`

Covered behaviors:

- due schedule execution
- deferred final delivery does not silently consume schedules
- scheduled job requeue / no duplicate rerun while deferred final is pending
- replay-driven finalize of deferred jobs
- stale schedule failure marker cleanup after success
- `skip_if_running` sees both processing and pending work
- pause/delete clears deferred cache
- pause/delete interrupts active schedule runs
- pause/delete can cancel active schedule tasks before a subprocess is fully visible
- helper-level due/job cancellation paths now treat externally initiated schedule-task cancellation as local cancel rather than loop-fatal cancel
- externally cancelled schedule runs are not requeued once the schedule has already been paused/deleted
- success/deferred finalize ordering windows are covered

### Replay / deferred finalize reliability

Evidence:

- `tests/test_wecom_runtime_reliability.py` -> `9 passed`
- combined run with schedule/runtime stack -> part of `167 passed`

Covered behaviors:

- cached payload replay
- partial tail preservation after send failure
- transient transport error clearing after recovery
- deferred scheduled job replay success:
  - pending job -> done
  - schedule definition advance
  - schedule-level done marker
  - stale failed marker cleanup
  - finalize ordering

### WeCom runtime status / control / interrupt core paths

Evidence:

- `tests/test_wecom_runtime_subscribe.py` -> `47 passed`
- focused control/resume/reset subset -> `7 passed`
- combined stack run -> part of `167 passed`

Representative covered behaviors:

- health/status split between WeCom state and local runtime state
- cached replay failure marks websocket error
- stable stream/session ids for control/error/busy replies
- reset lifecycle order and wait-for-exit semantics
- interrupt active task cancellation
- message failure reply preserves `message_failed`
- success clears stale `message_failed`
- interrupt suppression does not hide the next real failure

### Config / service / cleanup

Evidence:

- `tests/test_config_and_service.py` + `tests/test_file_send.py` -> `23 passed`
- combined stack run -> part of `167 passed`

Covered behaviors:

- config loading and path/secret handling
- cleanup terminal state
- schedule loop resilience
- schedule failure reflected in health
- invalid control/schedule requests do not produce unwanted side effects

### Execution / delivery core path

Evidence:

- `tests/test_execution_and_service_runtime.py` -> `43 passed`
- combined stack run -> part of `167 passed`

Covered behaviors:

- fresh exec vs resume exec
- resume-thread-not-found fallback
- stdout/output-file reply precedence
- final delivery success vs deferred-final-delivery rejection
- status stream + final stream ordering
- session lock release on success/failure
- cached payload fallback and replay semantics
- runtime thread-state update semantics
- runner invocation env/cwd formation

### Delivery readiness / multi-user isolation

Evidence:

- `tests/test_delivery_readiness.py` -> included in `167 passed`

Covered behaviors:

- per-chat thread isolation
- stable session reuse after idle/recycle
- runtime thread-state continuity after resumed activity

### Production `bridge.py` runtime path

Evidence:

- `tests/test_bridge_core.py` -> split full-file proof via four batches:
  - `70 passed`
  - `70 passed`
  - `70 passed`
  - `34 passed` + `33 passed`
- `tests/test_cleanup.py` + `tests/test_inbound_media.py` + `tests/test_port_check.py` + `tests/test_schedule_message_script.py` + `tests/test_send_file_script.py` -> `36 passed`
- `tests/test_multi_user_concurrency_script.py` + `tests/test_multi_user_soak_semantics.py` -> `7 passed`
- `python3 -m py_compile bridge.py`

Covered behaviors:

- session lifecycle / recycle / lease / registry semantics
- workspace bootstrap and cwd/codex-home setup
- upload queue / timeout / ambiguous-delivery handling
- schedule definition and scheduled-job handling
- control commands, queue resume, and cached-final-reply behavior
- production `run_codex` failure/fallback/status-stream semantics
- watchdog/startup/runtime-config shell integration

## Major Design Problems Already Resolved

This worktree has already addressed a long list of higher-risk design flaws, including:

- mixed status domains (`wecomStatus` vs local runtime failure state)
- control command side effects on read-only operations
- reset/interrupt lifecycle ordering bugs
- interrupt suppression relying only on fragile error-string matching
- replay partial-tail / transient transport error stickiness
- silent schedule consumption when final delivery was deferred
- scheduled-job duplicate rerun / missing finalize semantics
- `skip_if_running` only checking processing and not pending work
- fake `enqueue` capability exposure in `workspace_bridge`
- pause/delete only mutating definitions and not materialized work
- pause/delete not clearing deferred delivery cache
- pause/delete not interrupting currently running schedule work
- pause/delete not actually reaching the real scheduled-job execution body
- externally cancelled schedule runs being silently requeued after the user had already paused/deleted the schedule
- active schedule task/running state leaking across deferred/failure/cancel exits
- stale failed markers after later successful finalize
- stale success markers after later failed or externally cancelled finalize paths
- finalize ordering windows that allowed duplicate due detection
- cleanup not cancelling active schedule tasks

## Release Scope And Remaining Operational Evidence

- No deterministic code-release blocker is currently visible in either the `workspace_bridge` path or the default `bridge.py` path.
- Repository checks can establish release-candidate quality, but they cannot prove a specific production deployment.
- Production activation still requires artifacts from a real, production-like environment:
  - at least 10 concurrently active chats against the deployed service
  - an extended soak run
  - CPU, memory, subprocess-count, and queue-depth observations
  - watchdog restart/recovery evidence
  - live WeCom subscription, reconnect, same-chat busy/queue, and idle-resume checks
- Those checks require deployment credentials and infrastructure that are intentionally not exercised by the repository-only audit.

## Latest Evidence Update

The final re-review on 2026-08-03 found and closed additional release-facing issues:

- runtime roots now support multiple system users through a shared group and setgid `2770/2775` directories; nested state directories, state files, and locks inherit the parent collaboration policy instead of forcing one service UID and `0700/0600`
- Codex and Claude both inherit the Bridge process identity and supplementary groups; the legacy Claude `agentRunAsUser`, `agentRunAsGroup`, and `agentRuntimeRoot` fields are ignored without path validation
- Claude host-mode runs now include `--permission-mode bypassPermissions`, while custom commands that already set a permission mode are not duplicated
- deploy secrets no longer need to appear in shell history or process arguments; `--bot-secret-file` is the documented path, the managed secret file is private, and secret replacement is atomic
- health, restart-noise, and watchdog probes no longer put API credentials in Python process arguments
- the mock concurrency gate is credential-free, does not read the supplied production `.env`, uses disposable runtime/source/chatfile/output/file-send/agent-runtime roots, requires non-empty unique IDs, and proves overlapping subprocess execution
- API credential comparison uses constant-time comparison
- shutdown-reason state follows `BRIDGE_RUNTIME_ROOT`, so isolated instances no longer write this state into the repository root
- production and test dependencies reject unreviewed future major versions
- launcher PID files use private exclusive temporary files plus atomic replacement, reject non-regular/symlink targets, and no longer treat a directory target as a successful write
- watchdog fatal exits preserve their non-zero status, while signal exits retain conventional signal-derived statuses
- `start.sh` and watchdog verify PID ownership using both the repository script path and the normalized `BRIDGE_CONTROL_ROOT`, so stale PID files cannot terminate unrelated processes
- concurrent `start.sh` calls for one control root are serialized with a non-blocking directory `flock`; launched Bridge/watchdog processes explicitly close the lock fd so a completed start does not block later restarts
- pytest imports isolate all persistent roots before loading `bridge.py`; a representative queue/template-card run left the real runtime-directory aggregate SHA256 unchanged at `f8e5e2859eae8a7bf0f4e75483217724769fef9db4e66d374421297fdebcded4`

Verification completed on 2026-08-04:

- `sh ./check_quality.sh` -> passed compileall, Ruff `E9/F63/F7/F82`, and shell syntax checks
- `sh ./check_production_runtime.sh` -> `684 passed`
- `sh ./check_compatibility_runtime.sh` -> `338 passed`
- `sh ./test.sh` -> `1328 passed`
- `python3 ./check_multi_user_concurrency.py --chat-count 10 --message "readiness check" --mode mock --rounds 5` -> `5/5` rounds passed, with `maxConcurrentProcesses=10` in every round
- `python3 ./check_production_readiness.py --duration-sec 15 --concurrency 20` -> passed with 31,695 requests, zero failures, 2111.85 requests/sec, p95 12.035 ms, maximum RSS 71,500 KiB, zero descendants, zero queued files, and watchdog recovery in 1907.74 ms
- the production-style probe started through the real `start.sh` and watchdog, changed the Bridge PID after `SIGKILL`, proved the start lock was released, and reported `cleanup.ok=true` with no remaining owned processes
- `git diff --check` -> passed
- the production-style probe intentionally used an empty Bot configuration and no production credentials; it does not replace live WeCom subscription, reconnect, same-chat queue/busy, multi-chat, or idle-resume acceptance

## Completion Conclusion

Current conclusion for the reviewed repository runtime paths:

- the current quality bar is sufficient for a code release candidate
- no remaining deterministic blocker is visible in the exercised runtime, execution, schedule, replay, control, and file-send paths
- the worktree now has broad, current-environment evidence instead of only focused-subset evidence
- production activation remains conditional on the deployment-specific evidence listed above; this audit does not claim that live proof has already been collected
