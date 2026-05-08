# WeCom Workspace Bridge

This project is a fresh bridge runtime centered on persistent workspaces instead of running Codex directly inside a shared source directory.

## Design goals

- Separate `sourceDir` from the actual Codex working directory.
- Reuse a persistent user workspace across `single:` and `group-user:` chats.
- Use a room workspace for `group:` chats.
- Keep skill resolution intentionally simple:
  - `global skills`
  - `workspace skills`
- Make the workspace and skill model testable before the bridge runtime is rebuilt on top of it.

## Current scope

The first implementation in this repository focuses on:

- workspace identity and path layout
- chat key to workspace mapping
- deterministic source keys
- workspace directory creation
- two-layer skill discovery and precedence
- minimal bot config loading
- minimal `aiohttp` prepare-session service

## Layout

Example runtime layout:

```text
.workspace-bridge-runtime/
  workspaces/
    users/
      alice/
        src_123456789abc/
          project/
          skills/
          state/
    rooms/
      room_1/
        src_123456789abc/
          project/
          skills/
          state/
  skills/
    global/
      deploy/
        SKILL.md
  chatfiles/
  locks/
```

## Skill model

Only two runtime layers are supported:

1. `global`
2. `workspace`

If both layers define the same skill name, the workspace skill wins.

This is intentional. Project-level skills can still be bootstrapped into a workspace later, but they are not a separate runtime precedence layer.

## Minimal service

The repository now includes a minimal `aiohttp` service shell with:

- `GET /`
- `POST /api/prepare-session`

`POST /api/prepare-session` provisions the workspace, resolves effective skills, builds the prompt, and returns the session/run metadata without yet connecting to WeCom.
