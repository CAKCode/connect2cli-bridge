#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SECRET_FILE="/root/.codex/memories/mcp-atlassian.env"

if [[ -f "${SECRET_FILE}" ]]; then
  # shellcheck disable=SC1090
  source "${SECRET_FILE}"
fi

: "${AGORA_ATLASSIAN_PASSWORD:?Set AGORA_ATLASSIAN_PASSWORD in ${SECRET_FILE}.}"

export JIRA_URL="https://jira.agoralab.co"
export JIRA_USERNAME="chenzihang@agora.io"
export JIRA_API_TOKEN="${AGORA_ATLASSIAN_PASSWORD}"
export CONFLUENCE_URL="https://confluence.agoralab.co"
export CONFLUENCE_USERNAME="chenzihang@agora.io"
export CONFLUENCE_API_TOKEN="${AGORA_ATLASSIAN_PASSWORD}"
export AGORA_OAUTH_GRANT_TYPE="authorization_code"
export AGORA_OAUTH_BASE_URL="https://oauth.agoralab.co/oauth"
export AGORA_OAUTH_CLIENT_ID="QLKKe9NPZyrLualq8dUGZVYHu6bM6Wu1"
export AGORA_OAUTH_CLIENT_SECRET="NdNXlLvAmTFBMHAtzm8xeu890yUUJNQD"
export TOOLSETS="${TOOLSETS:-default}"

MCP_BIN="$(
  {
    find /root/.cache/uv/archive-v0 -path '*/bin/mcp-atlassian' 2>/dev/null
    find /tmp/uv-cache-mcp-atlassian/archive-v0 -path '*/bin/mcp-atlassian' 2>/dev/null
  } | sort | tail -n 1
)"

if [[ -n "${MCP_BIN}" && -x "${MCP_BIN}" ]]; then
  MCP_PY="$(dirname "${MCP_BIN}")/python"
  exec "${MCP_PY}" \
    "${SCRIPT_DIR}/mcp_atlassian_oauth_wrapper.py"
fi

export UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/uv-cache-mcp-atlassian}"
exec uvx git+https://github.com/LichKing-2234/mcp-atlassian
