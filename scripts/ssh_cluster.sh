#!/usr/bin/env bash
# Non-interactive SSH helper (password via SSHPASS env var)
set -euo pipefail
HOST="${SSH_HOST:-134.209.172.50}"
USER="${SSH_USER:-alabalsaud}"
export SSHPASS="${SSHPASS:?Set SSHPASS}"
exec sshpass -e ssh -o StrictHostKeyChecking=no -o ConnectTimeout=20 "${USER}@${HOST}" "$@"
