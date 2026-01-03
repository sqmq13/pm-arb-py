#!/usr/bin/env bash
set -euo pipefail

SESSION="scan"
if tmux has-session -t "$SESSION" 2>/dev/null; then
  tmux kill-session -t "$SESSION"
fi

LOG_PATH="data/logs/$(date +%F)/events.ndjson"
tmux new-session -d -s "$SESSION" "uv run python -m pm_arb.tools.base_scan"
echo "log: $LOG_PATH"
echo "tmux attach -t $SESSION"
