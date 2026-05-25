#!/bin/bash
set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="$PROJECT_ROOT/.venv/bin/activate"
AIRFLOW_HOME="$PROJECT_ROOT/reddit_pipeline"

# 1. Start Docker
echo "Starting Docker services..."
cd "$PROJECT_ROOT/reddit_pipeline"
docker compose up -d
cd "$PROJECT_ROOT"

# 2. Wait for Redis
echo "Waiting for Redis..."
until docker exec reddit_pipeline-redis-1 redis-cli ping 2>/dev/null | grep -q PONG; do
    sleep 1
done

# 3. Clear Redis seen sets
docker exec reddit_pipeline-redis-1 redis-cli DEL seen:posts seen:comments seen:users
echo "Redis cleared."

# 4. Launch tmux session with 2 panes (top/bottom)
tmux kill-session -t pipeline 2>/dev/null || true
tmux new-session -d -s pipeline

tmux split-window -v -t pipeline

# Airflow scheduler (top)
tmux send-keys -t pipeline:0.0 \
    "source $VENV && export AIRFLOW_HOME=$AIRFLOW_HOME && airflow scheduler" Enter

# Airflow api-server (bottom)
tmux send-keys -t pipeline:0.1 \
    "source $VENV && export AIRFLOW_HOME=$AIRFLOW_HOME && airflow api-server" Enter

tmux attach-session -t pipeline
