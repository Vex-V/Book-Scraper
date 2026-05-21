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
#until docker exec reddit_pipeline-redis-1 redis-cli ping 2>/dev/null | grep -q PONG; do
 #   sleep 1
#done

# 3. Clear Redis seen sets
docker exec reddit_pipeline-redis-1 redis-cli DEL seen:posts seen:comments seen:users
echo "Redis cleared."

# 4. Launch tmux session with 4 panes (2 left, 2 right)
tmux kill-session -t pipeline 2>/dev/null || true

tmux new-session -d -s pipeline

# Split into left and right
tmux split-window -h -t pipeline:0

# Split left column (pane 0)
tmux split-window -v -t pipeline:0.0

# Split right column (pane 1)
tmux split-window -v -t pipeline:0.1

# Optional: normalize layout
tmux select-layout -t pipeline tiled

# Airflow scheduler (top-left)
tmux send-keys -t pipeline:0.0 \
    "source $VENV && export AIRFLOW_HOME=$AIRFLOW_HOME && airflow scheduler" Enter

# Airflow api-server (bottom-left)
tmux send-keys -t pipeline:0.2 \
    "source $VENV && export AIRFLOW_HOME=$AIRFLOW_HOME && airflow api-server" Enter

sleep 5
# MongoDB consumer (top-right)
tmux send-keys -t pipeline:0.1 \
    "source $VENV && python $PROJECT_ROOT/reddit_pipeline/consumers/mongo_consumer.py" Enter

sleep 5
# Elasticsearch consumer (bottom-right)
tmux send-keys -t pipeline:0.3 \
    "source $VENV && python $PROJECT_ROOT/reddit_pipeline/consumers/elasticsearch_consumer.py" Enter

# Attach to session
tmux attach-session -t pipeline