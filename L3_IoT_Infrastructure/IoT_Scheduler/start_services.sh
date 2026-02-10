#!/bin/bash

# 1. Start Jupyter
jupyter lab --ServerApp.port=8889 --ServerApp.ip=0.0.0.0 --ServerApp.root_dir="/home/ilbekas" --ServerApp.token=1qruM_WK4s2A --allow-root &

sleep 5

# 2. Use Absolute Paths with Quotes
BASE_DIR="/home/ilbekas/Workload Scheduler/scheduler"
# Use the specific venv inside the scheduler folder
VENV_PATH="$BASE_DIR/venv/bin/python"
LOG_FILE="/var/log/api_server.log"

cd "$BASE_DIR"

# 3. Execution with full path
echo "Starting API from $BASE_DIR" >> "$LOG_FILE"
exec "$VENV_PATH" -u -m uvicorn api_server:app --host 0.0.0.0 --port 8000 >> "$LOG_FILE" 2>&1
