#!/bin/bash

# 1. Run generic background system services if required
if [ -f /usr/local/bin/run_services.sh ]; then
    /bin/bash /usr/local/bin/run_services.sh &
fi

# 2. Start Jupyter Lab
jupyter lab --ServerApp.port=8889 --ServerApp.ip=0.0.0.0 --ServerApp.root_dir="/home/greendigit" --ServerApp.token=1qruM_WK4s2A --allow-root &

sleep 3

# 3. Detect Python runtime (favor scheduler venv or fallback to app venv/system python)
if [ -f "/home/greendigit/app/greendigit-l3-iot-uth-deployment/core/scheduler/venv/bin/python" ]; then
    SCHEDULER_PYTHON="/home/greendigit/app/greendigit-l3-iot-uth-deployment/core/scheduler/venv/bin/python"
elif [ -f "/home/greendigit/app/venv/bin/python" ]; then
    SCHEDULER_PYTHON="/home/greendigit/app/venv/bin/python"
else
    SCHEDULER_PYTHON=$(which python3)
fi

# Determine python runtime for site_adapter (using /home/greendigit/app/venv if present)
if [ -f "/home/greendigit/app/venv/bin/python" ]; then
    ADAPTER_PYTHON="/home/greendigit/app/venv/bin/python"
else
    ADAPTER_PYTHON="$SCHEDULER_PYTHON"
fi

# 4. Start Site Adapter & Token Updater in the background (using exec to avoid extra bash processes)
ADAPTER_DIR="/home/greendigit/app/greendigit-l3-iot-uth-deployment/core/site_adapter"

if [ -d "$ADAPTER_DIR" ]; then
    echo "Starting site_adapter services from $ADAPTER_DIR using $ADAPTER_PYTHON..."
    
    # Run Token Updater
    if [ -f "$ADAPTER_DIR/token_updater.py" ]; then
        (cd "$ADAPTER_DIR" && exec "$ADAPTER_PYTHON" -u token_updater.py >> "$ADAPTER_DIR/token_updater.log" 2>&1) &
    fi

    # Run Site Adapter
    if [ -f "$ADAPTER_DIR/site_adapter.py" ]; then
        (cd "$ADAPTER_DIR" && exec "$ADAPTER_PYTHON" -u site_adapter.py >> "$ADAPTER_DIR/site_adapter.log" 2>&1) &
    fi
fi

# 5. Start API Server in the foreground (PID 1 / stays alive)
BASE_DIR="/home/greendigit/app/greendigit-l3-iot-uth-deployment/core/scheduler"
LOG_FILE="/var/log/api_server.log"
touch "$LOG_FILE"

cd "$BASE_DIR"
echo "Starting API from $BASE_DIR using $SCHEDULER_PYTHON" >> "$LOG_FILE"

exec "$SCHEDULER_PYTHON" -u -m uvicorn api_server:app --host 0.0.0.0 --port 8000 >> "$LOG_FILE" 2>&1
