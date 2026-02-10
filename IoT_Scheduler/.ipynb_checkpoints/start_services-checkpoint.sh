#!/bin/bash
# 1. Start Jupyter Lab in the background
jupyter lab --ServerApp.port=8889 --ServerApp.ip=0.0.0.0 --ServerApp.root_dir=/home/ilbekas --ServerApp.token=1qruM_WK4s2A &

# 2. Navigate to your scheduler directory
cd "/home/ilbekas/Workload Scheduler/scheduler"

# 3. Start FastAPI and pipe logs to /var/log/api_server.log
# We use 2>&1 to capture both standard output and error messages
./venv/bin/python -u -m uvicorn api_server:app --host 0.0.0.0 --port 8000 2>&1 | tee -a /var/log/api_server.log
