#!/usr/bin/env bash
# ==============================================================================
# Project V2.5 - Off-Peak Staggered Pipeline Orchestrator
# Dynamically manages resource allocation to respect the 4GB RAM hardware budget.
# ==============================================================================

# Exit immediately if any command exits with a non-zero status (Fail-fast rule)
set -e

# --- 1. Dynamic Environment Resolution (DRY & No-Hardcoding Check) ---
# Why: Hardcoding absolute paths makes code fragile when moving between local dev and cloud VMs.
# We dynamically compute the project root directory relative to the script location.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
export PYTHONPATH="${PROJECT_ROOT}"

# --- 2. Load Centralized Application Settings ---
# Why: Cron executes in a clean environment lacking system environment variables.
# We source the gitignored .env file to pass configuration contexts to Spark and dbt.
if [ -f "${PROJECT_ROOT}/.env" ]; then
    export $(grep -v '^#' "${PROJECT_ROOT}/.env" | xargs)
else
    echo "ERROR: Persistent configuration file (.env) missing at ${PROJECT_ROOT}" >&2
    exit 1
fi

echo "[$(date +'%Y-%m-%d %H:%M:%S')] Starting off-peak staggered orchestration routine..."

# --- 3. Graceful Termination of PySpark Streaming Core ---
# Why: A blunt 'kill -9' (SIGKILL) abruptly cuts off active JVM processes, causing 
# Spark to miss writing the final micro-batch metadata logs to GCS. This leads to 
# data reprocessing or out-of-sync offsets. We send SIGTERM (kill -15) instead.
STREAM_JOB_PATH="${PROJECT_ROOT}/src/stream_processor.py"
STREAM_PID=$(pgrep -f "${STREAM_JOB_PATH}" || true)

if [ -n "${STREAM_PID}" ]; then
    echo "[INFO] Active stream processor detected (PID: ${STREAM_PID}). Sending SIGTERM..."
    kill -15 "${STREAM_PID}"
    
    # Active polling loop to verify clean teardown exit state
    timeout=60
    while kill -0 "${STREAM_PID}" 2>/dev/null && [ ${timeout} -gt 0 ]; do
        sleep 1
        ((timeout--))
    done
    
    if kill -0 "${STREAM_PID}" 2>/dev/null; then
        echo "[WARNING] Processor failed to exit within window. Enforcing SIGKILL..."
        kill -9 "${STREAM_PID}"
    fi
    echo "[INFO] Streaming thread terminated cleanly. Compute memory is now fully released."
else
    echo "[INFO] Background streaming infrastructure is idle. No active process to stop."
fi

# --- 4. Trigger Staggered dbt Gold Matrix Transformations ---
# Why: With PySpark Streaming paused, the entire 4GB RAM budget is dedicated to dbt.
# Running dbt-spark with the 'session' profile spins up a temporary in-memory engine 
# that auto-destructs upon completion, preventing idle resource leakage.
echo "[$(date +'%Y-%m-%d %H:%M:%S')] Dispatching dbt Gold matrix models..."
cd "${PROJECT_ROOT}/dbt_project"
dbt run --profiles-dir . --target dev

# --- 5. Reactivate PySpark Streaming Thread ---
# Why: Maintenance and transformations are complete. We restart the real-time pipeline.
# 'nohup' prevents the streaming thread from dying when the active cron session ends.
echo "[$(date +'%Y-%m-%d %H:%M:%S')] Reactivating real-time streaming pipeline..."
mkdir -p "${PROJECT_ROOT}/logs"
nohup python3 "${STREAM_JOB_PATH}" >> "${PROJECT_ROOT}/logs/stream.log" 2>&1 &

echo "[$(date +'%Y-%m-%d %H:%M:%S')] Staggered orchestration cycle completed successfully."