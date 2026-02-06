#!/usr/bin/env bash
# run_streaming.sh — wrapper for cron to run indian_streaming_content.py
# inside the virtualenv with the required environment variables.

set -euo pipefail

PROJECT_DIR="/Users/arumugammani/Documents/GIT/python"
VENV_DIR="${PROJECT_DIR}/.venv"
LOG_FILE="${PROJECT_DIR}/cron_streaming.log"

cd "${PROJECT_DIR}"

# Load environment variables (RAPIDAPI_KEY, SENDER_EMAIL, etc.)
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

# Activate virtualenv and run
source "${VENV_DIR}/bin/activate"
python indian_streaming_content.py >> "${LOG_FILE}" 2>&1
