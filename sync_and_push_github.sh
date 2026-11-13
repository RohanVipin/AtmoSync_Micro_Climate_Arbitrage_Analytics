#!/usr/bin/env bash
# =====================================================================
#           AtmoSync Telemetry: GitHub Auto-Sync & Push (WSL / Linux)
#           Target Branch: feature/Darshan
# =====================================================================

set -e

ROOT_DIR="/mnt/d/Infotact Internship"
REPO_DIR="$ROOT_DIR/AtmoSync_Micro_Climate_Arbitrage_Analytics"
DBT_DIR="$ROOT_DIR/dbt_atmosync"

echo "====================================================================="
echo "          AtmoSync Telemetry: GitHub Auto-Sync & Push"
echo "          Target Branch: feature/Darshan"
echo "====================================================================="
echo ""

echo "[1/5] Synchronizing latest changes into Git repository..."

# Sync staging models
if [ -f "$DBT_DIR/models/staging/stg_sensor_events_v1.sql" ]; then
    cp -f "$DBT_DIR/models/staging/stg_sensor_events_v1.sql" "$REPO_DIR/models/staging/"
fi
if [ -f "$DBT_DIR/models/staging/_stg_sensor_events_v1.yml" ]; then
    cp -f "$DBT_DIR/models/staging/_stg_sensor_events_v1.yml" "$REPO_DIR/models/staging/"
fi

# Sync scripts
if [ -f "$DBT_DIR/scripts/kafka_to_snowflake_consumer.py" ]; then
    cp -f "$DBT_DIR/scripts/kafka_to_snowflake_consumer.py" "$REPO_DIR/scripts/"
fi

# Sync docs
if [ -f "$DBT_DIR/docs/KAFKA_SNOWFLAKE_INGESTION.md" ]; then
    cp -f "$DBT_DIR/docs/KAFKA_SNOWFLAKE_INGESTION.md" "$REPO_DIR/docs/"
fi

# Sync requirements
if [ -f "$DBT_DIR/requirements-ingestion.txt" ]; then
    cp -f "$DBT_DIR/requirements-ingestion.txt" "$REPO_DIR/"
fi

# Sync tests
if [ -d "$DBT_DIR/tests" ]; then
    cp -f "$DBT_DIR/tests"/assert_*.sql "$REPO_DIR/tests/" 2>/dev/null || true
    cp -f "$DBT_DIR/tests"/test_*.py "$REPO_DIR/tests/" 2>/dev/null || true
fi

# Sync root configs
if [ -f "$ROOT_DIR/snowflake_setup.sql" ]; then
    cp -f "$ROOT_DIR/snowflake_setup.sql" "$REPO_DIR/"
fi
if [ -f "$ROOT_DIR/docker-compose.superset.yml" ]; then
    cp -f "$ROOT_DIR/docker-compose.superset.yml" "$REPO_DIR/"
fi

echo "[OK] Synchronization complete."
echo ""

cd "$REPO_DIR"

echo "[2/5] Checking Git branch..."
git checkout feature/Darshan

echo ""
echo "[3/5] Inspecting working directory changes..."
git status --short
echo ""

COMMIT_MSG="${1:-}"
if [ -z "$COMMIT_MSG" ]; then
    read -r -p "Enter commit message (Press Enter for default): " USER_INPUT
    COMMIT_MSG="${USER_INPUT:-Update Task 2 Warehouse & ELT code on feature/Darshan}"
fi

echo ""
echo "[4/5] Staging and committing changes..."
git add -A
git commit -m "$COMMIT_MSG" || echo "[INFO] No changes to commit."

echo ""
echo "[5/5] Pushing to GitHub (origin feature/Darshan)...
git push origin feature/Darshan

echo ""
echo "====================================================================="
echo "[SUCCESS] All code successfully uploaded to GitHub!"
echo "Branch:  feature/Darshan"
echo "Repo:    https://github.com/RohanVipin/AtmoSync_Micro_Climate_Arbitrage_Analytics.git"
echo "====================================================================="
