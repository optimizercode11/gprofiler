#!/usr/bin/env bash
set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

echo "Installing dependencies..."
python3 -m pip install -q --upgrade setuptools pip
python3 -m pip install -r "$REPO_ROOT/requirements.txt" -r "$REPO_ROOT/dev-requirements.txt"

echo "Running Spark Agent Tests..."
export PYTHONPATH="$PYTHONPATH:$REPO_ROOT/granulate-utils:$REPO_ROOT"
python3 -m pytest -v "$SCRIPT_DIR/test_spark_agent.py" "$SCRIPT_DIR/test_spark_integration.py"
