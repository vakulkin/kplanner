#!/bin/bash

# Demo Data Management Script Wrapper
# Activates virtual environment and runs the demo_data.py script

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Activate virtual environment
if [ -d "$PROJECT_DIR/.venv" ]; then
    source "$PROJECT_DIR/.venv/bin/activate"
else
    echo "❌ Virtual environment not found at $PROJECT_DIR/.venv"
    echo "Please create it first: python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt"
    exit 1
fi

# Run the demo_data.py script with all arguments
python "$SCRIPT_DIR/demo_data.py" "$@"

# Deactivate virtual environment
deactivate
