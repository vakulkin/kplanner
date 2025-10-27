#!/bin/bash

# Demo Data Management Script Wrapper
# Activates virtual environment and runs the demo_data.py script
#
# Usage:
#   ./demo/demo_data.sh import [small|medium|large|huge]  # Import demo data (default: medium)
#   ./demo/demo_data.sh import large                      # Install large demo dataset
#   ./demo/demo_data.sh cleanup                           # Remove all demo data
#   ./demo/demo_data.sh verify                            # Verify imported data
#   ./demo/demo_data.sh stats                             # Show detailed statistics
#
# Examples:
#   ./demo/demo_data.sh import large    # Install large demo data
#   ./demo/demo_data.sh cleanup         # Clean up demo data
#   ./demo/demo_data.sh stats           # View statistics

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
python3 "$SCRIPT_DIR/demo_data.py" "$@"

# Deactivate virtual environment
deactivate
