#!/bin/bash

# KPlanner API Test Runner
# This script runs all tests for the KPlanner API using pytest
# Includes comprehensive tests for pagination, filtering, and bulk operations

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Use virtual environment Python if available
if [ -f "$PROJECT_DIR/.venv/bin/python" ]; then
    PYTHON="$PROJECT_DIR/.venv/bin/python"
    PYTEST="$PROJECT_DIR/.venv/bin/pytest"
else
    PYTHON="python3"
    PYTEST="pytest"
fi

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}   KPlanner API Test Suite${NC}"
echo -e "${BLUE}   Comprehensive Testing with pytest${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Check if pytest is installed
if ! command -v $PYTEST &> /dev/null; then
    echo -e "${YELLOW}pytest not found. Installing test dependencies...${NC}"
    $PYTHON -m pip install -r "$PROJECT_DIR/requirements.txt"
    echo ""
fi

# Check if server is running (optional - tests will work without it for unit tests)
echo -e "${YELLOW}Checking API server status...${NC}"
if curl -s http://localhost:8000/ > /dev/null 2>&1; then
    echo -e "${GREEN}✅ API server is running${NC}"
else
    echo -e "${YELLOW}⚠️  API server is not running${NC}"
    echo -e "${YELLOW}Tests will use TestClient (no server required)${NC}"
fi

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}   Running pytest test suite${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Change to project directory to ensure correct module resolution
cd "$PROJECT_DIR"

# Run pytest with various options
# -v: verbose
# -s: show print statements
# --tb=short: shorter traceback format
# Coverage options are now in pytest.ini

$PYTEST tests/test_api.py

# Show test summary
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}   Test Suite Complete!${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Optional: Run with coverage (now configured in pytest.ini)
read -p "$(echo -e ${YELLOW}Generate coverage report? \(y/n\): ${NC})" run_coverage

if [ "$run_coverage" = "y" ] || [ "$run_coverage" = "Y" ]; then
    echo ""
    echo -e "${BLUE}Generating coverage report...${NC}"
    $PYTEST tests/test_api.py --cov-report=html:tests/htmlcov --cov-report=term
    echo ""
    echo -e "${GREEN}Coverage report generated in tests/htmlcov/index.html${NC}"
fi

echo ""
echo -e "View API documentation at: ${BLUE}http://localhost:8000/docs${NC}"
echo ""
