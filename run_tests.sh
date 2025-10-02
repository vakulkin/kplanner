#!/bin/bash

# KPlanner API Test Runner
# This script runs all tests for the KPlanner API
# Now includes pagination tests and enhanced validation

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Use virtual environment Python if available
if [ -f "$SCRIPT_DIR/.venv/bin/python" ]; then
    PYTHON="$SCRIPT_DIR/.venv/bin/python"
else
    PYTHON="python3"
fi

# Track test results
TESTS_PASSED=0
TESTS_FAILED=0
START_TIME=$(date +%s)

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}   KPlanner API Test Suite v2.0${NC}"
echo -e "${BLUE}   With Pagination & Batch Testing${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Function to run a test and track results
run_test() {
    local test_name="$1"
    local test_command="$2"
    
    echo -e "${YELLOW}Running: ${test_name}${NC}"
    if eval "$test_command"; then
        echo -e "${GREEN}✅ PASSED: ${test_name}${NC}"
        ((TESTS_PASSED++))
        return 0
    else
        echo -e "${RED}❌ FAILED: ${test_name}${NC}"
        ((TESTS_FAILED++))
        return 1
    fi
    echo ""
}

# Check if server is running
echo -e "${YELLOW}Checking API server status...${NC}"
if curl -s http://localhost:8000/ > /dev/null 2>&1; then
    SERVER_INFO=$(curl -s http://localhost:8000/ | python3 -m json.tool 2>/dev/null || echo "{}")
    echo -e "${GREEN}✅ API server is running${NC}"
    echo "$SERVER_INFO" | grep -E "(mode|message)" || true
else
    echo -e "${RED}❌ API server is not running!${NC}"
    echo ""
    echo "Please start the server first:"
    echo "  source .venv/bin/activate"
    echo "  uvicorn main:app --host 0.0.0.0 --port 8000 --reload"
    echo ""
    exit 1
fi

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}   Phase 1: Comprehensive API Tests${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

run_test "Comprehensive API Tests" "$PYTHON test_api.py"

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}   Phase 2: Pagination Tests${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

run_test "Pagination & Batch Processing Tests" "$PYTHON test_pagination.py"

echo ""
echo ""
read -p "$(echo -e ${YELLOW}Run stress tests? \(y/n\): ${NC})" run_stress

if [ "$run_stress" = "y" ] || [ "$run_stress" = "Y" ]; then
    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}   Phase 3: Stress Tests${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""
    
    run_test "Stress Tests" "$PYTHON test_stress.py"
fi

# Calculate duration
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}   Test Suite Complete!${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo -e "${GREEN}Tests Passed: ${TESTS_PASSED}${NC}"
if [ $TESTS_FAILED -gt 0 ]; then
    echo -e "${RED}Tests Failed: ${TESTS_FAILED}${NC}"
fi
echo -e "Duration: ${DURATION}s"
echo ""
echo -e "View API documentation at: ${BLUE}http://localhost:8000/docs${NC}"
echo ""

# Exit with error if any tests failed
if [ $TESTS_FAILED -gt 0 ]; then
    exit 1
fi
