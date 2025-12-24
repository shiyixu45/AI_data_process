#!/bin/bash
# 大规模数据处理测试脚本

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

echo "=============================================="
echo "Data Processing Framework - Large Scale Tests"
echo "=============================================="

# Test 1: Small scale test (10K rows)
echo ""
echo "Test 1: Small scale test (10,000 rows)"
echo "--------------------------------------"
python tests/test_large_scale.py --rows 10000 --workers 2 --batch-size 500 --plugin passthrough

# Test 2: Medium scale test (100K rows)
echo ""
echo "Test 2: Medium scale test (100,000 rows)"
echo "----------------------------------------"
python tests/test_large_scale.py --rows 100000 --workers 4 --batch-size 1000 --plugin passthrough

# Test 3: Score filter plugin test
echo ""
echo "Test 3: Score filter plugin test (50,000 rows)"
echo "-----------------------------------------------"
python tests/test_large_scale.py --rows 50000 --workers 4 --batch-size 1000 --plugin score_filter

# Test 4: Keep order test
echo ""
echo "Test 4: Keep order test (50,000 rows)"
echo "-------------------------------------"
python tests/test_large_scale.py --rows 50000 --workers 4 --batch-size 1000 --plugin passthrough --keep-order

# Test 5: Large batch size test
echo ""
echo "Test 5: Large batch size test (100,000 rows)"
echo "--------------------------------------------"
python tests/test_large_scale.py --rows 100000 --workers 4 --batch-size 5000 --plugin passthrough

echo ""
echo "=============================================="
echo "All tests completed!"
echo "=============================================="

