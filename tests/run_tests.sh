#!/bin/bash
# ============================================
# 运行属性测试脚本
# ============================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=========================================="
echo "运行属性测试"
echo "=========================================="

cd "$PROJECT_DIR"

# 安装测试依赖
echo "安装测试依赖..."
pip install -q -r tests/requirements-test.txt

# 运行所有属性测试
echo ""
echo "运行属性测试..."
echo ""

pytest tests/ -v --tb=short \
    --hypothesis-show-statistics \
    -x  # 遇到第一个失败就停止

echo ""
echo "=========================================="
echo "测试完成"
echo "=========================================="
