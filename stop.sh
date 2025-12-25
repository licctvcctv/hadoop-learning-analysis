#!/bin/bash
# ============================================
# 停止脚本
# 基于Hadoop的大学生线上课程学习行为数据存储与分析系统
# ============================================

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $(date '+%Y-%m-%d %H:%M:%S') $1"
}

# 使用docker compose或docker-compose
if docker compose version &> /dev/null; then
    COMPOSE_CMD="docker compose"
else
    COMPOSE_CMD="docker-compose"
fi

echo ""
echo "============================================"
echo "  停止系统"
echo "============================================"
echo ""

# 检查是否要清理数据
if [ "$1" == "--clean" ] || [ "$1" == "-c" ]; then
    log_warn "将清理所有数据卷..."
    $COMPOSE_CMD down -v
    log_info "系统已停止，数据卷已清理"
else
    $COMPOSE_CMD down
    log_info "系统已停止，数据卷已保留"
    echo ""
    echo "提示: 使用 './stop.sh --clean' 可同时清理数据卷"
fi

echo ""
