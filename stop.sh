#!/bin/bash
# ============================================
# 停止所有服务
# ============================================

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

echo "停止服务..."

# 停止 Web 服务
if pkill -f "python.*app.py" 2>/dev/null; then
    echo -e "${GREEN}[OK]${NC} Web 服务已停止"
else
    echo -e "${RED}[--]${NC} Web 服务未运行"
fi

# 停止 Hadoop
source /etc/profile.d/hadoop.sh 2>/dev/null || true

if command -v stop-dfs.sh &> /dev/null; then
    stop-dfs.sh 2>/dev/null
    echo -e "${GREEN}[OK]${NC} HDFS 已停止"
fi

if command -v stop-yarn.sh &> /dev/null; then
    stop-yarn.sh 2>/dev/null
    echo -e "${GREEN}[OK]${NC} YARN 已停止"
fi

echo ""
echo "所有服务已停止"
