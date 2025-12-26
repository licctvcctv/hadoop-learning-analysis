#!/bin/bash
# ============================================
# HDFS目录初始化脚本
# 创建数据存储目录结构
# ============================================

set -e

# 颜色定义
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')] [INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] [WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR]${NC} $1"
}

# 等待HDFS就绪
wait_for_hdfs() {
    log_info "等待HDFS服务就绪..."
    max_attempts=30
    attempt=0
    
    while [ $attempt -lt $max_attempts ]; do
        if hdfs dfs -ls / >/dev/null 2>&1; then
            log_info "HDFS服务已就绪"
            return 0
        fi
        attempt=$((attempt + 1))
        log_warn "等待HDFS服务... ($attempt/$max_attempts)"
        sleep 10
    done
    
    log_error "HDFS服务未就绪，超时退出"
    exit 1
}

# 创建目录
create_directory() {
    local dir=$1
    log_info "创建目录: $dir"
    hdfs dfs -mkdir -p $dir
    hdfs dfs -chmod 777 $dir
}

# 主函数
main() {
    log_info "=========================================="
    log_info "HDFS目录初始化开始"
    log_info "=========================================="
    
    wait_for_hdfs
    
    # 创建数据目录结构
    log_info "创建数据目录结构..."
    
    # 学习行为数据根目录
    create_directory "/learning_data"
    
    # Spark检查点目录
    create_directory "/spark/checkpoint"
    
    # 临时目录
    create_directory "/tmp"
    
    log_info "=========================================="
    log_info "HDFS目录初始化完成"
    log_info "=========================================="
    
    # 显示目录结构
    log_info "HDFS目录结构:"
    hdfs dfs -ls / 2>/dev/null
}

main "$@"
