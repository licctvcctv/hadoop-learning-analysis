#!/bin/bash
# ============================================
# Flume容器启动脚本
# ============================================

set -e

# 日志函数
log_info() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO] [Flume] $1"
}

log_error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] [Flume] $1" >&2
}

log_warn() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [WARN] [Flume] $1"
}

# 等待服务可用
wait_for_service() {
    local host=$1
    local port=$2
    local service_name=$3
    local max_attempts=60
    local attempt=1
    
    log_info "等待 ${service_name} (${host}:${port}) 启动..."
    
    while [ $attempt -le $max_attempts ]; do
        if nc -z $host $port 2>/dev/null; then
            log_info "${service_name} 已就绪"
            return 0
        fi
        log_info "等待 ${service_name}... (尝试 $attempt/$max_attempts)"
        sleep 3
        attempt=$((attempt + 1))
    done
    
    log_error "${service_name} 启动超时"
    return 1
}

log_info "========== 启动 Flume Agent =========="

# 等待HDFS就绪
wait_for_service namenode 9000 "HDFS NameNode"

# 创建HDFS目录
log_info "确保HDFS目录存在..."
hdfs dfs -mkdir -p /user/learning_behavior/raw || true
hdfs dfs -chmod -R 777 /user/learning_behavior || true

# 确保本地日志目录存在
mkdir -p /data/logs
log_info "监控目录: /data/logs"

# 启动Flume Agent
log_info "启动Flume Agent..."
log_info "配置文件: /opt/flume/conf/flume.conf"

flume-ng agent \
    --name agent \
    --conf /opt/flume/conf \
    --conf-file /opt/flume/conf/flume.conf \
    -Dflume.root.logger=INFO,console \
    -Dflume.monitoring.type=http \
    -Dflume.monitoring.port=34545 &

FLUME_PID=$!

log_info "Flume Agent已启动 (PID: $FLUME_PID)"
log_info "监控端口: http://localhost:34545/metrics"

# 保持容器运行
wait $FLUME_PID
