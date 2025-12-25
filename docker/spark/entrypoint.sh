#!/bin/bash
# ============================================
# Spark容器启动脚本
# ============================================

set -e

# 日志函数
log_info() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO] [Spark] $1"
}

log_error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] [Spark] $1" >&2
}

log_warn() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [WARN] [Spark] $1"
}

# 等待服务可用
wait_for_service() {
    local host=$1
    local port=$2
    local service_name=$3
    local max_attempts=30
    local attempt=1
    
    log_info "等待 ${service_name} (${host}:${port}) 启动..."
    
    while [ $attempt -le $max_attempts ]; do
        if nc -z $host $port 2>/dev/null; then
            log_info "${service_name} 已就绪"
            return 0
        fi
        log_info "等待 ${service_name}... (尝试 $attempt/$max_attempts)"
        sleep 2
        attempt=$((attempt + 1))
    done
    
    log_error "${service_name} 启动超时"
    return 1
}

case "$SPARK_ROLE" in
    master)
        log_info "========== 启动 Spark Master =========="
        
        # 等待HDFS就绪
        wait_for_service namenode 9000 "HDFS NameNode"
        
        log_info "启动Spark Master..."
        ${SPARK_HOME}/sbin/start-master.sh
        
        sleep 5
        wait_for_service localhost 8080 "Spark Master Web UI"
        
        log_info "Spark Master启动完成"
        log_info "Web UI: http://localhost:8080"
        log_info "Spark Master URL: spark://spark-master:7077"
        ;;
        
    worker)
        log_info "========== 启动 Spark Worker =========="
        
        # 等待Spark Master就绪
        wait_for_service spark-master 7077 "Spark Master"
        
        log_info "启动Spark Worker..."
        ${SPARK_HOME}/sbin/start-worker.sh spark://spark-master:7077
        
        sleep 3
        log_info "Spark Worker启动完成"
        ;;
        
    *)
        log_error "未知的SPARK_ROLE: $SPARK_ROLE"
        log_info "支持的角色: master, worker"
        exit 1
        ;;
esac

# 保持容器运行
tail -f /dev/null
