#!/bin/bash
# ============================================
# Hadoop容器启动脚本
# ============================================

set -e

# 日志函数
log_info() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO] [Hadoop] $1"
}

log_error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] [Hadoop] $1" >&2
}

log_warn() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [WARN] [Hadoop] $1"
}

# 启动SSH服务
start_ssh() {
    log_info "启动SSH服务..."
    service ssh start
    if [ $? -eq 0 ]; then
        log_info "SSH服务启动成功"
    else
        log_error "SSH服务启动失败"
        exit 1
    fi
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

# 根据角色启动不同服务
case "$HADOOP_ROLE" in
    namenode)
        log_info "========== 启动 NameNode =========="
        start_ssh
        
        # 检查是否需要格式化
        if [ ! -d "/data/hdfs/namenode/current" ]; then
            log_info "首次启动，格式化HDFS..."
            hdfs namenode -format -force -nonInteractive
            if [ $? -eq 0 ]; then
                log_info "HDFS格式化成功"
            else
                log_error "HDFS格式化失败"
                exit 1
            fi
        else
            log_info "HDFS已格式化，跳过格式化步骤"
        fi
        
        log_info "启动NameNode服务..."
        hdfs namenode &
        
        # 等待NameNode启动
        sleep 5
        wait_for_service localhost 9870 "NameNode Web UI"
        
        log_info "NameNode启动完成，Web UI: http://localhost:9870"
        ;;
        
    datanode)
        log_info "========== 启动 DataNode =========="
        start_ssh
        
        # 等待NameNode就绪
        wait_for_service namenode 9000 "NameNode"
        
        log_info "启动DataNode服务..."
        hdfs datanode &
        
        sleep 3
        log_info "DataNode启动完成"
        ;;
        
    resourcemanager)
        log_info "========== 启动 ResourceManager =========="
        start_ssh
        
        # 等待NameNode就绪
        wait_for_service namenode 9000 "NameNode"
        
        log_info "启动ResourceManager服务..."
        yarn resourcemanager &
        
        sleep 5
        wait_for_service localhost 8088 "ResourceManager Web UI"
        
        log_info "ResourceManager启动完成，Web UI: http://localhost:8088"
        ;;
        
    nodemanager)
        log_info "========== 启动 NodeManager =========="
        start_ssh
        
        # 等待ResourceManager就绪
        wait_for_service resourcemanager 8088 "ResourceManager"
        
        log_info "启动NodeManager服务..."
        yarn nodemanager &
        
        sleep 3
        log_info "NodeManager启动完成"
        ;;
        
    *)
        log_error "未知的HADOOP_ROLE: $HADOOP_ROLE"
        log_info "支持的角色: namenode, datanode, resourcemanager, nodemanager"
        exit 1
        ;;
esac

# 保持容器运行
log_info "服务已启动，保持容器运行..."
tail -f /dev/null
