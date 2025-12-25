#!/bin/bash
# ============================================
# Hive容器启动脚本
# ============================================

set -e

log_info() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO] [Hive] $1"
}

log_error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] [Hive] $1" >&2
}

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

init_metastore() {
    log_info "检查Hive元数据库..."
    
    if schematool -dbType mysql -info > /dev/null 2>&1; then
        log_info "Hive元数据库已存在"
    else
        log_info "初始化Hive元数据库..."
        schematool -dbType mysql -initSchema
        if [ $? -eq 0 ]; then
            log_info "Hive元数据库初始化成功"
        else
            log_error "Hive元数据库初始化失败"
            exit 1
        fi
    fi
}

log_info "========== 启动 Hive Server =========="

# 等待依赖服务 (只等待 MySQL 和 HDFS)
wait_for_service mysql 3306 "MySQL"
wait_for_service namenode 9000 "HDFS NameNode"

# 创建HDFS目录
log_info "创建HDFS目录..."
hdfs dfs -mkdir -p /user/hive/warehouse || true
hdfs dfs -mkdir -p /tmp || true
hdfs dfs -chmod -R 777 /user/hive/warehouse || true
hdfs dfs -chmod -R 777 /tmp || true

# 创建学习行为数据目录
log_info "创建学习行为数据HDFS目录..."
hdfs dfs -mkdir -p /user/learning_behavior/raw || true
hdfs dfs -mkdir -p /user/learning_behavior/ods || true
hdfs dfs -mkdir -p /user/learning_behavior/dwd || true
hdfs dfs -mkdir -p /user/learning_behavior/dws || true
hdfs dfs -mkdir -p /user/learning_behavior/ads || true
hdfs dfs -chmod -R 777 /user/learning_behavior || true

log_info "HDFS目录创建完成"

# 初始化元数据库
init_metastore

# 启动Hive Metastore
log_info "启动Hive Metastore..."
hive --service metastore &
sleep 10

# 启动HiveServer2
log_info "启动HiveServer2..."
hive --service hiveserver2 &

# 等待HiveServer2启动
sleep 10
wait_for_service localhost 10000 "HiveServer2"

log_info "Hive启动完成"
log_info "HiveServer2 JDBC: jdbc:hive2://localhost:10000"
log_info "Hive Metastore: thrift://localhost:9083"

# 保持容器运行
tail -f /dev/null
