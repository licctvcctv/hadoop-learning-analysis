#!/bin/bash
# ============================================
# Hive数据仓库初始化脚本
# 创建数据库和所有表
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

# 配置
HIVE_SERVER=${HIVE_SERVER:-hive-server}
HIVE_PORT=${HIVE_PORT:-10000}
SCRIPTS_DIR=${SCRIPTS_DIR:-/scripts/hive}

# 等待Hive服务就绪
wait_for_hive() {
    log_info "等待Hive服务就绪..."
    max_attempts=30
    attempt=0
    
    while [ $attempt -lt $max_attempts ]; do
        if nc -z $HIVE_SERVER $HIVE_PORT 2>/dev/null; then
            log_info "Hive服务已就绪"
            return 0
        fi
        attempt=$((attempt + 1))
        log_warn "等待Hive服务... ($attempt/$max_attempts)"
        sleep 10
    done
    
    log_error "Hive服务未就绪，超时退出"
    exit 1
}

# 执行SQL文件
execute_sql_file() {
    local sql_file=$1
    local description=$2
    
    log_info "执行: $description"
    log_info "SQL文件: $sql_file"
    
    if [ -f "$sql_file" ]; then
        beeline -u "jdbc:hive2://$HIVE_SERVER:$HIVE_PORT" \
            -f "$sql_file" 2>&1 | while read line; do
            echo "  $line"
        done
        
        if [ ${PIPESTATUS[0]} -eq 0 ]; then
            log_info "$description - 完成"
        else
            log_error "$description - 失败"
            return 1
        fi
    else
        log_error "SQL文件不存在: $sql_file"
        return 1
    fi
}

# 主函数
main() {
    log_info "=========================================="
    log_info "Hive数据仓库初始化开始"
    log_info "=========================================="
    
    wait_for_hive
    
    # 创建数据库
    log_info "创建数据库..."
    beeline -u "jdbc:hive2://$HIVE_SERVER:$HIVE_PORT" \
        -e "CREATE DATABASE IF NOT EXISTS learning_behavior COMMENT '学习行为数据仓库';"
    
    # 按顺序执行SQL脚本
    execute_sql_file "$SCRIPTS_DIR/create_ods_tables.sql" "创建ODS层表"
    execute_sql_file "$SCRIPTS_DIR/create_dwd_tables.sql" "创建DWD层表"
    execute_sql_file "$SCRIPTS_DIR/create_dws_tables.sql" "创建DWS层表"
    execute_sql_file "$SCRIPTS_DIR/create_ads_tables.sql" "创建ADS层表"
    
    log_info "=========================================="
    log_info "Hive数据仓库初始化完成"
    log_info "=========================================="
    
    # 显示表列表
    log_info "已创建的表:"
    beeline -u "jdbc:hive2://$HIVE_SERVER:$HIVE_PORT" \
        -e "USE learning_behavior; SHOW TABLES;" 2>/dev/null | grep -v "^+" | grep -v "tab_name"
}

main "$@"
