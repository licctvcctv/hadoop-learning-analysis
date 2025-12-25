#!/bin/bash
# ============================================
# 演示流程脚本
# 完整演示系统功能
# ============================================

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# 日志函数
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_step() {
    echo -e "${CYAN}[STEP]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_banner() {
    echo -e "${BLUE}"
    echo "=============================================="
    echo "  大学生线上课程学习行为分析系统 - 演示"
    echo "=============================================="
    echo -e "${NC}"
}

wait_for_key() {
    echo ""
    read -p "按 Enter 继续下一步..." key
    echo ""
}

# 检查服务状态
check_services() {
    log_step "检查服务状态..."
    
    services=("namenode:9870" "resourcemanager:8088" "hive-server:10000" "spark-master:8080" "web:5000")
    
    for service in "${services[@]}"; do
        host="${service%%:*}"
        port="${service##*:}"
        if docker exec namenode nc -z $host $port 2>/dev/null; then
            log_info "$host:$port - 正常"
        else
            log_warn "$host:$port - 未就绪"
        fi
    done
}

# 步骤1: 生成测试数据
step1_generate_data() {
    log_step "步骤1: 生成学习行为测试数据"
    echo "将生成1000条模拟的学习行为日志数据..."
    
    # 在宿主机运行数据生成器，输出到 data/logs 目录
    # 该目录通过 docker-compose 卷挂载共享给 flume 容器
    if [ -f "$PROJECT_DIR/src/data_generator/cli.py" ]; then
        cd "$PROJECT_DIR"
        python -m src.data_generator.cli \
            --count 1000 \
            --output "$PROJECT_DIR/data/logs" \
            --format json
        
        log_info "数据生成完成，文件位置: $PROJECT_DIR/data/logs/"
        log_info "Flume将自动采集数据到HDFS: /user/learning_behavior/raw/"
    else
        log_error "数据生成器不存在: $PROJECT_DIR/src/data_generator/cli.py"
        log_info "请确保在项目根目录运行此脚本"
    fi
}

# 步骤2: 查看HDFS数据
step2_check_hdfs() {
    log_step "步骤2: 查看HDFS数据目录"
    
    log_info "HDFS根目录结构:"
    docker exec namenode hdfs dfs -ls / 2>/dev/null || log_warn "HDFS目录为空或未初始化"
    
    log_info "学习行为数据目录:"
    docker exec namenode hdfs dfs -ls /user/learning_behavior 2>/dev/null || log_warn "数据目录不存在"
    
    log_info "原始数据目录 (Flume输出):"
    docker exec namenode hdfs dfs -ls /user/learning_behavior/raw 2>/dev/null || log_warn "原始数据目录为空"
}

# 步骤3: 执行Hive ETL
step3_run_hive_etl() {
    log_step "步骤3: 执行Hive数据仓库ETL"
    
    log_info "创建Hive数据库和表..."
    
    # 创建ODS层表
    log_info "创建ODS层表..."
    docker exec hive-server beeline -u "jdbc:hive2://localhost:10000" \
        -f "/scripts/hive/create_ods_tables.sql" 2>/dev/null || log_warn "ODS表创建可能已存在"
    
    # 创建DWD层表
    log_info "创建DWD层表..."
    docker exec hive-server beeline -u "jdbc:hive2://localhost:10000" \
        -f "/scripts/hive/create_dwd_tables.sql" 2>/dev/null || log_warn "DWD表创建可能已存在"
    
    # 创建DWS层表
    log_info "创建DWS层表..."
    docker exec hive-server beeline -u "jdbc:hive2://localhost:10000" \
        -f "/scripts/hive/create_dws_tables.sql" 2>/dev/null || log_warn "DWS表创建可能已存在"
    
    # 创建ADS层表
    log_info "创建ADS层表..."
    docker exec hive-server beeline -u "jdbc:hive2://localhost:10000" \
        -f "/scripts/hive/create_ads_tables.sql" 2>/dev/null || log_warn "ADS表创建可能已存在"
    
    # 修复ODS分区（发现Flume写入的数据）
    log_info "修复ODS分区..."
    docker exec hive-server beeline -u "jdbc:hive2://localhost:10000" \
        -e "USE learning_behavior; MSCK REPAIR TABLE ods_learning_behavior;" 2>/dev/null || log_warn "分区修复可能无新分区"
    
    # 执行ETL加载
    log_info "执行ETL数据加载..."
    docker exec hive-server beeline -u "jdbc:hive2://localhost:10000" \
        -f "/scripts/hive/etl_load.sql" 2>/dev/null || log_warn "ETL加载可能失败，请检查数据"
    
    # 显示表列表
    log_info "显示已创建的表..."
    docker exec hive-server beeline -u "jdbc:hive2://localhost:10000" \
        -e "USE learning_behavior; SHOW TABLES;" 2>/dev/null
    
    log_info "Hive ETL完成"
}

# 步骤4: 执行Spark分析
step4_run_spark_analysis() {
    log_step "步骤4: 执行Spark数据分析"
    
    log_info "执行学习时长统计..."
    docker exec spark-master /opt/spark/bin/spark-submit \
        --master yarn \
        --deploy-mode client \
        /opt/spark/work/spark_analysis/study_duration.py 2>&1 | tail -20
    
    log_info "Spark分析完成"
}

# 步骤5: 查看Web界面
step5_show_web() {
    log_step "步骤5: Web可视化界面"
    
    echo ""
    echo "请在浏览器中访问以下地址:"
    echo ""
    echo -e "  ${CYAN}数据分析仪表盘:${NC} http://localhost:5000"
    echo -e "  ${CYAN}系统监控页面:${NC}   http://localhost:5000/monitor"
    echo -e "  ${CYAN}日志查看页面:${NC}   http://localhost:5000/logs"
    echo ""
    echo "组件Web UI:"
    echo -e "  ${CYAN}HDFS NameNode:${NC}   http://localhost:9870"
    echo -e "  ${CYAN}YARN ResourceManager:${NC} http://localhost:8088"
    echo -e "  ${CYAN}Spark Master:${NC}    http://localhost:8080"
    echo -e "  ${CYAN}HiveServer2:${NC}     http://localhost:10002"
    echo ""
}

# 快速演示
quick_demo() {
    print_banner
    
    log_info "开始快速演示..."
    echo ""
    
    check_services
    wait_for_key
    
    step1_generate_data
    wait_for_key
    
    step2_check_hdfs
    wait_for_key
    
    step5_show_web
    
    echo ""
    log_info "演示完成!"
    echo ""
}

# 完整演示
full_demo() {
    print_banner
    
    log_info "开始完整演示流程..."
    echo ""
    
    check_services
    wait_for_key
    
    step1_generate_data
    wait_for_key
    
    step2_check_hdfs
    wait_for_key
    
    step3_run_hive_etl
    wait_for_key
    
    step4_run_spark_analysis
    wait_for_key
    
    step5_show_web
    
    echo ""
    log_info "完整演示完成!"
    echo ""
}

# 主函数
main() {
    case "${1:-quick}" in
        quick)
            quick_demo
            ;;
        full)
            full_demo
            ;;
        generate)
            step1_generate_data
            ;;
        hdfs)
            step2_check_hdfs
            ;;
        hive)
            step3_run_hive_etl
            ;;
        spark)
            step4_run_spark_analysis
            ;;
        web)
            step5_show_web
            ;;
        *)
            echo "用法: $0 [quick|full|generate|hdfs|hive|spark|web]"
            echo ""
            echo "  quick    - 快速演示（默认）"
            echo "  full     - 完整演示流程"
            echo "  generate - 仅生成测试数据"
            echo "  hdfs     - 查看HDFS数据"
            echo "  hive     - 执行Hive ETL"
            echo "  spark    - 执行Spark分析"
            echo "  web      - 显示Web界面地址"
            exit 1
            ;;
    esac
}

main "$@"
