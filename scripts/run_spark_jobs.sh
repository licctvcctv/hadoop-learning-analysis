#!/bin/bash
# ============================================
# Spark分析任务调度脚本
# 按顺序执行所有Spark分析任务
# 使用YARN模式提交任务
# ============================================

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# 日志函数
log_info() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')] [INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] [WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR]${NC} $1"
}

log_step() {
    echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')] [STEP]${NC} $1"
}

# 配置
SPARK_HOME=${SPARK_HOME:-/opt/spark}
SPARK_ANALYSIS_DIR=${SPARK_ANALYSIS_DIR:-/opt/spark/work/spark_analysis}
LOG_DIR=${LOG_DIR:-/opt/spark/logs}

# 确保日志目录存在
mkdir -p $LOG_DIR

# 执行Spark任务 - 使用YARN模式
run_spark_job() {
    local job_name=$1
    local job_file=$2
    local log_file="$LOG_DIR/${job_name}_$(date '+%Y%m%d_%H%M%S').log"
    
    log_step "开始执行: $job_name"
    log_info "任务文件: $job_file"
    log_info "日志文件: $log_file"
    
    start_time=$(date +%s)
    
    # 使用YARN模式提交Spark任务
    $SPARK_HOME/bin/spark-submit \
        --master yarn \
        --deploy-mode client \
        --driver-memory 1g \
        --executor-memory 1g \
        --executor-cores 1 \
        --num-executors 2 \
        --conf spark.sql.warehouse.dir=hdfs://namenode:9000/user/hive/warehouse \
        --conf spark.hadoop.hive.metastore.uris=thrift://hive-server:9083 \
        --conf spark.yarn.submit.waitAppCompletion=true \
        $job_file 2>&1 | tee $log_file
    
    exit_code=${PIPESTATUS[0]}
    end_time=$(date +%s)
    elapsed=$((end_time - start_time))
    
    if [ $exit_code -eq 0 ]; then
        log_info "$job_name 执行成功，耗时: ${elapsed}秒"
        return 0
    else
        log_error "$job_name 执行失败，退出码: $exit_code"
        return 1
    fi
}

# 主函数
main() {
    log_info "=========================================="
    log_info "Spark分析任务调度开始"
    log_info "=========================================="
    log_info "运行模式: YARN"
    log_info "分析脚本目录: $SPARK_ANALYSIS_DIR"
    log_info "日志目录: $LOG_DIR"
    
    total_start=$(date +%s)
    failed_jobs=()
    
    # 任务列表
    declare -a jobs=(
        "study_duration:$SPARK_ANALYSIS_DIR/study_duration.py"
        "course_popularity:$SPARK_ANALYSIS_DIR/course_popularity.py"
        "behavior_pattern:$SPARK_ANALYSIS_DIR/behavior_pattern.py"
        "abnormal_detection:$SPARK_ANALYSIS_DIR/abnormal_detection.py"
    )
    
    # 执行所有任务
    for job in "${jobs[@]}"; do
        job_name="${job%%:*}"
        job_file="${job##*:}"
        
        echo ""
        log_info "------------------------------------------"
        
        if [ -f "$job_file" ]; then
            if run_spark_job "$job_name" "$job_file"; then
                log_info "$job_name 完成"
            else
                failed_jobs+=("$job_name")
                log_warn "继续执行下一个任务..."
            fi
        else
            log_error "任务文件不存在: $job_file"
            failed_jobs+=("$job_name")
        fi
    done
    
    # 汇总结果
    echo ""
    log_info "=========================================="
    log_info "Spark分析任务调度完成"
    log_info "=========================================="
    
    total_end=$(date +%s)
    total_elapsed=$((total_end - total_start))
    
    log_info "总耗时: ${total_elapsed}秒"
    log_info "成功任务: $((${#jobs[@]} - ${#failed_jobs[@]}))/${#jobs[@]}"
    
    if [ ${#failed_jobs[@]} -gt 0 ]; then
        log_error "失败任务: ${failed_jobs[*]}"
        exit 1
    else
        log_info "所有任务执行成功!"
        exit 0
    fi
}

# 支持单独执行某个任务
if [ "$1" != "" ]; then
    case "$1" in
        study_duration)
            run_spark_job "study_duration" "$SPARK_ANALYSIS_DIR/study_duration.py"
            ;;
        course_popularity)
            run_spark_job "course_popularity" "$SPARK_ANALYSIS_DIR/course_popularity.py"
            ;;
        behavior_pattern)
            run_spark_job "behavior_pattern" "$SPARK_ANALYSIS_DIR/behavior_pattern.py"
            ;;
        abnormal_detection)
            run_spark_job "abnormal_detection" "$SPARK_ANALYSIS_DIR/abnormal_detection.py"
            ;;
        all)
            main
            ;;
        *)
            echo "用法: $0 [study_duration|course_popularity|behavior_pattern|abnormal_detection|all]"
            echo "  不带参数默认执行所有任务"
            exit 1
            ;;
    esac
else
    main
fi
