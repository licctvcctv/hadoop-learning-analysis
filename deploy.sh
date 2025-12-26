#!/bin/bash
# ============================================
# 一键部署脚本
# 基于Hadoop的大学生线上课程学习行为数据存储与分析系统
# ============================================

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${GREEN}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $(date '+%Y-%m-%d %H:%M:%S') $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') $1"
}

log_step() {
    echo -e "${BLUE}[STEP]${NC} $(date '+%Y-%m-%d %H:%M:%S') $1"
}

# 检查Docker环境
check_docker() {
    log_step "检查Docker环境..."
    
    if ! command -v docker &> /dev/null; then
        log_error "Docker未安装，请先安装Docker"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        log_error "Docker Compose未安装，请先安装Docker Compose"
        exit 1
    fi
    
    if ! docker info &> /dev/null; then
        log_error "Docker服务未运行，请启动Docker服务"
        exit 1
    fi
    
    log_info "Docker环境检查通过"
}


# 创建必要的目录
create_directories() {
    log_step "创建必要的目录..."
    
    mkdir -p logs/hadoop/namenode
    mkdir -p logs/hadoop/datanode1
    mkdir -p logs/hadoop/datanode2
    mkdir -p logs/hadoop/resourcemanager
    mkdir -p logs/hadoop/nodemanager1
    mkdir -p logs/hadoop/nodemanager2
    mkdir -p logs/spark
    mkdir -p logs/flume
    mkdir -p logs/web
    mkdir -p logs/data-generator
    mkdir -p data/logs
    
    log_info "目录创建完成"
}

# 构建Docker镜像
build_images() {
    log_step "构建Docker镜像..."
    
    if docker compose version &> /dev/null; then
        COMPOSE_CMD="docker compose"
    else
        COMPOSE_CMD="docker-compose"
    fi
    
    $COMPOSE_CMD build --no-cache
    
    log_info "Docker镜像构建完成"
}

# 启动服务
start_services() {
    log_step "启动服务..."
    
    if docker compose version &> /dev/null; then
        COMPOSE_CMD="docker compose"
    else
        COMPOSE_CMD="docker-compose"
    fi
    
    $COMPOSE_CMD up -d
    
    log_info "服务启动命令已执行"
}

# 等待服务就绪
wait_for_services() {
    log_step "等待服务就绪..."
    
    local max_wait=300
    local waited=0
    local interval=10
    
    # 等待NameNode
    log_info "等待HDFS NameNode..."
    while [ $waited -lt $max_wait ]; do
        if curl -s http://localhost:9870 > /dev/null 2>&1; then
            log_info "HDFS NameNode已就绪"
            break
        fi
        sleep $interval
        waited=$((waited + interval))
        log_info "等待中... ($waited/$max_wait 秒)"
    done
    
    if [ $waited -ge $max_wait ]; then
        log_warn "HDFS NameNode启动超时，请检查日志"
    fi
    
    # 等待YARN ResourceManager
    waited=0
    log_info "等待YARN ResourceManager..."
    while [ $waited -lt $max_wait ]; do
        if curl -s http://localhost:8088 > /dev/null 2>&1; then
            log_info "YARN ResourceManager已就绪"
            break
        fi
        sleep $interval
        waited=$((waited + interval))
        log_info "等待中... ($waited/$max_wait 秒)"
    done
    
    # 等待Spark Master
    waited=0
    log_info "等待Spark Master..."
    while [ $waited -lt $max_wait ]; do
        if curl -s http://localhost:8080 > /dev/null 2>&1; then
            log_info "Spark Master已就绪"
            break
        fi
        sleep $interval
        waited=$((waited + interval))
        log_info "等待中... ($waited/$max_wait 秒)"
    done
}

# 显示服务状态
show_status() {
    log_step "服务状态..."
    
    if docker compose version &> /dev/null; then
        COMPOSE_CMD="docker compose"
    else
        COMPOSE_CMD="docker-compose"
    fi
    
    echo ""
    $COMPOSE_CMD ps
    echo ""
}

# 显示访问地址
show_urls() {
    echo ""
    echo "============================================"
    echo "  系统部署完成！"
    echo "============================================"
    echo ""
    echo "访问地址："
    echo "  - HDFS NameNode:       http://localhost:9870"
    echo "  - YARN ResourceManager: http://localhost:8088"
    echo "  - Spark Master:        http://localhost:8080"
    echo "  - Spark Worker:        http://localhost:8081"
    echo "  - Web可视化界面:        http://localhost:5000"
    echo ""
    echo "日志目录: ./logs/"
    echo ""
    echo "停止系统: ./stop.sh"
    echo "============================================"
}

# 主函数
main() {
    echo ""
    echo "============================================"
    echo "  基于Hadoop的大学生线上课程学习行为"
    echo "  数据存储与分析系统 - 一键部署"
    echo "============================================"
    echo ""
    
    check_docker
    create_directories
    build_images
    start_services
    wait_for_services
    show_status
    show_urls
}

# 执行主函数
main "$@"
