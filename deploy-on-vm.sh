#!/bin/bash
# ============================================
# 学习行为分析系统 - 虚拟机一键部署脚本
# 适用于 CentOS 7 系统
# ============================================

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_step() { echo -e "${BLUE}[STEP]${NC} $1"; }

# 获取本机 IP
get_local_ip() {
    hostname -I | awk '{print $1}'
}

LOCAL_IP=$(get_local_ip)
PROJECT_DIR=$(cd "$(dirname "$0")" && pwd)

echo "============================================"
echo "  学习行为分析系统 - 一键部署"
echo "============================================"
echo "  项目目录: $PROJECT_DIR"
echo "  本机 IP:  $LOCAL_IP"
echo "============================================"
echo ""

# ==================== 1. 安装 Java ====================
log_step "1/6 检查 Java 环境..."
if ! command -v java &> /dev/null; then
    log_info "安装 OpenJDK 8..."
    yum install -y java-1.8.0-openjdk java-1.8.0-openjdk-devel -q
fi
log_info "Java 版本: $(java -version 2>&1 | head -1)"

# ==================== 2. 检查 Hadoop ====================
log_step "2/6 检查 Hadoop..."

# 优先检查系统已安装的 Hadoop
if command -v hdfs &> /dev/null; then
    HADOOP_HOME=$(dirname $(dirname $(which hdfs)))
    log_info "检测到系统 Hadoop: $HADOOP_HOME"
    log_info "版本: $(hadoop version 2>&1 | head -1)"
elif [ -d "/opt/software/hadoop" ]; then
    HADOOP_HOME="/opt/software/hadoop"
    log_info "检测到 Hadoop: $HADOOP_HOME"
elif [ -d "/opt/hadoop" ]; then
    HADOOP_HOME="/opt/hadoop"
    log_info "检测到 Hadoop: $HADOOP_HOME"
else
    log_error "Hadoop 未安装，请先安装 Hadoop"
    exit 1
fi

# ==================== 3. 检查 Spark ====================
log_step "3/6 检查 Spark..."

# 优先检查系统已安装的 Spark
if command -v spark-submit &> /dev/null; then
    SPARK_HOME=$(dirname $(dirname $(which spark-submit)))
    log_info "检测到系统 Spark: $SPARK_HOME"
elif [ -d "/opt/software/spark" ]; then
    SPARK_HOME="/opt/software/spark"
    log_info "检测到 Spark: $SPARK_HOME"
elif [ -d "/opt/spark" ]; then
    SPARK_HOME="/opt/spark"
    log_info "检测到 Spark: $SPARK_HOME"
else
    log_warn "Spark 未安装，跳过（Web 服务不依赖 Spark）"
    SPARK_HOME=""
fi

# ==================== 4. 配置环境 ====================
log_step "4/6 配置环境变量..."

# 获取 JAVA_HOME
if [ -z "$JAVA_HOME" ]; then
    JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
fi

# 环境变量写入用户目录（不需要 root 权限）
ENV_FILE="$HOME/.bashrc"
if ! grep -q "HADOOP_HOME" $ENV_FILE 2>/dev/null; then
    cat >> $ENV_FILE << EOF

# Hadoop 环境变量
export JAVA_HOME=$JAVA_HOME
export HADOOP_HOME=$HADOOP_HOME
export SPARK_HOME=$SPARK_HOME
export PATH=\$PATH:\$JAVA_HOME/bin:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$SPARK_HOME/bin
export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop
EOF
    log_info "环境变量已添加到 ~/.bashrc"
fi

# 加载环境变量
export JAVA_HOME=$JAVA_HOME
export HADOOP_HOME=$HADOOP_HOME
export SPARK_HOME=$SPARK_HOME
export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop

log_info "JAVA_HOME: $JAVA_HOME"
log_info "HADOOP_HOME: $HADOOP_HOME"
log_info "SPARK_HOME: $SPARK_HOME"

log_info "环境配置完成"

# ==================== 5. 启动 Hadoop ====================
log_step "5/6 检查 Hadoop 服务..."

# 检查 HDFS 是否已经在运行
if jps 2>/dev/null | grep -q "NameNode"; then
    log_info "HDFS 已在运行"
else
    log_warn "HDFS 未运行，尝试启动..."
    start-dfs.sh 2>/dev/null || true
    sleep 5
    
    if jps 2>/dev/null | grep -q "NameNode"; then
        log_info "HDFS 启动成功"
    else
        log_warn "HDFS 启动失败，请手动启动: start-dfs.sh"
    fi
fi

# 显示运行的 Java 进程
log_info "Java 进程:"
jps 2>/dev/null | grep -v Jps | while read line; do echo "    $line"; done

# 创建 HDFS 目录
log_info "创建 HDFS 目录..."
hdfs dfs -mkdir -p /user/learning_behavior/raw 2>/dev/null || true
hdfs dfs -mkdir -p /user/learning_behavior/results 2>/dev/null || true
hdfs dfs -chmod -R 777 /user/learning_behavior 2>/dev/null || true
log_info "HDFS 目录已创建"

# ==================== 6. 启动 Web 服务 ====================
log_step "6/6 启动 Web 服务..."

# 检查 Python
PYTHON_CMD=""
if command -v python3 &> /dev/null; then
    PYTHON_CMD="python3"
elif command -v python &> /dev/null; then
    PYTHON_CMD="python"
else
    log_error "Python 未安装"
    exit 1
fi
log_info "使用 Python: $PYTHON_CMD"

# 安装 Flask（如果没有）
$PYTHON_CMD -c "import flask" 2>/dev/null || {
    log_info "安装 Flask..."
    pip3 install flask requests -q 2>/dev/null || pip install flask requests -q 2>/dev/null || {
        log_error "Flask 安装失败，请手动安装: pip install flask requests"
        exit 1
    }
}

# 停止旧进程
pkill -f "python.*app.py" 2>/dev/null || true
sleep 2

# 创建日志目录
mkdir -p $PROJECT_DIR/logs/web

# 获取 HDFS NameNode 地址
HDFS_HOST=$(hdfs getconf -confKey dfs.namenode.http-address 2>/dev/null | cut -d: -f1 || echo "localhost")
HDFS_PORT=$(hdfs getconf -confKey dfs.namenode.http-address 2>/dev/null | cut -d: -f2 || echo "9870")
log_info "HDFS WebHDFS: $HDFS_HOST:$HDFS_PORT"

# 启动 Web 服务
cd $PROJECT_DIR
export HDFS_HOST=$HDFS_HOST
export HDFS_PORT=$HDFS_PORT
nohup $PYTHON_CMD src/web/app.py > logs/web/app.log 2>&1 &

sleep 3

if pgrep -f "python.*app.py" > /dev/null; then
    log_info "Web 服务启动成功"
else
    log_error "Web 服务启动失败，查看日志:"
    cat logs/web/app.log | tail -20
    exit 1
fi

# ==================== 完成 ====================
echo ""
echo "============================================"
echo -e "  ${GREEN}部署完成!${NC}"
echo "============================================"
echo ""
echo "  运行进程:"
jps | grep -v Jps | while read line; do echo "    - $line"; done
echo "    - Web App (Flask)"
echo ""
echo "  访问地址:"
echo "    - Web 可视化:  http://${LOCAL_IP}:5000"
echo "    - HDFS Web UI: http://${LOCAL_IP}:9870"
echo ""
echo "  默认登录: admin / admin123"
echo ""
echo "  常用命令:"
echo "    - 查看日志: tail -f $PROJECT_DIR/logs/web/app.log"
echo "    - 停止服务: $PROJECT_DIR/stop.sh"
echo "    - 查看HDFS: hdfs dfs -ls /user/learning_behavior/raw"
echo "============================================"
