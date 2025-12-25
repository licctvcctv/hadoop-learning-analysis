# ============================================
# Flume镜像
# 包含Flume 1.11.0
# ============================================
FROM eclipse-temurin:8-jdk

LABEL maintainer="Learning Behavior Analysis System"
LABEL description="Flume 1.11.0 for Log Collection"

# 设置环境变量
ENV HADOOP_VERSION=3.3.6
ENV FLUME_VERSION=1.11.0
ENV HADOOP_HOME=/opt/hadoop
ENV FLUME_HOME=/opt/flume
ENV PATH=${PATH}:${HADOOP_HOME}/bin:${FLUME_HOME}/bin
ENV JAVA_HOME=/opt/java/openjdk
ENV HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop

# 安装必要的工具
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    wget \
    procps \
    net-tools \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# 下载并安装Hadoop（用于HDFS客户端）
RUN wget -q https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar -xzf hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} ${HADOOP_HOME} && \
    rm hadoop-${HADOOP_VERSION}.tar.gz

# 下载并安装Flume
RUN wget -q https://archive.apache.org/dist/flume/${FLUME_VERSION}/apache-flume-${FLUME_VERSION}-bin.tar.gz && \
    tar -xzf apache-flume-${FLUME_VERSION}-bin.tar.gz && \
    mv apache-flume-${FLUME_VERSION}-bin ${FLUME_HOME} && \
    rm apache-flume-${FLUME_VERSION}-bin.tar.gz

# 创建目录
RUN mkdir -p /data/logs && \
    mkdir -p /opt/flume/logs && \
    mkdir -p /data/flume/checkpoint && \
    mkdir -p /data/flume/data

# 设置JAVA_HOME
RUN echo "export JAVA_HOME=${JAVA_HOME}" >> ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh
RUN echo "export JAVA_HOME=${JAVA_HOME}" >> ${FLUME_HOME}/conf/flume-env.sh

# 复制启动脚本
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
