# ============================================
# Hadoop基础镜像
# 包含Hadoop 3.3.6完整安装
# ============================================
FROM eclipse-temurin:8-jdk

LABEL maintainer="Learning Behavior Analysis System"
LABEL description="Hadoop 3.3.6 Base Image"

# 设置环境变量
ENV HADOOP_VERSION=3.3.6
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
ENV PATH=${PATH}:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin
ENV JAVA_HOME=/opt/java/openjdk
ENV HDFS_NAMENODE_USER=root
ENV HDFS_DATANODE_USER=root
ENV HDFS_SECONDARYNAMENODE_USER=root
ENV YARN_RESOURCEMANAGER_USER=root
ENV YARN_NODEMANAGER_USER=root

# 安装必要的工具
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    wget \
    ssh \
    rsync \
    procps \
    net-tools \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# 配置SSH免密登录
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys

# 创建SSH配置
RUN echo "Host *" >> ~/.ssh/config && \
    echo "  StrictHostKeyChecking no" >> ~/.ssh/config && \
    echo "  UserKnownHostsFile=/dev/null" >> ~/.ssh/config && \
    chmod 0600 ~/.ssh/config

# 下载并安装Hadoop
RUN wget -q https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar -xzf hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} ${HADOOP_HOME} && \
    rm hadoop-${HADOOP_VERSION}.tar.gz

# 创建数据目录
RUN mkdir -p /data/hdfs/namenode && \
    mkdir -p /data/hdfs/datanode && \
    mkdir -p /opt/hadoop/logs

# 设置JAVA_HOME到hadoop-env.sh
RUN echo "export JAVA_HOME=${JAVA_HOME}" >> ${HADOOP_CONF_DIR}/hadoop-env.sh

# 复制启动脚本
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# 暴露端口
# HDFS NameNode
EXPOSE 9870 9000
# HDFS DataNode
EXPOSE 9864 9866 9867
# YARN ResourceManager
EXPOSE 8088 8030 8031 8032 8033
# YARN NodeManager
EXPOSE 8040 8042
# MapReduce JobHistory
EXPOSE 19888

ENTRYPOINT ["/entrypoint.sh"]
