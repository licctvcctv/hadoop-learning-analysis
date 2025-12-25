# ============================================
# Hive镜像
# 包含Hive 3.1.3 + MySQL连接器
# ============================================
FROM eclipse-temurin:8-jdk

LABEL maintainer="Learning Behavior Analysis System"
LABEL description="Hive 3.1.3 with MySQL Metastore"

# 设置环境变量
ENV HADOOP_VERSION=3.3.6
ENV HIVE_VERSION=3.1.3
ENV HADOOP_HOME=/opt/hadoop
ENV HIVE_HOME=/opt/hive
ENV PATH=${PATH}:${HADOOP_HOME}/bin:${HIVE_HOME}/bin
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

# 下载并安装Hadoop（用于客户端工具）
RUN wget -q https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar -xzf hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} ${HADOOP_HOME} && \
    rm hadoop-${HADOOP_VERSION}.tar.gz

# 下载并安装Hive
RUN wget -q https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz && \
    tar -xzf apache-hive-${HIVE_VERSION}-bin.tar.gz && \
    mv apache-hive-${HIVE_VERSION}-bin ${HIVE_HOME} && \
    rm apache-hive-${HIVE_VERSION}-bin.tar.gz

# 下载MySQL JDBC驱动
RUN wget -q https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar && \
    mv mysql-connector-java-8.0.28.jar ${HIVE_HOME}/lib/

# 解决Guava版本冲突
RUN rm -f ${HIVE_HOME}/lib/guava-19.0.jar && \
    cp ${HADOOP_HOME}/share/hadoop/common/lib/guava-27.0-jre.jar ${HIVE_HOME}/lib/

# 创建日志目录
RUN mkdir -p /opt/hive/logs

# 设置JAVA_HOME
RUN echo "export JAVA_HOME=${JAVA_HOME}" >> ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh

# 复制启动脚本
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# 复制Hive配置
COPY hive-site.xml ${HIVE_HOME}/conf/

# 复制Hadoop配置
COPY core-site.xml ${HADOOP_HOME}/etc/hadoop/
COPY hdfs-site.xml ${HADOOP_HOME}/etc/hadoop/

# 暴露端口
# HiveServer2
EXPOSE 10000 10002
# Hive Metastore
EXPOSE 9083

ENTRYPOINT ["/entrypoint.sh"]
