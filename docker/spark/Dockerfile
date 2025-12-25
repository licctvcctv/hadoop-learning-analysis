# ============================================
# Spark镜像
# 包含Spark 3.4.0 + PySpark
# ============================================
FROM eclipse-temurin:8-jdk

LABEL maintainer="Learning Behavior Analysis System"
LABEL description="Spark 3.4.0 with PySpark and Hive Support"

# 设置环境变量
ENV HADOOP_VERSION=3.3.6
ENV SPARK_VERSION=3.4.0
ENV HADOOP_HOME=/opt/hadoop
ENV SPARK_HOME=/opt/spark
ENV PATH=${PATH}:${HADOOP_HOME}/bin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin
ENV JAVA_HOME=/opt/java/openjdk
ENV HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
ENV SPARK_CONF_DIR=${SPARK_HOME}/conf
ENV PYSPARK_PYTHON=python3
ENV PYTHONPATH=${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip

# 安装必要的工具和Python
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    wget \
    procps \
    net-tools \
    netcat-openbsd \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# 安装Python依赖
RUN pip3 install --no-cache-dir --break-system-packages \
    pyspark==${SPARK_VERSION} \
    pyhive \
    thrift \
    pandas \
    numpy

# 下载并安装Hadoop（用于客户端工具）
RUN wget -q https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar -xzf hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} ${HADOOP_HOME} && \
    rm hadoop-${HADOOP_VERSION}.tar.gz

# 下载并安装Spark
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

# 创建日志目录
RUN mkdir -p /opt/spark/logs

# 设置JAVA_HOME
RUN echo "export JAVA_HOME=${JAVA_HOME}" >> ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh

# 复制启动脚本
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# 暴露端口
# Spark Master
EXPOSE 8080 7077
# Spark Worker
EXPOSE 8081
# Spark History Server
EXPOSE 18080

ENTRYPOINT ["/entrypoint.sh"]
