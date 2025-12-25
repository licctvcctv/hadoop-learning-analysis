#!/usr/bin/env bash
# ============================================
# Spark环境配置
# 基于Hadoop的大学生线上课程学习行为数据存储与分析系统
# ============================================

export JAVA_HOME=/usr/local/openjdk-8
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export SPARK_HOME=/opt/spark
export SPARK_CONF_DIR=/opt/spark/conf

# Spark Master配置
export SPARK_MASTER_HOST=spark-master
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080

# Spark Worker配置
export SPARK_WORKER_CORES=2
export SPARK_WORKER_MEMORY=2g
export SPARK_WORKER_WEBUI_PORT=8081

# 日志配置
export SPARK_LOG_DIR=/opt/spark/logs

# Python配置
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
