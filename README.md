# 基于Hadoop的大学生线上课程学习行为数据存储与分析平台

## 项目简介

本项目是一个基于Hadoop生态圈的大数据分析平台，用于存储和分析大学生线上课程学习行为数据。

## 技术架构

### Hadoop生态圈组件
- **HDFS**: 分布式文件系统，用于存储学习行为数据
  - 1个NameNode + 2个DataNode
- **YARN**: 资源调度管理
  - 1个ResourceManager + 2个NodeManager  
- **Spark**: 分布式计算引擎，用于数据分析
  - 1个Master + 2个Worker
- **Flask**: Web可视化界面

### 集群架构
```
┌─────────────────────────────────────────────────────────────┐
│                    Docker Compose 集群                       │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────┐    ┌──────────┐    ┌──────────┐              │
│  │ NameNode │    │DataNode-1│    │DataNode-2│              │
│  │  (HDFS)  │    │  (HDFS)  │    │  (HDFS)  │              │
│  │ Port:9870│    │          │    │          │              │
│  └──────────┘    └──────────┘    └──────────┘              │
│                                                             │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐              │
│  │ Resource │    │  Node    │    │  Node    │              │
│  │ Manager  │    │Manager-1 │    │Manager-2 │              │
│  │ Port:8088│    │          │    │          │              │
│  └──────────┘    └──────────┘    └──────────┘              │
│                                                             │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐              │
│  │  Spark   │    │  Spark   │    │  Spark   │              │
│  │  Master  │    │ Worker-1 │    │ Worker-2 │              │
│  │ Port:8080│    │          │    │          │              │
│  └──────────┘    └──────────┘    └──────────┘              │
│                                                             │
│  ┌──────────┐                                               │
│  │Flask Web │                                               │
│  │ Port:5000│                                               │
│  └──────────┘                                               │
└─────────────────────────────────────────────────────────────┘
```

## 功能模块

### 1. 数据存储 (HDFS)
- 学习行为数据以JSON格式存储在HDFS
- 存储路径: `/user/learning_behavior/raw/`
- 支持分布式存储，数据自动复制到多个DataNode

### 2. 数据分析 (Spark)
- 学生学习时长统计与排行
- 课程热度分析
- 学习行为类型分布
- 学习时间分布分析
- 学习预警检测

### 3. 数据可视化 (Flask)
- 实时数据概览
- 图表展示（ECharts）
- 系统监控面板
- 日志查看

## 快速部署

### 环境要求
- Docker
- Docker Compose

### 启动集群
```bash
cd hadoop-learning-analysis
docker-compose -f docker-compose-cluster.yml up -d
```

### 访问地址
- Web可视化: http://localhost:5000
- HDFS Web UI: http://localhost:9870
- YARN Web UI: http://localhost:8088
- Spark Web UI: http://localhost:8080

## 目录结构
```
hadoop-learning-analysis/
├── docker-compose-cluster.yml  # Docker编排文件
├── hadoop.env                  # Hadoop环境变量
├── src/
│   ├── spark_analysis/         # Spark分析脚本
│   │   ├── generate_data.py    # 数据生成
│   │   └── analyze_data.py     # 数据分析
│   └── web/                    # Web应用
│       ├── app.py              # Flask主程序
│       └── templates/          # HTML模板
├── config/                     # 配置文件
└── scripts/                    # 脚本文件
```

## 数据流程

1. **数据生成**: Web应用后台自动生成模拟学习行为数据
2. **数据存储**: 数据通过WebHDFS API写入HDFS
3. **数据分析**: Spark从HDFS读取数据进行分析
4. **结果展示**: Flask读取分析结果并展示

## 运行Spark分析任务

```bash
# 进入spark-master容器
docker exec -it spark-master bash

# 运行数据生成
/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/generate_data.py

# 运行数据分析
/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/analyze_data.py
```
