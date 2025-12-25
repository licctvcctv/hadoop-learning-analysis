# 基于Hadoop的大学生线上课程学习行为数据存储与分析系统

## 项目简介

本系统是一个基于Hadoop生态圈的大数据分析平台，用于收集、存储和分析大学生在线学习平台的行为数据。系统采用Docker容器化部署，支持一键启动完整的大数据分析环境。

## 技术栈

- **容器化**: Docker + Docker Compose
- **分布式存储**: Hadoop HDFS 3.3.6
- **资源调度**: YARN
- **数据采集**: Flume 1.11.0
- **数据仓库**: Hive 3.1.3
- **计算引擎**: Spark 3.4.0
- **可视化**: Flask + ECharts
- **编程语言**: Python, Shell, SQL

## 目录结构

```
hadoop-learning-analysis/
├── docker/                    # Docker相关文件
│   ├── hadoop/               # Hadoop镜像配置
│   ├── hive/                 # Hive镜像配置
│   ├── spark/                # Spark镜像配置
│   ├── flume/                # Flume镜像配置
│   └── web/                  # Web应用镜像配置
├── config/                    # 配置文件
│   ├── hadoop/               # Hadoop配置文件
│   ├── hive/                 # Hive配置文件
│   ├── spark/                # Spark配置文件
│   └── flume/                # Flume配置文件
├── scripts/                   # 脚本文件
│   ├── hive/                 # Hive SQL脚本
│   └── init/                 # 初始化脚本
├── src/                       # 源代码
│   ├── data_generator/       # 数据生成器
│   ├── spark_analysis/       # Spark分析任务
│   └── web/                  # Web应用
├── data/                      # 数据目录
│   └── logs/                 # 生成的日志数据
├── logs/                      # 系统日志目录
│   ├── hadoop/               # Hadoop日志
│   ├── hive/                 # Hive日志
│   ├── spark/                # Spark日志
│   ├── flume/                # Flume日志
│   └── web/                  # Web应用日志
├── docker-compose.yml         # Docker编排文件
├── deploy.sh                  # 一键部署脚本
├── stop.sh                    # 停止脚本
├── .env                       # 环境变量配置
└── README.md                  # 项目说明
```

## 快速开始

### 前置要求

- Docker 20.10+
- Docker Compose 2.0+
- 至少 8GB 可用内存
- 至少 20GB 可用磁盘空间

### 一键部署

```bash
# 克隆项目
git clone <repository-url>
cd hadoop-learning-analysis

# 启动系统
./deploy.sh

# 停止系统
./stop.sh
```

### 访问地址

| 服务 | 地址 | 说明 |
|------|------|------|
| HDFS NameNode | http://localhost:9870 | HDFS管理界面 |
| YARN ResourceManager | http://localhost:8088 | YARN资源管理 |
| Spark Master | http://localhost:8080 | Spark管理界面 |
| Hive Server | localhost:10000 | Hive JDBC连接 |
| Web可视化 | http://localhost:5000 | 数据可视化界面 |

## 功能模块

### 1. 数据采集
- 模拟生成大学生学习行为数据
- 支持多种行为类型：视频观看、作业提交、测验完成等
- 通过Flume实时采集到HDFS

### 2. 数据存储
- HDFS分布式存储
- 按日期分区存储
- 数据副本保证可靠性

### 3. 数据仓库
- ODS层：原始数据
- DWD层：明细数据
- DWS层：汇总统计
- ADS层：应用数据

### 4. 数据分析
- 学习时长统计
- 课程热度分析
- 学习行为模式分析
- 学习异常检测

### 5. 数据可视化
- 学习时长排行榜
- 课程热度图表
- 学习时间分布图
- 学习预警列表

## 日志查看

所有组件日志统一输出到 `./logs/` 目录：

```bash
# 查看Hadoop日志
tail -f logs/hadoop/namenode/*.log

# 查看Spark日志
tail -f logs/spark/spark-master.log

# 查看Flume日志
tail -f logs/flume/flume-agent.log

# 查看Web应用日志
tail -f logs/web/flask-app.log
```

也可以通过Web界面查看日志：http://localhost:5000/logs

## 常见问题

### Q: 容器启动失败怎么办？
A: 检查Docker资源配置，确保有足够的内存和磁盘空间。查看 `logs/` 目录下的日志文件定位问题。

### Q: HDFS无法访问？
A: 等待NameNode完全启动（约1-2分钟），检查 `logs/hadoop/namenode/` 下的日志。

### Q: Hive查询报错？
A: 确保MySQL元数据库已初始化，检查 `logs/hive/` 下的日志。

## 许可证

MIT License
