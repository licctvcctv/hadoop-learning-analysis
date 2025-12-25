-- ============================================
-- ODS层表创建脚本
-- 原始数据层 - 存储从Flume采集的原始学习行为日志
-- ============================================

-- 创建数据库
CREATE DATABASE IF NOT EXISTS learning_behavior
COMMENT '大学生线上课程学习行为数据仓库'
LOCATION '/user/learning_behavior/';

USE learning_behavior;

-- 删除已存在的表（开发环境使用）
DROP TABLE IF EXISTS ods_learning_behavior;

-- 创建ODS层原始数据表
-- 外部表，关联HDFS上的原始日志数据
-- 路径与 Flume 配置的 hdfs.path 对齐: /user/learning_behavior/raw/
CREATE EXTERNAL TABLE ods_learning_behavior (
    student_id STRING COMMENT '学生ID',
    student_name STRING COMMENT '学生姓名',
    course_id STRING COMMENT '课程ID',
    course_name STRING COMMENT '课程名称',
    behavior_type STRING COMMENT '行为类型: video_watch/homework_submit/quiz_complete/forum_post/material_download',
    duration INT COMMENT '持续时长(秒)',
    `timestamp` STRING COMMENT '行为时间',
    ip_address STRING COMMENT 'IP地址',
    device_type STRING COMMENT '设备类型: PC/Mobile/Tablet',
    chapter_id STRING COMMENT '章节ID',
    score DOUBLE COMMENT '得分(仅适用于测验和作业)'
)
COMMENT 'ODS层-学习行为原始数据表'
PARTITIONED BY (
    year STRING COMMENT '年份',
    month STRING COMMENT '月份',
    day STRING COMMENT '日期'
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/user/learning_behavior/raw/'
TBLPROPERTIES (
    'creator' = 'learning_behavior_system',
    'create_time' = '2024-12-20'
);

-- 显示表结构
DESCRIBE FORMATTED ods_learning_behavior;

-- 修复分区（自动发现Flume写入的分区）
-- 注意：此命令需要在数据写入后执行
MSCK REPAIR TABLE ods_learning_behavior;

SHOW TABLES;
