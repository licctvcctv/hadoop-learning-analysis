-- ============================================
-- DWD层表创建脚本
-- 明细数据层 - 清洗和转换后的学习行为数据
-- ============================================

USE learning_behavior;

-- 删除已存在的表
DROP TABLE IF EXISTS dwd_learning_behavior;

-- 创建DWD层明细数据表
CREATE TABLE dwd_learning_behavior (
    student_id STRING COMMENT '学生ID',
    student_name STRING COMMENT '学生姓名',
    course_id STRING COMMENT '课程ID',
    course_name STRING COMMENT '课程名称',
    behavior_type STRING COMMENT '行为类型',
    duration INT COMMENT '持续时长(秒)',
    behavior_time TIMESTAMP COMMENT '行为时间',
    behavior_date STRING COMMENT '行为日期 yyyy-MM-dd',
    behavior_hour INT COMMENT '行为小时 0-23',
    behavior_weekday INT COMMENT '星期几 1-7 (1=周日, 2=周一, ..., 7=周六, 使用DAYOFWEEK函数)',
    behavior_period STRING COMMENT '时间段: morning/afternoon/evening/night',
    ip_address STRING COMMENT 'IP地址',
    device_type STRING COMMENT '设备类型',
    chapter_id STRING COMMENT '章节ID',
    score DOUBLE COMMENT '得分',
    is_valid INT COMMENT '是否有效记录 1=有效 0=无效',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT 'DWD层-学习行为明细数据表'
PARTITIONED BY (dt STRING COMMENT '日期分区')
STORED AS PARQUET
LOCATION '/user/learning_behavior/dwd/'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);

-- 显示表结构
DESCRIBE FORMATTED dwd_learning_behavior;
