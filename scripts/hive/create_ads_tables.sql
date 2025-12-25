-- ============================================
-- ADS层表创建脚本
-- 应用数据层 - 面向应用的数据表
-- ============================================

USE learning_behavior;

-- ==================== 学习预警表 ====================
-- 与 abnormal_detection.py 的 all_alerts 输出对齐
DROP TABLE IF EXISTS ads_learning_alerts;

CREATE TABLE ads_learning_alerts (
    alert_id BIGINT COMMENT '预警ID',
    student_id STRING COMMENT '学生ID',
    student_name STRING COMMENT '学生姓名',
    alert_type STRING COMMENT '预警类型: 长时间未学习/学习时长过低/成绩异常',
    alert_level STRING COMMENT '预警级别: 高/中/低',
    alert_message STRING COMMENT '预警信息',
    last_activity_date DATE COMMENT '最后活动日期',
    inactive_days INT COMMENT '未活跃天数',
    alert_time TIMESTAMP COMMENT '预警时间',
    is_handled INT COMMENT '是否已处理 0=未处理 1=已处理'
)
COMMENT 'ADS层-学习预警表'
PARTITIONED BY (dt DATE COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/user/learning_behavior/ads/learning_alerts/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- ==================== 学习排行榜表 ====================
DROP TABLE IF EXISTS ads_learning_ranking;

CREATE TABLE ads_learning_ranking (
    rank_type STRING COMMENT '排行类型: duration/score/activity',
    ranking INT COMMENT '排名',
    student_id STRING COMMENT '学生ID',
    student_name STRING COMMENT '学生姓名',
    metric_value DOUBLE COMMENT '指标值',
    metric_unit STRING COMMENT '指标单位',
    update_time TIMESTAMP COMMENT '更新时间'
)
COMMENT 'ADS层-学习排行榜表'
PARTITIONED BY (dt DATE COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/user/learning_behavior/ads/learning_ranking/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- ==================== 数据概览表 ====================
DROP TABLE IF EXISTS ads_data_summary;

CREATE TABLE ads_data_summary (
    total_students INT COMMENT '学生总数',
    total_courses INT COMMENT '课程总数',
    total_behaviors BIGINT COMMENT '行为记录总数',
    total_study_hours DOUBLE COMMENT '总学习时长(小时)',
    avg_study_hours DOUBLE COMMENT '人均学习时长(小时)',
    active_rate DOUBLE COMMENT '活跃率(%)',
    update_time TIMESTAMP COMMENT '更新时间'
)
COMMENT 'ADS层-数据概览表'
PARTITIONED BY (dt DATE COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/user/learning_behavior/ads/data_summary/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- 显示所有ADS表
SHOW TABLES LIKE 'ads_*';
