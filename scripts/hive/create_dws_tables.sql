-- ============================================
-- DWS层表创建脚本
-- 汇总数据层 - 按维度汇总的统计数据
-- ============================================

USE learning_behavior;

-- ==================== 学生学习汇总表 ====================
DROP TABLE IF EXISTS dws_student_learning_summary;

CREATE TABLE dws_student_learning_summary (
    student_id STRING COMMENT '学生ID',
    student_name STRING COMMENT '学生姓名',
    total_study_duration INT COMMENT '总学习时长(秒)',
    total_study_hours DOUBLE COMMENT '总学习时长(小时)',
    total_courses INT COMMENT '学习课程数',
    video_watch_count INT COMMENT '视频观看次数',
    video_watch_duration INT COMMENT '视频观看总时长(秒)',
    homework_submit_count INT COMMENT '作业提交次数',
    quiz_complete_count INT COMMENT '测验完成次数',
    forum_post_count INT COMMENT '论坛发帖次数',
    material_download_count INT COMMENT '资料下载次数',
    total_behavior_count INT COMMENT '总行为次数',
    avg_score DOUBLE COMMENT '平均得分',
    max_score DOUBLE COMMENT '最高得分',
    min_score DOUBLE COMMENT '最低得分',
    first_study_time TIMESTAMP COMMENT '首次学习时间',
    last_study_time TIMESTAMP COMMENT '最后学习时间',
    active_days INT COMMENT '活跃天数',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT 'DWS层-学生学习汇总表'
PARTITIONED BY (dt DATE COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/user/learning_behavior/dws/student_summary/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- ==================== 课程热度汇总表 ====================
DROP TABLE IF EXISTS dws_course_popularity;

CREATE TABLE dws_course_popularity (
    course_id STRING COMMENT '课程ID',
    course_name STRING COMMENT '课程名称',
    student_count INT COMMENT '学习人数',
    total_duration INT COMMENT '总学习时长(秒)',
    total_hours DOUBLE COMMENT '总学习时长(小时)',
    avg_duration DOUBLE COMMENT '人均学习时长(秒)',
    avg_hours DOUBLE COMMENT '人均学习时长(小时)',
    behavior_count INT COMMENT '行为总数',
    video_watch_count INT COMMENT '视频观看次数',
    homework_submit_count INT COMMENT '作业提交次数',
    quiz_complete_count INT COMMENT '测验完成次数',
    avg_score DOUBLE COMMENT '平均得分',
    popularity_rank INT COMMENT '热度排名',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT 'DWS层-课程热度汇总表'
PARTITIONED BY (dt DATE COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/user/learning_behavior/dws/course_popularity/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- ==================== 按小时学习分布表 ====================
-- 与 behavior_pattern.py 的 hourly_stats 输出对齐
DROP TABLE IF EXISTS dws_hourly_distribution;

CREATE TABLE dws_hourly_distribution (
    hour INT COMMENT '小时(0-23)',
    behavior_count BIGINT COMMENT '行为次数',
    student_count BIGINT COMMENT '学生人数',
    total_duration BIGINT COMMENT '总学习时长(秒)',
    avg_duration DOUBLE COMMENT '平均学习时长(秒)',
    time_period STRING COMMENT '时间段描述(上午/中午/下午/晚上/深夜)',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT 'DWS层-按小时学习分布表'
PARTITIONED BY (dt DATE COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/user/learning_behavior/dws/hourly_distribution/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- ==================== 按星期学习分布表 ====================
-- 与 behavior_pattern.py 的 weekly_stats 输出对齐
DROP TABLE IF EXISTS dws_weekly_distribution;

CREATE TABLE dws_weekly_distribution (
    day_of_week INT COMMENT '星期几(1=周日,2=周一,...,7=周六)',
    week_name STRING COMMENT '星期名称',
    behavior_count BIGINT COMMENT '行为次数',
    student_count BIGINT COMMENT '学生人数',
    total_duration BIGINT COMMENT '总学习时长(秒)',
    avg_duration DOUBLE COMMENT '平均学习时长(秒)',
    is_weekend INT COMMENT '是否周末(0=否,1=是)',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT 'DWS层-按星期学习分布表'
PARTITIONED BY (dt DATE COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/user/learning_behavior/dws/weekly_distribution/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- ==================== 行为类型分布表 ====================
-- 与 behavior_pattern.py 的 behavior_stats 输出对齐
DROP TABLE IF EXISTS dws_behavior_distribution;

CREATE TABLE dws_behavior_distribution (
    behavior_type STRING COMMENT '行为类型',
    behavior_count BIGINT COMMENT '行为次数',
    student_count BIGINT COMMENT '学生人数',
    course_count BIGINT COMMENT '课程数',
    total_duration BIGINT COMMENT '总时长(秒)',
    avg_duration DOUBLE COMMENT '平均时长(秒)',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT 'DWS层-行为类型分布表'
PARTITIONED BY (dt DATE COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/user/learning_behavior/dws/behavior_distribution/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- ==================== 学习时间分布表 (ETL使用) ====================
-- 与 etl_load.sql 的 INSERT INTO dws_study_time_distribution 对齐
DROP TABLE IF EXISTS dws_study_time_distribution;

CREATE TABLE dws_study_time_distribution (
    hour_of_day INT COMMENT '小时(0-23)',
    behavior_count BIGINT COMMENT '行为次数',
    student_count BIGINT COMMENT '学生人数',
    total_duration BIGINT COMMENT '总学习时长(秒)',
    avg_duration DOUBLE COMMENT '平均学习时长(秒)',
    time_period STRING COMMENT '时间段描述',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT 'DWS层-学习时间分布表(ETL)'
PARTITIONED BY (dt DATE COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/user/learning_behavior/dws/study_time_distribution/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- ==================== 行为类型分布表 (ETL使用) ====================
-- 与 etl_load.sql 的 INSERT INTO dws_behavior_type_distribution 对齐
DROP TABLE IF EXISTS dws_behavior_type_distribution;

CREATE TABLE dws_behavior_type_distribution (
    behavior_type STRING COMMENT '行为类型',
    behavior_type_name STRING COMMENT '行为类型名称',
    behavior_count BIGINT COMMENT '行为次数',
    student_count BIGINT COMMENT '学生人数',
    total_duration BIGINT COMMENT '总时长(秒)',
    avg_duration DOUBLE COMMENT '平均时长(秒)',
    percentage DOUBLE COMMENT '占比(%)',
    etl_time TIMESTAMP COMMENT 'ETL处理时间'
)
COMMENT 'DWS层-行为类型分布表(ETL)'
PARTITIONED BY (dt DATE COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/user/learning_behavior/dws/behavior_type_distribution/'
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

-- 显示所有DWS表
SHOW TABLES LIKE 'dws_*';
