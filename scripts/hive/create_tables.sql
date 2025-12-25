-- 创建数据库
CREATE DATABASE IF NOT EXISTS learning_behavior;
USE learning_behavior;

-- ODS层：原始数据表
CREATE TABLE IF NOT EXISTS ods_learning_behavior (
    student_id STRING,
    student_name STRING,
    course_id STRING,
    course_name STRING,
    behavior_type STRING,
    duration INT,
    timestamp STRING,
    ip_address STRING,
    device_type STRING,
    chapter_id STRING,
    score DOUBLE
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE;

-- 加载数据
LOAD DATA INPATH '/user/learning_behavior/raw/learning_behavior_20251225_173217.json' 
INTO TABLE ods_learning_behavior;

-- DWD层：明细数据表
CREATE TABLE IF NOT EXISTS dwd_learning_behavior (
    student_id STRING,
    student_name STRING,
    course_id STRING,
    course_name STRING,
    behavior_type STRING,
    duration INT,
    behavior_time STRING,
    behavior_date STRING,
    behavior_hour INT,
    ip_address STRING,
    device_type STRING,
    chapter_id STRING,
    score DOUBLE,
    is_valid INT
)
STORED AS PARQUET;

-- 插入DWD数据
INSERT OVERWRITE TABLE dwd_learning_behavior
SELECT 
    student_id,
    student_name,
    course_id,
    course_name,
    behavior_type,
    duration,
    timestamp as behavior_time,
    substr(timestamp, 1, 10) as behavior_date,
    cast(substr(timestamp, 12, 2) as int) as behavior_hour,
    ip_address,
    device_type,
    chapter_id,
    score,
    1 as is_valid
FROM ods_learning_behavior
WHERE student_id IS NOT NULL;

-- DWS层：学生学习汇总表
CREATE TABLE IF NOT EXISTS dws_student_learning_summary (
    student_id STRING,
    student_name STRING,
    total_study_duration BIGINT,
    total_study_hours DOUBLE,
    total_courses INT,
    video_watch_count BIGINT,
    video_watch_duration BIGINT,
    homework_submit_count BIGINT,
    quiz_complete_count BIGINT,
    forum_post_count BIGINT,
    material_download_count BIGINT,
    total_behavior_count BIGINT,
    avg_score DOUBLE,
    max_score DOUBLE,
    min_score DOUBLE,
    first_study_time STRING,
    last_study_time STRING,
    active_days BIGINT,
    etl_time STRING,
    dt STRING
)
STORED AS PARQUET;

-- 插入学生汇总数据
INSERT OVERWRITE TABLE dws_student_learning_summary
SELECT 
    student_id,
    max(student_name) as student_name,
    sum(duration) as total_study_duration,
    round(sum(duration) / 3600.0, 2) as total_study_hours,
    count(distinct course_id) as total_courses,
    sum(case when behavior_type = 'video_watch' then 1 else 0 end) as video_watch_count,
    sum(case when behavior_type = 'video_watch' then duration else 0 end) as video_watch_duration,
    sum(case when behavior_type = 'homework_submit' then 1 else 0 end) as homework_submit_count,
    sum(case when behavior_type = 'quiz_complete' then 1 else 0 end) as quiz_complete_count,
    sum(case when behavior_type = 'forum_post' then 1 else 0 end) as forum_post_count,
    sum(case when behavior_type = 'material_download' then 1 else 0 end) as material_download_count,
    count(*) as total_behavior_count,
    round(avg(score), 2) as avg_score,
    max(score) as max_score,
    min(score) as min_score,
    min(behavior_time) as first_study_time,
    max(behavior_time) as last_study_time,
    count(distinct behavior_date) as active_days,
    current_timestamp() as etl_time,
    current_date() as dt
FROM dwd_learning_behavior
WHERE is_valid = 1
GROUP BY student_id;

-- DWS层：课程热度表
CREATE TABLE IF NOT EXISTS dws_course_popularity (
    course_id STRING,
    course_name STRING,
    student_count BIGINT,
    total_hours DOUBLE,
    total_behaviors BIGINT
)
STORED AS PARQUET;

-- 插入课程热度数据
INSERT OVERWRITE TABLE dws_course_popularity
SELECT 
    course_id,
    max(course_name) as course_name,
    count(distinct student_id) as student_count,
    round(sum(duration) / 3600.0, 2) as total_hours,
    count(*) as total_behaviors
FROM dwd_learning_behavior
WHERE is_valid = 1
GROUP BY course_id;

-- DWS层：小时分布表
CREATE TABLE IF NOT EXISTS dws_hourly_distribution (
    hour INT,
    behavior_count BIGINT
)
STORED AS PARQUET;

-- 插入小时分布数据
INSERT OVERWRITE TABLE dws_hourly_distribution
SELECT 
    behavior_hour as hour,
    count(*) as behavior_count
FROM dwd_learning_behavior
WHERE is_valid = 1
GROUP BY behavior_hour;

-- DWS层：行为类型分布表
CREATE TABLE IF NOT EXISTS dws_behavior_distribution (
    behavior_type STRING,
    behavior_count BIGINT
)
STORED AS PARQUET;

-- 插入行为分布数据
INSERT OVERWRITE TABLE dws_behavior_distribution
SELECT 
    behavior_type,
    count(*) as behavior_count
FROM dwd_learning_behavior
WHERE is_valid = 1
GROUP BY behavior_type;

-- ADS层：学习预警表
CREATE TABLE IF NOT EXISTS ads_learning_alerts (
    student_id STRING,
    student_name STRING,
    alert_type STRING,
    alert_level STRING,
    alert_message STRING,
    alert_time STRING
)
STORED AS PARQUET;

-- 插入预警数据（低活跃度学生）
INSERT OVERWRITE TABLE ads_learning_alerts
SELECT 
    student_id,
    student_name,
    'low_engagement' as alert_type,
    CASE 
        WHEN total_study_hours < 5 THEN 'high'
        WHEN total_study_hours < 10 THEN 'medium'
        ELSE 'low'
    END as alert_level,
    concat('学习时长仅 ', round(total_study_hours, 1), ' 小时') as alert_message,
    current_timestamp() as alert_time
FROM dws_student_learning_summary
WHERE total_study_hours < 15
ORDER BY total_study_hours ASC
LIMIT 10;
