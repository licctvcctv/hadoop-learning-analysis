-- ============================================
-- ETL数据加载脚本
-- 从ODS层加载数据到DWD层和DWS层
-- ============================================

USE learning_behavior;

-- 设置动态分区
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=10000;
SET hive.exec.max.dynamic.partitions.pernode=1000;

-- ==================== ODS -> DWD ====================
-- 数据清洗和转换

INSERT OVERWRITE TABLE dwd_learning_behavior PARTITION (dt)
SELECT
    student_id,
    student_name,
    course_id,
    course_name,
    behavior_type,
    duration,
    CAST(`timestamp` AS TIMESTAMP) AS behavior_time,
    SUBSTR(`timestamp`, 1, 10) AS behavior_date,
    HOUR(CAST(`timestamp` AS TIMESTAMP)) AS behavior_hour,
    DAYOFWEEK(CAST(`timestamp` AS TIMESTAMP)) AS behavior_weekday,
    CASE 
        WHEN HOUR(CAST(`timestamp` AS TIMESTAMP)) BETWEEN 6 AND 11 THEN 'morning'
        WHEN HOUR(CAST(`timestamp` AS TIMESTAMP)) BETWEEN 12 AND 17 THEN 'afternoon'
        WHEN HOUR(CAST(`timestamp` AS TIMESTAMP)) BETWEEN 18 AND 22 THEN 'evening'
        ELSE 'night'
    END AS behavior_period,
    ip_address,
    device_type,
    chapter_id,
    score,
    CASE 
        WHEN student_id IS NOT NULL 
             AND course_id IS NOT NULL 
             AND behavior_type IS NOT NULL 
             AND duration > 0 
             AND duration < 86400  -- 小于24小时
        THEN 1 
        ELSE 0 
    END AS is_valid,
    CURRENT_TIMESTAMP AS etl_time,
    SUBSTR(`timestamp`, 1, 10) AS dt
FROM ods_learning_behavior
WHERE student_id IS NOT NULL
  AND `timestamp` IS NOT NULL;

-- ==================== DWD -> DWS 学生学习汇总 ====================

INSERT OVERWRITE TABLE dws_student_learning_summary PARTITION (dt)
SELECT
    student_id,
    MAX(student_name) AS student_name,
    SUM(duration) AS total_study_duration,
    ROUND(SUM(duration) / 3600.0, 2) AS total_study_hours,
    COUNT(DISTINCT course_id) AS total_courses,
    SUM(CASE WHEN behavior_type = 'video_watch' THEN 1 ELSE 0 END) AS video_watch_count,
    SUM(CASE WHEN behavior_type = 'video_watch' THEN duration ELSE 0 END) AS video_watch_duration,
    SUM(CASE WHEN behavior_type = 'homework_submit' THEN 1 ELSE 0 END) AS homework_submit_count,
    SUM(CASE WHEN behavior_type = 'quiz_complete' THEN 1 ELSE 0 END) AS quiz_complete_count,
    SUM(CASE WHEN behavior_type = 'forum_post' THEN 1 ELSE 0 END) AS forum_post_count,
    SUM(CASE WHEN behavior_type = 'material_download' THEN 1 ELSE 0 END) AS material_download_count,
    COUNT(*) AS total_behavior_count,
    ROUND(AVG(score), 2) AS avg_score,
    MAX(score) AS max_score,
    MIN(score) AS min_score,
    MIN(behavior_time) AS first_study_time,
    MAX(behavior_time) AS last_study_time,
    COUNT(DISTINCT behavior_date) AS active_days,
    CURRENT_TIMESTAMP AS etl_time,
    CURRENT_DATE AS dt
FROM dwd_learning_behavior
WHERE is_valid = 1
GROUP BY student_id;

-- ==================== DWD -> DWS 课程热度汇总 ====================

INSERT OVERWRITE TABLE dws_course_popularity PARTITION (dt)
SELECT
    course_id,
    MAX(course_name) AS course_name,
    COUNT(DISTINCT student_id) AS student_count,
    SUM(duration) AS total_duration,
    ROUND(SUM(duration) / 3600.0, 2) AS total_hours,
    ROUND(AVG(duration), 2) AS avg_duration,
    ROUND(AVG(duration) / 3600.0, 4) AS avg_hours,
    COUNT(*) AS behavior_count,
    SUM(CASE WHEN behavior_type = 'video_watch' THEN 1 ELSE 0 END) AS video_watch_count,
    SUM(CASE WHEN behavior_type = 'homework_submit' THEN 1 ELSE 0 END) AS homework_submit_count,
    SUM(CASE WHEN behavior_type = 'quiz_complete' THEN 1 ELSE 0 END) AS quiz_complete_count,
    ROUND(AVG(score), 2) AS avg_score,
    ROW_NUMBER() OVER (ORDER BY COUNT(DISTINCT student_id) DESC) AS popularity_rank,
    CURRENT_TIMESTAMP AS etl_time,
    CURRENT_DATE AS dt
FROM dwd_learning_behavior
WHERE is_valid = 1
GROUP BY course_id;

-- ==================== DWD -> DWS 学习时间分布 ====================

INSERT OVERWRITE TABLE dws_study_time_distribution PARTITION (dt)
SELECT
    behavior_hour AS hour_of_day,
    COUNT(*) AS behavior_count,
    COUNT(DISTINCT student_id) AS student_count,
    SUM(duration) AS total_duration,
    ROUND(AVG(duration), 2) AS avg_duration,
    CASE 
        WHEN behavior_hour BETWEEN 6 AND 8 THEN '早晨 (6-8点)'
        WHEN behavior_hour BETWEEN 9 AND 11 THEN '上午 (9-11点)'
        WHEN behavior_hour BETWEEN 12 AND 13 THEN '中午 (12-13点)'
        WHEN behavior_hour BETWEEN 14 AND 17 THEN '下午 (14-17点)'
        WHEN behavior_hour BETWEEN 18 AND 21 THEN '晚上 (18-21点)'
        WHEN behavior_hour BETWEEN 22 AND 23 THEN '深夜 (22-23点)'
        ELSE '凌晨 (0-5点)'
    END AS time_period,
    CURRENT_TIMESTAMP AS etl_time,
    CURRENT_DATE AS dt
FROM dwd_learning_behavior
WHERE is_valid = 1
GROUP BY behavior_hour;

-- ==================== DWD -> DWS 行为类型分布 ====================

INSERT OVERWRITE TABLE dws_behavior_type_distribution PARTITION (dt)
SELECT
    behavior_type,
    CASE behavior_type
        WHEN 'video_watch' THEN '视频观看'
        WHEN 'homework_submit' THEN '作业提交'
        WHEN 'quiz_complete' THEN '测验完成'
        WHEN 'forum_post' THEN '论坛发帖'
        WHEN 'material_download' THEN '资料下载'
        ELSE behavior_type
    END AS behavior_type_name,
    COUNT(*) AS behavior_count,
    COUNT(DISTINCT student_id) AS student_count,
    SUM(duration) AS total_duration,
    ROUND(AVG(duration), 2) AS avg_duration,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage,
    CURRENT_TIMESTAMP AS etl_time,
    CURRENT_DATE AS dt
FROM dwd_learning_behavior
WHERE is_valid = 1
GROUP BY behavior_type;

-- 显示ETL结果统计
SELECT 'dwd_learning_behavior' AS table_name, COUNT(*) AS row_count FROM dwd_learning_behavior
UNION ALL
SELECT 'dws_student_learning_summary', COUNT(*) FROM dws_student_learning_summary
UNION ALL
SELECT 'dws_course_popularity', COUNT(*) FROM dws_course_popularity
UNION ALL
SELECT 'dws_study_time_distribution', COUNT(*) FROM dws_study_time_distribution
UNION ALL
SELECT 'dws_behavior_type_distribution', COUNT(*) FROM dws_behavior_type_distribution;
