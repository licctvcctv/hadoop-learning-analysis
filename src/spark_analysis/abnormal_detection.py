#!/usr/bin/env python3
# ============================================
# 学习异常检测
# 检测长时间未学习的学生，生成预警数据
# ============================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta
from logger import setup_spark_logger, SparkJobContext

# 预警阈值配置
INACTIVE_DAYS_THRESHOLD = 7      # 未学习天数阈值
LOW_DURATION_THRESHOLD = 1800    # 低学习时长阈值（秒）
LOW_SCORE_THRESHOLD = 60         # 低分阈值

def main():
    logger = setup_spark_logger('AbnormalDetection', '/opt/spark/logs/abnormal_detection.log')
    
    with SparkJobContext(logger, '学习异常检测') as ctx:
        logger.info("创建SparkSession...")
        spark = SparkSession.builder \
            .appName("AbnormalDetectionAnalysis") \
            .enableHiveSupport() \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        
        try:
            logger.info(f"预警阈值配置:")
            logger.info(f"  - 未学习天数阈值: {INACTIVE_DAYS_THRESHOLD} 天")
            logger.info(f"  - 低学习时长阈值: {LOW_DURATION_THRESHOLD} 秒")
            logger.info(f"  - 低分阈值: {LOW_SCORE_THRESHOLD} 分")
            
            logger.info("读取DWD层学习行为数据...")
            df = spark.sql("""
                SELECT 
                    student_id,
                    student_name,
                    course_id,
                    course_name,
                    behavior_type,
                    duration,
                    score,
                    behavior_time,
                    behavior_date
                FROM learning_behavior.dwd_learning_behavior
                WHERE is_valid = 1
            """)
            
            record_count = df.count()
            ctx.set_record_count(record_count)
            logger.info(f"读取到 {record_count} 条有效记录")
            
            if record_count == 0:
                logger.warning("没有数据可分析")
                return
            
            current_date = datetime.now().date()
            current_date_str = current_date.strftime('%Y-%m-%d')
            logger.info(f"当前日期: {current_date_str}")
            
            # 1. 检测长时间未学习的学生
            logger.info("检测长时间未学习的学生...")
            student_last_activity = df.groupBy("student_id", "student_name") \
                .agg(
                    F.max("behavior_date").alias("last_activity_date"),
                    F.sum("duration").alias("total_duration"),
                    F.count("*").alias("total_behaviors")
                )
            
            inactive_students = student_last_activity \
                .withColumn("current_date", F.lit(current_date_str).cast("date")) \
                .withColumn("inactive_days", 
                    F.datediff(F.col("current_date"), F.col("last_activity_date"))) \
                .filter(F.col("inactive_days") >= INACTIVE_DAYS_THRESHOLD) \
                .withColumn("alert_type", F.lit("长时间未学习")) \
                .withColumn("alert_level", 
                    F.when(F.col("inactive_days") >= 14, "高")
                    .when(F.col("inactive_days") >= 10, "中")
                    .otherwise("低")) \
                .withColumn("alert_message", 
                    F.concat(F.lit("学生已"), F.col("inactive_days"), F.lit("天未进行学习活动"))) \
                .select(
                    "student_id", "student_name", "alert_type", "alert_level",
                    "alert_message", "last_activity_date", "inactive_days"
                )
            
            inactive_count = inactive_students.count()
            logger.info(f"检测到 {inactive_count} 名长时间未学习的学生")
            
            # 2. 检测学习时长过低的学生
            logger.info("检测学习时长过低的学生...")
            low_duration_students = student_last_activity \
                .filter(F.col("total_duration") < LOW_DURATION_THRESHOLD) \
                .withColumn("alert_type", F.lit("学习时长过低")) \
                .withColumn("alert_level", 
                    F.when(F.col("total_duration") < 600, "高")
                    .when(F.col("total_duration") < 1200, "中")
                    .otherwise("低")) \
                .withColumn("alert_message", 
                    F.concat(F.lit("学生总学习时长仅"), 
                        F.round(F.col("total_duration") / 60, 1), F.lit("分钟"))) \
                .withColumn("inactive_days", F.lit(0)) \
                .select(
                    "student_id", "student_name", "alert_type", "alert_level",
                    "alert_message", "last_activity_date", "inactive_days"
                )
            
            low_duration_count = low_duration_students.count()
            logger.info(f"检测到 {low_duration_count} 名学习时长过低的学生")
            
            # 3. 检测成绩异常的学生
            logger.info("检测成绩异常的学生...")
            student_scores = df.filter(F.col("score").isNotNull()) \
                .groupBy("student_id", "student_name") \
                .agg(
                    F.round(F.avg("score"), 2).alias("avg_score"),
                    F.min("score").alias("min_score"),
                    F.max("behavior_date").alias("last_activity_date")
                )
            
            low_score_students = student_scores \
                .filter(F.col("avg_score") < LOW_SCORE_THRESHOLD) \
                .withColumn("alert_type", F.lit("成绩异常")) \
                .withColumn("alert_level", 
                    F.when(F.col("avg_score") < 40, "高")
                    .when(F.col("avg_score") < 50, "中")
                    .otherwise("低")) \
                .withColumn("alert_message", 
                    F.concat(F.lit("学生平均成绩仅"), F.col("avg_score"), F.lit("分"))) \
                .withColumn("inactive_days", F.lit(0)) \
                .select(
                    "student_id", "student_name", "alert_type", "alert_level",
                    "alert_message", "last_activity_date", "inactive_days"
                )
            
            low_score_count = low_score_students.count()
            logger.info(f"检测到 {low_score_count} 名成绩异常的学生")
            
            # 合并所有预警
            logger.info("合并所有预警数据...")
            all_alerts = inactive_students \
                .union(low_duration_students) \
                .union(low_score_students) \
                .withColumn("alert_id", F.monotonically_increasing_id()) \
                .withColumn("alert_time", F.current_timestamp()) \
                .withColumn("is_handled", F.lit(0)) \
                .withColumn("dt", F.current_date())
            
            total_alerts = all_alerts.count()
            logger.info(f"总预警数量: {total_alerts}")
            
            # 显示预警结果
            logger.info("预警结果:")
            all_alerts.show(20, truncate=False)
            
            # 按预警级别统计
            alert_summary = all_alerts.groupBy("alert_level", "alert_type").count()
            logger.info("预警统计:")
            alert_summary.show(truncate=False)
            
            # 写入ADS层预警表
            logger.info("写入ADS层学习预警表...")
            all_alerts.write \
                .mode("overwrite") \
                .partitionBy("dt") \
                .saveAsTable("learning_behavior.ads_learning_alerts")
            
            logger.info("学习异常检测完成")
            
        finally:
            spark.stop()
            logger.info("SparkSession已关闭")


if __name__ == "__main__":
    main()
