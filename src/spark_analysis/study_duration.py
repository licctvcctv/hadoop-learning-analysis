#!/usr/bin/env python3
# ============================================
# 学习时长统计分析
# 按学生统计总学习时长
# ============================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from logger import setup_spark_logger, SparkJobContext

def main():
    logger = setup_spark_logger('StudyDuration', '/opt/spark/logs/study_duration.log')
    
    with SparkJobContext(logger, '学习时长统计分析') as ctx:
        # 创建SparkSession
        logger.info("创建SparkSession...")
        spark = SparkSession.builder \
            .appName("StudyDurationAnalysis") \
            .enableHiveSupport() \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        
        try:
            # 读取DWD层数据
            logger.info("读取DWD层学习行为数据...")
            df = spark.sql("""
                SELECT 
                    student_id,
                    student_name,
                    course_id,
                    behavior_type,
                    duration,
                    behavior_time,
                    behavior_date,
                    score
                FROM learning_behavior.dwd_learning_behavior
                WHERE is_valid = 1
            """)
            
            record_count = df.count()
            ctx.set_record_count(record_count)
            logger.info(f"读取到 {record_count} 条有效记录")
            
            if record_count == 0:
                logger.warning("没有数据可分析")
                return
            
            # 按学生汇总统计
            logger.info("按学生汇总统计...")
            student_summary = df.groupBy("student_id") \
                .agg(
                    F.first("student_name").alias("student_name"),
                    F.sum("duration").alias("total_study_duration"),
                    F.round(F.sum("duration") / 3600, 2).alias("total_study_hours"),
                    F.countDistinct("course_id").alias("total_courses"),
                    F.sum(F.when(F.col("behavior_type") == "video_watch", 1).otherwise(0)).alias("video_watch_count"),
                    F.sum(F.when(F.col("behavior_type") == "video_watch", F.col("duration")).otherwise(0)).alias("video_watch_duration"),
                    F.sum(F.when(F.col("behavior_type") == "homework_submit", 1).otherwise(0)).alias("homework_submit_count"),
                    F.sum(F.when(F.col("behavior_type") == "quiz_complete", 1).otherwise(0)).alias("quiz_complete_count"),
                    F.sum(F.when(F.col("behavior_type") == "forum_post", 1).otherwise(0)).alias("forum_post_count"),
                    F.sum(F.when(F.col("behavior_type") == "material_download", 1).otherwise(0)).alias("material_download_count"),
                    F.count("*").alias("total_behavior_count"),
                    F.round(F.avg("score"), 2).alias("avg_score"),
                    F.max("score").alias("max_score"),
                    F.min("score").alias("min_score"),
                    F.min("behavior_time").alias("first_study_time"),
                    F.max("behavior_time").alias("last_study_time"),
                    F.countDistinct("behavior_date").alias("active_days")
                ) \
                .withColumn("etl_time", F.current_timestamp()) \
                .withColumn("dt", F.current_date())
            
            # 显示统计结果
            logger.info("学习时长统计结果（Top 10）:")
            student_summary.orderBy(F.desc("total_study_hours")).show(10, truncate=False)
            
            # 写入Hive表
            logger.info("写入DWS层汇总表...")
            student_summary.write \
                .mode("overwrite") \
                .partitionBy("dt") \
                .saveAsTable("learning_behavior.dws_student_learning_summary")
            
            logger.info("学习时长统计完成")
            
        finally:
            spark.stop()
            logger.info("SparkSession已关闭")


if __name__ == "__main__":
    main()
