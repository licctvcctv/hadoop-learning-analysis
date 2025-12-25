#!/usr/bin/env python3
# ============================================
# 课程热度分析
# 统计各课程学习人数和总时长，生成热度排名
# ============================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from logger import setup_spark_logger, SparkJobContext

def main():
    logger = setup_spark_logger('CoursePopularity', '/opt/spark/logs/course_popularity.log')
    
    with SparkJobContext(logger, '课程热度分析') as ctx:
        logger.info("创建SparkSession...")
        spark = SparkSession.builder \
            .appName("CoursePopularityAnalysis") \
            .enableHiveSupport() \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        
        try:
            logger.info("读取DWD层学习行为数据...")
            df = spark.sql("""
                SELECT 
                    course_id,
                    course_name,
                    student_id,
                    behavior_type,
                    duration,
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
            
            # 按课程汇总统计
            logger.info("按课程汇总统计...")
            course_stats = df.groupBy("course_id") \
                .agg(
                    F.first("course_name").alias("course_name"),
                    F.countDistinct("student_id").alias("student_count"),
                    F.sum("duration").alias("total_duration"),
                    F.round(F.sum("duration") / 3600, 2).alias("total_hours"),
                    F.round(F.avg("duration"), 2).alias("avg_duration"),
                    F.round(F.avg("duration") / 3600, 4).alias("avg_hours"),
                    F.count("*").alias("behavior_count"),
                    F.sum(F.when(F.col("behavior_type") == "video_watch", 1).otherwise(0)).alias("video_watch_count"),
                    F.sum(F.when(F.col("behavior_type") == "homework_submit", 1).otherwise(0)).alias("homework_submit_count"),
                    F.sum(F.when(F.col("behavior_type") == "quiz_complete", 1).otherwise(0)).alias("quiz_complete_count"),
                    F.round(F.avg("score"), 2).alias("avg_score")
                )
            
            # 添加热度排名（按学习人数降序）
            window_spec = Window.orderBy(F.desc("student_count"))
            course_popularity = course_stats \
                .withColumn("popularity_rank", F.row_number().over(window_spec)) \
                .withColumn("etl_time", F.current_timestamp()) \
                .withColumn("dt", F.current_date())
            
            # 显示结果
            logger.info("课程热度排名:")
            course_popularity.orderBy("popularity_rank").show(truncate=False)
            
            # 写入Hive表
            logger.info("写入DWS层课程热度表...")
            course_popularity.write \
                .mode("overwrite") \
                .partitionBy("dt") \
                .saveAsTable("learning_behavior.dws_course_popularity")
            
            logger.info("课程热度分析完成")
            
        finally:
            spark.stop()
            logger.info("SparkSession已关闭")


if __name__ == "__main__":
    main()
