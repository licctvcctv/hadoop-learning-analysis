#!/usr/bin/env python3
# ============================================
# 学习行为模式分析
# 分析学习时间分布（按小时、星期）和行为类型分布
# ============================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from logger import setup_spark_logger, SparkJobContext

def main():
    logger = setup_spark_logger('BehaviorPattern', '/opt/spark/logs/behavior_pattern.log')
    
    with SparkJobContext(logger, '学习行为模式分析') as ctx:
        logger.info("创建SparkSession...")
        spark = SparkSession.builder \
            .appName("BehaviorPatternAnalysis") \
            .enableHiveSupport() \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        
        try:
            logger.info("读取DWD层学习行为数据...")
            df = spark.sql("""
                SELECT 
                    student_id,
                    course_id,
                    behavior_type,
                    duration,
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
            
            # 提取时间特征
            df_with_time = df \
                .withColumn("hour", F.hour("behavior_time")) \
                .withColumn("day_of_week", F.dayofweek("behavior_date")) \
                .withColumn("week_name", 
                    F.when(F.col("day_of_week") == 1, "周日")
                    .when(F.col("day_of_week") == 2, "周一")
                    .when(F.col("day_of_week") == 3, "周二")
                    .when(F.col("day_of_week") == 4, "周三")
                    .when(F.col("day_of_week") == 5, "周四")
                    .when(F.col("day_of_week") == 6, "周五")
                    .otherwise("周六"))
            
            # 1. 按小时统计学习分布
            logger.info("统计按小时学习分布...")
            hourly_stats = df_with_time.groupBy("hour") \
                .agg(
                    F.count("*").alias("behavior_count"),
                    F.countDistinct("student_id").alias("student_count"),
                    F.sum("duration").alias("total_duration"),
                    F.round(F.avg("duration"), 2).alias("avg_duration")
                ) \
                .withColumn("time_period",
                    F.when((F.col("hour") >= 6) & (F.col("hour") < 12), "上午")
                    .when((F.col("hour") >= 12) & (F.col("hour") < 14), "中午")
                    .when((F.col("hour") >= 14) & (F.col("hour") < 18), "下午")
                    .when((F.col("hour") >= 18) & (F.col("hour") < 22), "晚上")
                    .otherwise("深夜")) \
                .withColumn("etl_time", F.current_timestamp()) \
                .withColumn("dt", F.current_date())
            
            logger.info("按小时学习分布:")
            hourly_stats.orderBy("hour").show(24, truncate=False)
            
            # 2. 按星期统计学习分布
            logger.info("统计按星期学习分布...")
            weekly_stats = df_with_time.groupBy("day_of_week", "week_name") \
                .agg(
                    F.count("*").alias("behavior_count"),
                    F.countDistinct("student_id").alias("student_count"),
                    F.sum("duration").alias("total_duration"),
                    F.round(F.avg("duration"), 2).alias("avg_duration")
                ) \
                .withColumn("is_weekend", 
                    F.when(F.col("day_of_week").isin([1, 7]), 1).otherwise(0)) \
                .withColumn("etl_time", F.current_timestamp()) \
                .withColumn("dt", F.current_date())
            
            logger.info("按星期学习分布:")
            weekly_stats.orderBy("day_of_week").show(truncate=False)
            
            # 3. 按行为类型统计分布
            logger.info("统计行为类型分布...")
            behavior_stats = df.groupBy("behavior_type") \
                .agg(
                    F.count("*").alias("behavior_count"),
                    F.countDistinct("student_id").alias("student_count"),
                    F.countDistinct("course_id").alias("course_count"),
                    F.sum("duration").alias("total_duration"),
                    F.round(F.avg("duration"), 2).alias("avg_duration")
                ) \
                .withColumn("etl_time", F.current_timestamp()) \
                .withColumn("dt", F.current_date())
            
            logger.info("行为类型分布:")
            behavior_stats.show(truncate=False)
            
            # 写入Hive表
            logger.info("写入DWS层时间分布表...")
            hourly_stats.write \
                .mode("overwrite") \
                .partitionBy("dt") \
                .saveAsTable("learning_behavior.dws_hourly_distribution")
            
            logger.info("写入DWS层星期分布表...")
            weekly_stats.write \
                .mode("overwrite") \
                .partitionBy("dt") \
                .saveAsTable("learning_behavior.dws_weekly_distribution")
            
            logger.info("写入DWS层行为类型分布表...")
            behavior_stats.write \
                .mode("overwrite") \
                .partitionBy("dt") \
                .saveAsTable("learning_behavior.dws_behavior_distribution")
            
            logger.info("学习行为模式分析完成")
            
        finally:
            spark.stop()
            logger.info("SparkSession已关闭")


if __name__ == "__main__":
    main()
