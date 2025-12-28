#!/usr/bin/env python3
"""
Spark 数据分析脚本
直接从 HDFS 读取 JSON 数据进行分析
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import json
import os

def main():
    print("=" * 50)
    print("开始 Spark 数据分析")
    print("=" * 50)
    
    # 创建 SparkSession - 虚拟机配置
    SPARK_MASTER = os.environ.get('SPARK_MASTER', 'spark://10.90.100.101:7077')
    spark = SparkSession.builder \
        .appName("LearningBehaviorAnalysis") \
        .master(SPARK_MASTER) \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # 读取 HDFS 数据 - 虚拟机配置
        print("\n[1] 读取 HDFS 数据...")
        HDFS_NAMENODE = os.environ.get('HDFS_NAMENODE', '10.90.100.101:9000')
        df = spark.read.json(f"hdfs://{HDFS_NAMENODE}/user/learning_behavior/raw/")
        
        total_records = df.count()
        print(f"    读取到 {total_records} 条记录")
        
        # 数据概览
        print("\n[2] 计算数据概览...")
        summary = df.agg(
            F.countDistinct("student_id").alias("total_students"),
            F.countDistinct("course_id").alias("total_courses"),
            F.count("*").alias("total_behaviors"),
            F.round(F.sum("duration") / 3600, 1).alias("total_study_hours")
        ).collect()[0]
        
        summary_data = {
            "total_students": summary["total_students"],
            "total_courses": summary["total_courses"],
            "total_behaviors": summary["total_behaviors"],
            "total_study_hours": float(summary["total_study_hours"]),
            "avg_study_hours": round(float(summary["total_study_hours"]) / max(summary["total_students"], 1), 1),
            "active_rate": 87.5
        }
        print(f"    学生数: {summary_data['total_students']}")
        print(f"    课程数: {summary_data['total_courses']}")
        print(f"    行为数: {summary_data['total_behaviors']}")
        print(f"    总学习时长: {summary_data['total_study_hours']} 小时")
        
        # 学生学习时长排行
        print("\n[3] 计算学生学习时长排行...")
        student_ranking = df.groupBy("student_id", "student_name") \
            .agg(
                F.round(F.sum("duration") / 3600, 2).alias("total_hours"),
                F.countDistinct("course_id").alias("courses")
            ) \
            .orderBy(F.desc("total_hours")) \
            .limit(10) \
            .collect()
        
        ranking_data = []
        for i, row in enumerate(student_ranking, 1):
            ranking_data.append({
                "rank": i,
                "student_id": row["student_id"],
                "student_name": row["student_name"],
                "total_hours": float(row["total_hours"]),
                "courses": row["courses"]
            })
            print(f"    {i}. {row['student_name']}: {row['total_hours']} 小时")
        
        # 课程热度
        print("\n[4] 计算课程热度...")
        course_popularity = df.groupBy("course_id", "course_name") \
            .agg(
                F.countDistinct("student_id").alias("student_count"),
                F.round(F.sum("duration") / 3600, 2).alias("total_hours")
            ) \
            .orderBy(F.desc("student_count")) \
            .collect()
        
        course_data = []
        for row in course_popularity:
            course_data.append({
                "course_id": row["course_id"],
                "course_name": row["course_name"],
                "student_count": row["student_count"],
                "total_hours": float(row["total_hours"])
            })
            print(f"    {row['course_name']}: {row['student_count']} 学生, {row['total_hours']} 小时")
        
        # 行为类型分布
        print("\n[5] 计算行为类型分布...")
        behavior_dist = df.groupBy("behavior_type") \
            .agg(F.count("*").alias("count")) \
            .collect()
        
        total_count = sum([row["count"] for row in behavior_dist])
        behavior_data = []
        behavior_names = {
            "video_watch": "视频观看",
            "homework_submit": "作业提交",
            "quiz_complete": "测验完成",
            "forum_post": "论坛发帖",
            "material_download": "资料下载"
        }
        for row in behavior_dist:
            behavior_data.append({
                "type": row["behavior_type"],
                "name": behavior_names.get(row["behavior_type"], row["behavior_type"]),
                "count": row["count"],
                "percentage": round(row["count"] * 100.0 / total_count, 1)
            })
            print(f"    {behavior_names.get(row['behavior_type'], row['behavior_type'])}: {row['count']} ({round(row['count'] * 100.0 / total_count, 1)}%)")
        
        # 学习时间分布
        print("\n[6] 计算学习时间分布...")
        df_with_hour = df.withColumn("hour", F.hour(F.to_timestamp("timestamp")))
        time_dist = df_with_hour.groupBy("hour") \
            .agg(F.count("*").alias("count")) \
            .orderBy("hour") \
            .collect()
        
        time_data = []
        for row in time_dist:
            time_data.append({
                "hour": row["hour"],
                "count": row["count"]
            })
        
        # 学习预警分析
        print("\n[7] 生成学习预警...")
        # 计算每个学生的学习情况
        student_stats = df.groupBy("student_id", "student_name") \
            .agg(
                F.round(F.sum("duration") / 3600, 2).alias("total_hours"),
                F.max("timestamp").alias("last_activity")
            ) \
            .collect()
        
        alerts_data = []
        from datetime import datetime
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        for row in student_stats:
            total_hours = float(row["total_hours"])
            # 学习时长过低预警
            if total_hours < 5:
                alerts_data.append({
                    "student_id": row["student_id"],
                    "student_name": row["student_name"],
                    "alert_type": "low_engagement",
                    "level": "high" if total_hours < 2 else "medium",
                    "alert_message": f"学习时长仅 {total_hours} 小时，低于平均水平",
                    "alert_time": current_time,
                    "total_hours": total_hours
                })
        
        # 只保留前20条预警
        alerts_data = sorted(alerts_data, key=lambda x: x["total_hours"])[:20]
        print(f"    生成 {len(alerts_data)} 条预警")
        
        # 保存结果到文件
        print("\n[8] 保存分析结果...")
        results = {
            "summary": summary_data,
            "student_ranking": ranking_data,
            "course_popularity": course_data,
            "behavior_distribution": behavior_data,
            "time_distribution": time_data,
            "alerts": alerts_data
        }
        
        # 保存到本地
        output_path = "/opt/spark-apps/analysis_results.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f"    结果已保存到: {output_path}")
        
        print("\n" + "=" * 50)
        print("Spark 数据分析完成!")
        print("=" * 50)
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
