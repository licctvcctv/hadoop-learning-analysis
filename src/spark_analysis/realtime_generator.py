#!/usr/bin/env python3
"""
实时数据生成器
持续生成学习行为数据并追加到 HDFS
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import random
import uuid
from datetime import datetime
import time
import sys

def generate_batch(spark, batch_size, students, courses, behavior_types):
    """生成一批数据"""
    records = []
    now = datetime.now()
    
    for _ in range(batch_size):
        student = random.choice(students)
        course = random.choice(courses)
        behavior = random.choice(behavior_types)
        
        # 根据行为类型设置时长
        if behavior == "video_watch":
            duration = random.randint(300, 3600)
        elif behavior == "homework_submit":
            duration = random.randint(1800, 7200)
        elif behavior == "quiz_complete":
            duration = random.randint(600, 1800)
        elif behavior == "forum_post":
            duration = random.randint(120, 600)
        else:
            duration = random.randint(60, 300)
        
        records.append({
            "record_id": str(uuid.uuid4()),
            "student_id": student[0],
            "student_name": student[1],
            "course_id": course[0],
            "course_name": course[1],
            "behavior_type": behavior,
            "duration": duration,
            "timestamp": now.strftime("%Y-%m-%d %H:%M:%S")
        })
    
    schema = StructType([
        StructField("record_id", StringType(), False),
        StructField("student_id", StringType(), False),
        StructField("student_name", StringType(), False),
        StructField("course_id", StringType(), False),
        StructField("course_name", StringType(), False),
        StructField("behavior_type", StringType(), False),
        StructField("duration", IntegerType(), False),
        StructField("timestamp", StringType(), False),
    ])
    
    return spark.createDataFrame(records, schema)

def main():
    # 参数
    batch_size = int(sys.argv[1]) if len(sys.argv) > 1 else 100
    interval = int(sys.argv[2]) if len(sys.argv) > 2 else 5
    max_batches = int(sys.argv[3]) if len(sys.argv) > 3 else 0  # 0 = 无限
    
    print("=" * 60)
    print("实时数据生成器")
    print(f"每批数量: {batch_size}, 间隔: {interval}秒, 最大批次: {max_batches or '无限'}")
    print("=" * 60)
    
    spark = SparkSession.builder \
        .appName("RealtimeDataGenerator") \
        .master("spark://spark-master:7077") \
        .config("spark.executor.memory", "512m") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    # 学生和课程数据
    students = [
        ("STU_20210001", "张三"), ("STU_20210002", "李四"), ("STU_20210003", "王五"),
        ("STU_20210004", "赵六"), ("STU_20210005", "钱七"), ("STU_20210006", "孙八"),
        ("STU_20210007", "周九"), ("STU_20210008", "吴十"), ("STU_20210009", "郑十一"),
        ("STU_20210010", "王十二"), ("STU_20210011", "冯十三"), ("STU_20210012", "陈十四"),
        ("STU_20210013", "褚十五"), ("STU_20210014", "卫十六"), ("STU_20210015", "蒋十七"),
        ("STU_20210016", "沈十八"), ("STU_20210017", "韩十九"), ("STU_20210018", "杨二十"),
        ("STU_20210019", "朱二一"), ("STU_20210020", "秦二二"), ("STU_20210021", "尤二三"),
        ("STU_20210022", "许二四"), ("STU_20210023", "何二五"), ("STU_20210024", "吕二六"),
        ("STU_20210025", "施二七"), ("STU_20210026", "张二八"), ("STU_20210027", "孔二九"),
        ("STU_20210028", "曹三十"), ("STU_20210029", "严三一"), ("STU_20210030", "华三二"),
    ]
    
    courses = [
        ("COURSE_001", "大数据技术基础"), ("COURSE_002", "Python程序设计"),
        ("COURSE_003", "数据库原理"), ("COURSE_004", "机器学习导论"),
        ("COURSE_005", "云计算技术"), ("COURSE_006", "数据挖掘"),
        ("COURSE_007", "分布式系统"), ("COURSE_008", "人工智能基础"),
    ]
    
    behavior_types = ["video_watch", "homework_submit", "quiz_complete", "forum_post", "material_download"]
    
    hdfs_path = "hdfs://namenode:9000/user/learning_behavior/raw"
    batch_count = 0
    total_records = 0
    
    try:
        print(f"\n开始生成数据... (Ctrl+C 停止)\n")
        
        while max_batches == 0 or batch_count < max_batches:
            batch_count += 1
            
            # 生成数据
            df = generate_batch(spark, batch_size, students, courses, behavior_types)
            
            # 追加到 HDFS
            df.write.mode("append").json(hdfs_path)
            
            total_records += batch_size
            now = datetime.now().strftime("%H:%M:%S")
            print(f"[{now}] 批次 {batch_count}: +{batch_size} 条, 累计 {total_records} 条")
            
            if max_batches == 0 or batch_count < max_batches:
                time.sleep(interval)
        
        print(f"\n完成! 共生成 {total_records} 条记录")
        
    except KeyboardInterrupt:
        print(f"\n\n已停止! 共生成 {total_records} 条记录")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
