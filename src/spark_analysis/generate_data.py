#!/usr/bin/env python3
"""
Spark 数据生成脚本
使用 Spark 分布式生成大规模学习行为数据并存储到 HDFS
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import random
import uuid
from datetime import datetime, timedelta

def main():
    print("=" * 60)
    print("Spark 分布式数据生成器")
    print("=" * 60)
    
    # 创建 SparkSession
    spark = SparkSession.builder \
        .appName("LearningDataGenerator") \
        .master("spark://spark-master:7077") \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # 配置参数
        NUM_STUDENTS = 50       # 学生数量
        NUM_RECORDS = 10000     # 总记录数
        
        print(f"\n[配置] 学生数: {NUM_STUDENTS}, 记录数: {NUM_RECORDS}")
        
        # 定义学生信息
        student_names = [
            "张三", "李四", "王五", "赵六", "钱七", "孙八", "周九", "吴十",
            "郑十一", "王十二", "冯十三", "陈十四", "褚十五", "卫十六", "蒋十七", "沈十八",
            "韩十九", "杨二十", "朱二一", "秦二二", "尤二三", "许二四", "何二五", "吕二六",
            "施二七", "张二八", "孔二九", "曹三十", "严三一", "华三二", "金三三", "魏三四",
            "陶三五", "姜三六", "戚三七", "谢三八", "邹三九", "喻四十", "柏四一", "水四二",
            "窦四三", "章四四", "云四五", "苏四六", "潘四七", "葛四八", "奚四九", "范五十"
        ]
        
        students = [(f"STU_2021{str(i).zfill(4)}", student_names[i % len(student_names)]) 
                    for i in range(NUM_STUDENTS)]
        
        # 定义课程信息
        courses = [
            ("COURSE_001", "大数据技术基础"),
            ("COURSE_002", "Python程序设计"),
            ("COURSE_003", "数据库原理"),
            ("COURSE_004", "机器学习导论"),
            ("COURSE_005", "云计算技术"),
            ("COURSE_006", "数据挖掘"),
            ("COURSE_007", "分布式系统"),
            ("COURSE_008", "人工智能基础"),
        ]
        
        # 行为类型
        behavior_types = ["video_watch", "homework_submit", "quiz_complete", "forum_post", "material_download"]
        
        print("\n[1] 使用 Spark 生成分布式数据...")
        
        # 创建基础 DataFrame - 生成 ID 序列
        ids_df = spark.range(NUM_RECORDS)
        
        # 广播学生和课程数据
        students_bc = spark.sparkContext.broadcast(students)
        courses_bc = spark.sparkContext.broadcast(courses)
        behaviors_bc = spark.sparkContext.broadcast(behavior_types)
        
        # 定义 UDF 生成随机数据
        @F.udf(StringType())
        def random_student_id(seed):
            random.seed(seed)
            return students_bc.value[random.randint(0, len(students_bc.value) - 1)][0]
        
        @F.udf(StringType())
        def random_student_name(student_id):
            for sid, name in students_bc.value:
                if sid == student_id:
                    return name
            return "未知"
        
        @F.udf(StringType())
        def random_course_id(seed):
            random.seed(seed + 1000)
            return courses_bc.value[random.randint(0, len(courses_bc.value) - 1)][0]
        
        @F.udf(StringType())
        def random_course_name(course_id):
            for cid, name in courses_bc.value:
                if cid == course_id:
                    return name
            return "未知课程"
        
        @F.udf(StringType())
        def random_behavior(seed):
            random.seed(seed + 2000)
            return behaviors_bc.value[random.randint(0, len(behaviors_bc.value) - 1)]
        
        @F.udf(IntegerType())
        def random_duration(behavior, seed):
            random.seed(seed + 3000)
            if behavior == "video_watch":
                return random.randint(300, 3600)
            elif behavior == "homework_submit":
                return random.randint(1800, 7200)
            elif behavior == "quiz_complete":
                return random.randint(600, 1800)
            elif behavior == "forum_post":
                return random.randint(120, 600)
            else:
                return random.randint(60, 300)
        
        @F.udf(StringType())
        def random_timestamp(seed):
            random.seed(seed + 4000)
            base = datetime(2025, 9, 1)
            days = random.randint(0, 115)  # 9月1日到12月25日
            hours = random.randint(0, 23)
            minutes = random.randint(0, 59)
            seconds = random.randint(0, 59)
            dt = base + timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        
        @F.udf(StringType())
        def gen_uuid(seed):
            random.seed(seed + 5000)
            return str(uuid.UUID(int=random.getrandbits(128)))
        
        # 生成数据
        df = ids_df \
            .withColumn("record_id", gen_uuid(F.col("id"))) \
            .withColumn("student_id", random_student_id(F.col("id"))) \
            .withColumn("course_id", random_course_id(F.col("id"))) \
            .withColumn("behavior_type", random_behavior(F.col("id"))) \
            .withColumn("timestamp", random_timestamp(F.col("id")))
        
        df = df \
            .withColumn("student_name", random_student_name(F.col("student_id"))) \
            .withColumn("course_name", random_course_name(F.col("course_id"))) \
            .withColumn("duration", random_duration(F.col("behavior_type"), F.col("id")))
        
        # 选择最终列
        final_df = df.select(
            "record_id", "student_id", "student_name", 
            "course_id", "course_name", "behavior_type", 
            "duration", "timestamp"
        )
        
        print(f"    生成 {final_df.count()} 条记录")
        
        # 显示样例数据
        print("\n[2] 样例数据:")
        final_df.show(5, truncate=False)
        
        # 保存到 HDFS
        print("\n[3] 保存数据到 HDFS...")
        hdfs_path = "hdfs://namenode:9000/user/learning_behavior/raw"
        
        # 先删除旧数据
        try:
            spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark._jsc.hadoopConfiguration()
            ).delete(spark._jvm.org.apache.hadoop.fs.Path(hdfs_path), True)
            print(f"    已清理旧数据: {hdfs_path}")
        except:
            pass
        
        # 写入新数据
        final_df.write.mode("overwrite").json(hdfs_path)
        print(f"    数据已保存到: {hdfs_path}")
        
        # 统计信息
        print("\n[4] 数据统计:")
        print(f"    学生数: {final_df.select('student_id').distinct().count()}")
        print(f"    课程数: {final_df.select('course_id').distinct().count()}")
        print(f"    总记录: {final_df.count()}")
        
        # 行为分布
        print("\n    行为类型分布:")
        final_df.groupBy("behavior_type").count().orderBy(F.desc("count")).show()
        
        print("\n" + "=" * 60)
        print("Spark 数据生成完成!")
        print("=" * 60)
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
