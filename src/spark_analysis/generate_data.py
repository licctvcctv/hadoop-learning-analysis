#!/usr/bin/env python3
"""
Spark 数据生成脚本
使用 Spark 分布式生成大规模学习行为数据并存储到 HDFS
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

def main():
    print("=" * 60)
    print("Spark 分布式数据生成器")
    print("=" * 60)
    
    # 创建 SparkSession - 虚拟机配置
    SPARK_MASTER = os.environ.get('SPARK_MASTER', 'spark://10.90.100.101:7077')
    spark = SparkSession.builder \
        .appName("LearningDataGenerator") \
        .master(SPARK_MASTER) \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # 配置参数
        NUM_STUDENTS = 50
        NUM_RECORDS = 10000
        
        print(f"\n[配置] 学生数: {NUM_STUDENTS}, 记录数: {NUM_RECORDS}")
        
        # 定义学生信息
        student_data = [
            ("STU_20210000", "张三"), ("STU_20210001", "李四"), ("STU_20210002", "王五"),
            ("STU_20210003", "赵六"), ("STU_20210004", "钱七"), ("STU_20210005", "孙八"),
            ("STU_20210006", "周九"), ("STU_20210007", "吴十"), ("STU_20210008", "郑十一"),
            ("STU_20210009", "王十二"), ("STU_20210010", "冯十三"), ("STU_20210011", "陈十四"),
            ("STU_20210012", "褚十五"), ("STU_20210013", "卫十六"), ("STU_20210014", "蒋十七"),
            ("STU_20210015", "沈十八"), ("STU_20210016", "韩十九"), ("STU_20210017", "杨二十"),
            ("STU_20210018", "朱二一"), ("STU_20210019", "秦二二"), ("STU_20210020", "尤二三"),
            ("STU_20210021", "许二四"), ("STU_20210022", "何二五"), ("STU_20210023", "吕二六"),
            ("STU_20210024", "施二七"), ("STU_20210025", "张二八"), ("STU_20210026", "孔二九"),
            ("STU_20210027", "曹三十"), ("STU_20210028", "严三一"), ("STU_20210029", "华三二"),
            ("STU_20210030", "金三三"), ("STU_20210031", "魏三四"), ("STU_20210032", "陶三五"),
            ("STU_20210033", "姜三六"), ("STU_20210034", "戚三七"), ("STU_20210035", "谢三八"),
            ("STU_20210036", "邹三九"), ("STU_20210037", "喻四十"), ("STU_20210038", "柏四一"),
            ("STU_20210039", "水四二"), ("STU_20210040", "窦四三"), ("STU_20210041", "章四四"),
            ("STU_20210042", "云四五"), ("STU_20210043", "苏四六"), ("STU_20210044", "潘四七"),
            ("STU_20210045", "葛四八"), ("STU_20210046", "奚四九"), ("STU_20210047", "范五十"),
            ("STU_20210048", "彭五一"), ("STU_20210049", "鲁五二")
        ]
        
        course_data = [
            ("COURSE_001", "大数据技术基础"),
            ("COURSE_002", "Python程序设计"),
            ("COURSE_003", "数据库原理"),
            ("COURSE_004", "机器学习导论"),
            ("COURSE_005", "云计算技术"),
            ("COURSE_006", "数据挖掘"),
            ("COURSE_007", "分布式系统"),
            ("COURSE_008", "人工智能基础"),
        ]
        
        behavior_types = ["video_watch", "homework_submit", "quiz_complete", "forum_post", "material_download"]
        
        print("\n[1] 使用 Spark 生成分布式数据...")
        
        # 创建学生和课程 DataFrame
        students_df = spark.createDataFrame(student_data, ["student_id", "student_name"])
        courses_df = spark.createDataFrame(course_data, ["course_id", "course_name"])
        
        # 生成基础数据
        ids_df = spark.range(NUM_RECORDS).withColumnRenamed("id", "row_id")
        
        # 使用内置函数生成随机数据（避免 UDF 序列化问题）
        df = ids_df \
            .withColumn("record_id", F.expr("uuid()")) \
            .withColumn("student_idx", (F.rand() * NUM_STUDENTS).cast("int")) \
            .withColumn("course_idx", (F.rand() * len(course_data)).cast("int")) \
            .withColumn("behavior_idx", (F.rand() * len(behavior_types)).cast("int")) \
            .withColumn("days_offset", (F.rand() * 115).cast("int")) \
            .withColumn("hours", (F.rand() * 24).cast("int")) \
            .withColumn("minutes", (F.rand() * 60).cast("int")) \
            .withColumn("seconds", (F.rand() * 60).cast("int"))
        
        # 映射行为类型
        behavior_mapping = F.when(F.col("behavior_idx") == 0, "video_watch") \
            .when(F.col("behavior_idx") == 1, "homework_submit") \
            .when(F.col("behavior_idx") == 2, "quiz_complete") \
            .when(F.col("behavior_idx") == 3, "forum_post") \
            .otherwise("material_download")
        
        df = df.withColumn("behavior_type", behavior_mapping)
        
        # 根据行为类型生成时长
        duration_expr = F.when(F.col("behavior_type") == "video_watch", 
                               (F.rand() * 3300 + 300).cast("int")) \
            .when(F.col("behavior_type") == "homework_submit", 
                  (F.rand() * 5400 + 1800).cast("int")) \
            .when(F.col("behavior_type") == "quiz_complete", 
                  (F.rand() * 1200 + 600).cast("int")) \
            .when(F.col("behavior_type") == "forum_post", 
                  (F.rand() * 480 + 120).cast("int")) \
            .otherwise((F.rand() * 240 + 60).cast("int"))
        
        df = df.withColumn("duration", duration_expr)
        
        # 生成时间戳
        base_date = "2025-09-01"
        df = df.withColumn("timestamp", 
            F.date_format(
                F.expr(f"date_add('{base_date}', days_offset)") + 
                F.expr("make_interval(0, 0, 0, 0, hours, minutes, seconds)"),
                "yyyy-MM-dd HH:mm:ss"
            )
        )
        
        # 关联学生信息
        students_indexed = students_df.withColumn("idx", F.monotonically_increasing_id())
        # 重新索引确保连续
        students_indexed = spark.createDataFrame(
            [(i, s[0], s[1]) for i, s in enumerate(student_data)],
            ["idx", "student_id", "student_name"]
        )
        
        courses_indexed = spark.createDataFrame(
            [(i, c[0], c[1]) for i, c in enumerate(course_data)],
            ["idx", "course_id", "course_name"]
        )
        
        # Join 获取学生和课程信息
        df = df.join(students_indexed, df.student_idx == students_indexed.idx, "left") \
               .drop("idx", "student_idx")
        df = df.join(courses_indexed, df.course_idx == courses_indexed.idx, "left") \
               .drop("idx", "course_idx")
        
        # 选择最终列
        final_df = df.select(
            "record_id", "student_id", "student_name", 
            "course_id", "course_name", "behavior_type", 
            "duration", "timestamp"
        )
        
        # 缓存以提高性能
        final_df.cache()
        
        record_count = final_df.count()
        print(f"    生成 {record_count} 条记录")
        
        # 显示样例数据
        print("\n[2] 样例数据:")
        final_df.show(5, truncate=False)
        
        # 保存到 HDFS
        print("\n[3] 保存数据到 HDFS...")
        HDFS_NAMENODE = os.environ.get('HDFS_NAMENODE', '10.90.100.101:9000')
        hdfs_path = f"hdfs://{HDFS_NAMENODE}/user/learning_behavior/raw"
        
        # 写入数据
        final_df.write.mode("overwrite").json(hdfs_path)
        print(f"    数据已保存到: {hdfs_path}")
        
        # 统计信息
        print("\n[4] 数据统计:")
        print(f"    学生数: {final_df.select('student_id').distinct().count()}")
        print(f"    课程数: {final_df.select('course_id').distinct().count()}")
        print(f"    总记录: {record_count}")
        
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
