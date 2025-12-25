#!/usr/bin/env python3
"""
轻量级 HDFS 数据生成器
直接通过 WebHDFS API 写入数据，不占用 Spark 资源
"""

import json
import random
import uuid
import time
import requests
from datetime import datetime
import sys

# HDFS WebHDFS 配置
HDFS_HOST = "namenode"
HDFS_PORT = 9870
HDFS_PATH = "/user/learning_behavior/raw"

# 学生数据
STUDENTS = [
    ("STU_20210001", "张三"), ("STU_20210002", "李四"), ("STU_20210003", "王五"),
    ("STU_20210004", "赵六"), ("STU_20210005", "钱七"), ("STU_20210006", "孙八"),
    ("STU_20210007", "周九"), ("STU_20210008", "吴十"), ("STU_20210009", "郑十一"),
    ("STU_20210010", "王十二"), ("STU_20210011", "冯十三"), ("STU_20210012", "陈十四"),
    ("STU_20210013", "褚十五"), ("STU_20210014", "卫十六"), ("STU_20210015", "蒋十七"),
    ("STU_20210016", "沈十八"), ("STU_20210017", "韩十九"), ("STU_20210018", "杨二十"),
    ("STU_20210019", "朱二一"), ("STU_20210020", "秦二二"),
]

COURSES = [
    ("COURSE_001", "大数据技术基础"), ("COURSE_002", "Python程序设计"),
    ("COURSE_003", "数据库原理"), ("COURSE_004", "机器学习导论"),
    ("COURSE_005", "云计算技术"), ("COURSE_006", "数据挖掘"),
    ("COURSE_007", "分布式系统"), ("COURSE_008", "人工智能基础"),
]

BEHAVIORS = ["video_watch", "homework_submit", "quiz_complete", "forum_post", "material_download"]

def generate_record():
    student = random.choice(STUDENTS)
    course = random.choice(COURSES)
    behavior = random.choice(BEHAVIORS)
    
    duration_map = {
        "video_watch": (300, 3600),
        "homework_submit": (1800, 7200),
        "quiz_complete": (600, 1800),
        "forum_post": (120, 600),
        "material_download": (60, 300),
    }
    dur_range = duration_map[behavior]
    
    return {
        "record_id": str(uuid.uuid4()),
        "student_id": student[0],
        "student_name": student[1],
        "course_id": course[0],
        "course_name": course[1],
        "behavior_type": behavior,
        "duration": random.randint(dur_range[0], dur_range[1]),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

def write_to_hdfs(records):
    """通过 WebHDFS 写入数据"""
    filename = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{random.randint(1000,9999)}.json"
    filepath = f"{HDFS_PATH}/{filename}"
    
    # 准备数据 - 每行一个 JSON
    content = "\n".join(json.dumps(r, ensure_ascii=False) for r in records)
    
    # 创建文件
    url = f"http://{HDFS_HOST}:{HDFS_PORT}/webhdfs/v1{filepath}?op=CREATE&overwrite=true"
    resp = requests.put(url, allow_redirects=False)
    
    if resp.status_code == 307:
        # 跟随重定向写入数据
        data_url = resp.headers['Location']
        resp2 = requests.put(data_url, data=content.encode('utf-8'), 
                            headers={'Content-Type': 'application/octet-stream'})
        return resp2.status_code == 201
    return False

def main():
    batch_size = int(sys.argv[1]) if len(sys.argv) > 1 else 20
    interval = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    
    print(f"HDFS 数据生成器启动 - 每 {interval} 秒生成 {batch_size} 条")
    print("=" * 50)
    
    total = 0
    batch = 0
    
    try:
        while True:
            batch += 1
            records = [generate_record() for _ in range(batch_size)]
            
            if write_to_hdfs(records):
                total += batch_size
                print(f"[{datetime.now().strftime('%H:%M:%S')}] 批次 {batch}: +{batch_size} 条, 累计 {total}")
            else:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] 写入失败")
            
            time.sleep(interval)
    except KeyboardInterrupt:
        print(f"\n停止! 共生成 {total} 条")

if __name__ == "__main__":
    main()
