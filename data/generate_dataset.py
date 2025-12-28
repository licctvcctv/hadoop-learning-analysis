#!/usr/bin/env python3
"""
生成高度真实的大学生线上课程学习行为数据集
2000行 CSV 格式
"""

import csv
import random
from datetime import datetime, timedelta

# ============================================
# 配置参数
# ============================================
NUM_RECORDS = 2000
START_DATE = datetime(2025, 9, 1)  # 秋季学期开始
END_DATE = datetime(2025, 12, 25)   # 学期末
OUTPUT_FILE = "data/learning_behavior_dataset.csv"

# ============================================
# 基础数据
# ============================================

# 真实的中文姓名
STUDENT_NAMES = [
    "张伟", "李娜", "王强", "刘洋", "陈静", "杨敏", "赵杰", "黄磊",
    "周婷", "吴刚", "徐欢", "孙亮", "马超", "胡芳", "郭明", "林霞",
    "何勇", "高艳", "罗军", "郑丽", "梁涛", "谢琳", "宋涛", "唐敏",
    "韩冰", "冯华", "于平", "董鹏", "萧然", "程浩", "曹颖", "袁伟",
    "邓秀英", "许文静", "傅明", "沈晓东", "曾强", "彭丽", "吕华", "苏勇",
    "卢明", "蒋欣", "蔡伟", "贾芳", "丁峰", "魏静", "薛敏", "叶强",
    "阎丽", "余明", "潘华", "杜娟", "戴勇", "夏琳", "钟涛", "汪敏",
    "田强", "任丽", "姜华", "范娟", "方明", "石涛", "姚丽", "谭勇",
    "廖敏", "邹华", "熊涛", "金丽", "陆明", "郝芳", "孔亮", "白静",
    "崔敏", "康华", "毛涛", "邱丽", "秦勇", "江敏", "史芳", "顾强"
]

# 课程数据
COURSES = [
    {"id": "COURSE_001", "name": "大数据技术基础", "chapters": 12, "credits": 3.0},
    {"id": "COURSE_002", "name": "Python程序设计", "chapters": 15, "credits": 4.0},
    {"id": "COURSE_003", "name": "数据库原理", "chapters": 10, "credits": 3.0},
    {"id": "COURSE_004", "name": "机器学习导论", "chapters": 14, "credits": 4.0},
    {"id": "COURSE_005", "name": "云计算技术", "chapters": 11, "credits": 3.0},
    {"id": "COURSE_006", "name": "数据挖掘", "chapters": 13, "credits": 3.0},
    {"id": "COURSE_007", "name": "分布式系统", "chapters": 9, "credits": 2.0},
    {"id": "COURSE_008", "name": "人工智能基础", "chapters": 16, "credits": 4.0},
]

# 行为类型及权重（视频观看最常见）
BEHAVIOR_TYPES = [
    {"type": "video_watch", "weight": 45},        # 视频观看 45%
    {"type": "homework_submit", "weight": 15},    # 作业提交 15%
    {"type": "quiz_complete", "weight": 20},      # 测验完成 20%
    {"type": "forum_post", "weight": 10},         # 论坛发帖 10%
    {"type": "material_download", "weight": 10},  # 资料下载 10%
]

# 设备类型（移动端使用越来越多）
DEVICE_TYPES = [
    {"type": "PC", "weight": 40},
    {"type": "Mobile", "weight": 45},
    {"type": "Tablet", "weight": 15}
]

# 浏览器类型
BROWSERS = ["Chrome", "Safari", "Firefox", "Edge", "MicroMessenger"]

# 操作系统
OPERATING_SYSTEMS = ["Windows 10", "Windows 11", "macOS", "iOS", "Android", "Ubuntu"]

# IP地址段（模拟校园网）
IP_PREFIXES = ["10.1.", "10.2.", "10.3.", "192.168.1.", "192.168.2."]

# ============================================
# 辅助函数
# ============================================

def weighted_choice(items):
    """根据权重随机选择"""
    total_weight = sum(item["weight"] for item in items)
    r = random.uniform(0, total_weight)
    for item in items:
        r -= item["weight"]
        if r <= 0:
            return item["type"]
    return items[-1]["type"]

def generate_student_id(index):
    """生成学生ID"""
    return f"STU_202{random.randint(0, 2)}{str(index).zfill(4)}"

def generate_ip():
    """生成IP地址"""
    prefix = random.choice(IP_PREFIXES)
    return f"{prefix}{random.randint(1, 254)}.{random.randint(1, 254)}"

def generate_timestamp(start, end):
    """
    生成带学习时间分布特征的时间戳
    - 工作日 8:00-22:00 更多学习行为
    - 周末分布相对均匀
    - 考试周（第15-16周）学习活动增加
    """
    # 生成基础随机时间
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    base_time = start + timedelta(seconds=random_seconds)

    hour = base_time.hour
    weekday = base_time.weekday()  # 0=周一, 6=周日
    week_num = (base_time - start).days // 7 + 1

    # 学习时间偏好权重
    time_weight = 1.0

    # 工作日白天时段权重更高
    if weekday < 5:  # 工作日
        if 8 <= hour <= 11:
            time_weight = 3.0  # 上午学习高峰
        elif 14 <= hour <= 17:
            time_weight = 2.5  # 下午学习
        elif 19 <= hour <= 22:
            time_weight = 3.5  # 晚上学习高峰
        elif 0 <= hour <= 6:
            time_weight = 0.1  # 凌晨极少学习
    else:  # 周末
        if 9 <= hour <= 23:
            time_weight = 1.8
        elif 0 <= hour <= 7:
            time_weight = 0.3

    # 考试周权重增加
    if week_num in [15, 16]:
        time_weight *= 1.5

    # 根据权重决定是否重新采样
    if random.random() > (time_weight / 4.0):
        # 重新生成时间
        return generate_timestamp(start, end)

    return base_time

def generate_duration(behavior_type):
    """根据行为类型生成合理的持续时长"""
    if behavior_type == "video_watch":
        # 视频观看：5分钟到2小时，大部分在30-60分钟
        return int(random.triangular(300, 7200, 2400))
    elif behavior_type == "homework_submit":
        # 作业提交：15分钟到4小时
        return int(random.triangular(900, 14400, 5400))
    elif behavior_type == "quiz_complete":
        # 测验：5分钟到1小时
        return int(random.triangular(300, 3600, 1800))
    elif behavior_type == "forum_post":
        # 论坛发帖：1分钟到30分钟
        return int(random.triangular(60, 1800, 300))
    elif behavior_type == "material_download":
        # 资料下载：10秒到5分钟
        return int(random.triangular(10, 300, 60))
    return random.randint(60, 3600)

def generate_score(behavior_type):
    """生成成绩（正态分布，均值75，有挂科情况）"""
    if behavior_type in ["quiz_complete", "homework_submit"]:
        # 正态分布，均值75，标准差12
        score = random.gauss(75, 12)
        # 有5%概率不及格
        if random.random() < 0.05:
            score = random.uniform(40, 59)
        return round(max(0, min(100, score)), 1)
    return None

def generate_chapter(course, timestamp):
    """根据时间生成合理的章节进度"""
    week_num = ((timestamp - START_DATE).days // 7) + 1
    total_chapters = course["chapters"]
    # 假设每周进度大致均匀，有一定超前或滞后
    expected_chapter = min(total_chapters, (week_num * total_chapters) // 16)
    # 允许±2章的波动
    chapter = max(1, min(total_chapters, expected_chapter + random.randint(-2, 2)))
    return f"CH_{str(chapter).zfill(3)}"

# ============================================
# 数据生成
# ============================================

def generate_dataset():
    """生成数据集"""

    # 生成学生列表
    students = []
    for i, name in enumerate(STUDENT_NAMES):
        students.append({
            "id": generate_student_id(i),
            "name": name,
            "major": random.choice(["计算机科学与技术", "软件工程", "数据科学", "人工智能", "网络工程"])
        })

    print(f"开始生成 {NUM_RECORDS} 条学习行为记录...")
    print(f"时间范围: {START_DATE.date()} 至 {END_DATE.date()}")
    print(f"学生数量: {len(students)}")
    print(f"课程数量: {len(COURSES)}")

    records = []

    # 生成记录
    for i in range(NUM_RECORDS):
        student = random.choice(students)
        course = random.choice(COURSES)
        behavior_type = weighted_choice(BEHAVIOR_TYPES)
        device_type = weighted_choice(DEVICE_TYPES)

        timestamp = generate_timestamp(START_DATE, END_DATE)

        record = {
            "record_id": f"REC_{timestamp.strftime('%Y%m%d%H%M%S')}_{str(i).zfill(4)}",
            "student_id": student["id"],
            "student_name": student["name"],
            "student_major": student["major"],
            "course_id": course["id"],
            "course_name": course["name"],
            "course_credits": course["credits"],
            "behavior_type": behavior_type,
            "duration": generate_duration(behavior_type),
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "date": timestamp.strftime("%Y-%m-%d"),
            "hour": timestamp.hour,
            "weekday": timestamp.weekday(),
            "week_number": ((timestamp - START_DATE).days // 7) + 1,
            "ip_address": generate_ip(),
            "device_type": device_type,
            "browser": random.choice(BROWSERS),
            "os": random.choice(OPERATING_SYSTEMS),
            "chapter_id": generate_chapter(course, timestamp),
            "score": generate_score(behavior_type),
            "is_weekend": 1 if timestamp.weekday() >= 5 else 0,
            "is_exam_week": 1 if ((timestamp - START_DATE).days // 7 + 1) in [15, 16] else 0
        }

        records.append(record)

        if (i + 1) % 500 == 0:
            print(f"  已生成 {i + 1}/{NUM_RECORDS} 条记录")

    # 按时间排序
    records.sort(key=lambda x: x["timestamp"])

    return records

# ============================================
# 主程序
# ============================================

if __name__ == "__main__":

    # 生成数据
    records = generate_dataset()

    # 写入CSV文件
    print(f"\n正在写入CSV文件: {OUTPUT_FILE}")

    with open(OUTPUT_FILE, 'w', encoding='utf-8-sig', newline='') as f:
        fieldnames = records[0].keys()
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    print(f"数据集生成完成！")
    print(f"  文件路径: {OUTPUT_FILE}")
    print(f"  记录数量: {len(records)}")

    # 统计信息
    print("\n数据统计:")
    print(f"  学生数量: {len(set(r['student_id'] for r in records))}")
    print(f"  课程数量: {len(set(r['course_id'] for r in records))}")

    behavior_counts = {}
    for r in records:
        bt = r["behavior_type"]
        behavior_counts[bt] = behavior_counts.get(bt, 0) + 1

    print("\n行为类型分布:")
    for bt, count in sorted(behavior_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"  {bt}: {count} ({count/len(records)*100:.1f}%)")

    # 时间分布
    weekday_records = sum(1 for r in records if r["is_weekend"] == 0)
    weekend_records = sum(1 for r in records if r["is_weekend"] == 1)
    print(f"\n工作日记录: {weekday_records} ({weekday_records/len(records)*100:.1f}%)")
    print(f"周末记录: {weekend_records} ({weekend_records/len(records)*100:.1f}%)")

    # 设备分布
    device_counts = {}
    for r in records:
        dt = r["device_type"]
        device_counts[dt] = device_counts.get(dt, 0) + 1
    print("\n设备类型分布:")
    for dt, count in sorted(device_counts.items()):
        print(f"  {dt}: {count} ({count/len(records)*100:.1f}%)")

    print("\n前5条记录预览:")
    for i, r in enumerate(records[:5]):
        print(f"  {i+1}. {r['student_name']} - {r['course_name']} - {r['behavior_type']} - {r['timestamp']}")
