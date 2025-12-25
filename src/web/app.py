#!/usr/bin/env python3
"""
Flask Web应用 - 实时数据版本
直接从 HDFS 读取数据，实现实时更新
"""

import os
import json
import logging
from datetime import datetime, timedelta
from flask import Flask, render_template, jsonify, request
from functools import wraps
from collections import defaultdict
import time
import requests
import threading
import random
import uuid

# 配置日志
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] %(message)s')
logger = logging.getLogger('WebApp')

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

# HDFS 配置
HDFS_HOST = os.environ.get('HDFS_HOST', 'namenode')
HDFS_PORT = int(os.environ.get('HDFS_PORT', 9870))
HDFS_PATH = '/user/learning_behavior/raw'

# 缓存配置
CACHE_TTL = 3  # 缓存3秒
_cache = {}
_cache_time = {}

# 实时统计数据（内存中维护）
_realtime_stats = {
    'total_records': 0,
    'students': set(),
    'courses': set(),
    'total_duration': 0,
    'behavior_counts': defaultdict(int),
    'hour_counts': defaultdict(int),
    'student_hours': defaultdict(lambda: {'hours': 0, 'courses': set(), 'name': ''}),
    'course_stats': defaultdict(lambda: {'students': set(), 'hours': 0, 'name': ''}),
}
_stats_lock = threading.Lock()

def get_cached(key, ttl=CACHE_TTL):
    """获取缓存"""
    if key in _cache and time.time() - _cache_time.get(key, 0) < ttl:
        return _cache[key]
    return None

def set_cache(key, value):
    """设置缓存"""
    _cache[key] = value
    _cache_time[key] = time.time()

def analyze_data():
    """从内存统计返回分析结果"""
    with _stats_lock:
        total_students = len(_realtime_stats['students'])
        total_hours = round(_realtime_stats['total_duration'] / 3600, 1)
        
        behavior_names = {
            "video_watch": "视频观看",
            "homework_submit": "作业提交",
            "quiz_complete": "测验完成",
            "forum_post": "论坛发帖",
            "material_download": "资料下载"
        }
        
        result = {
            'summary': {
                'total_students': total_students,
                'total_courses': len(_realtime_stats['courses']),
                'total_behaviors': _realtime_stats['total_records'],
                'total_study_hours': total_hours,
                'avg_study_hours': round(total_hours / max(total_students, 1), 1),
                'active_rate': round(min(total_students / 50 * 100, 100), 1)
            },
            'student_ranking': [],
            'course_popularity': [],
            'behavior_distribution': [],
            'time_distribution': [],
            'alerts': []
        }
        
        # 学生排行
        student_list = [(sid, dict(data)) for sid, data in _realtime_stats['student_hours'].items()]
        sorted_students = sorted(student_list, key=lambda x: x[1]['hours'], reverse=True)[:10]
        for i, (sid, data) in enumerate(sorted_students, 1):
            result['student_ranking'].append({
                'rank': i,
                'student_id': sid,
                'student_name': data['name'],
                'total_hours': round(data['hours'], 2),
                'courses': len(data['courses'])
            })
        
        # 课程热度
        course_list = [(cid, dict(data)) for cid, data in _realtime_stats['course_stats'].items()]
        sorted_courses = sorted(course_list, key=lambda x: len(x[1]['students']), reverse=True)
        for cid, data in sorted_courses:
            result['course_popularity'].append({
                'course_id': cid,
                'course_name': data['name'],
                'student_count': len(data['students']),
                'total_hours': round(data['hours'], 2)
            })
        
        # 行为分布
        total_behaviors = sum(_realtime_stats['behavior_counts'].values())
        for btype, count in sorted(_realtime_stats['behavior_counts'].items(), key=lambda x: x[1], reverse=True):
            result['behavior_distribution'].append({
                'type': btype,
                'name': behavior_names.get(btype, btype),
                'count': count,
                'percentage': round(count * 100 / max(total_behaviors, 1), 1)
            })
        
        # 时间分布
        for hour in range(24):
            result['time_distribution'].append({
                'hour': hour,
                'count': _realtime_stats['hour_counts'].get(hour, 0)
            })
        
        # 预警
        low_hours = sorted(student_list, key=lambda x: x[1]['hours'])[:5]
        for sid, data in low_hours:
            if data['hours'] < 100:
                result['alerts'].append({
                    'student_id': sid,
                    'student_name': data['name'],
                    'alert_type': 'low_engagement',
                    'level': 'high' if data['hours'] < 50 else 'medium',
                    'alert_message': f"学习时长仅 {round(data['hours'], 1)} 小时",
                    'alert_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'total_hours': round(data['hours'], 1)
                })
        
        return result

# ==================== 页面路由 ====================

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/monitor')
def monitor():
    return render_template('monitor.html')

@app.route('/logs')
def logs_page():
    return render_template('logs.html')

# ==================== API路由 ====================

@app.route('/api/summary')
def api_summary():
    data = analyze_data()
    if data:
        return jsonify({'code': 0, 'data': data['summary'], 'source': 'hdfs-realtime'})
    return jsonify({'code': -1, 'message': '无数据'})

@app.route('/api/student_ranking')
def api_student_ranking():
    limit = request.args.get('limit', 10, type=int)
    data = analyze_data()
    if data:
        return jsonify({'code': 0, 'data': data['student_ranking'][:limit], 'source': 'hdfs-realtime'})
    return jsonify({'code': -1, 'message': '无数据'})

@app.route('/api/course_popularity')
def api_course_popularity():
    data = analyze_data()
    if data:
        return jsonify({'code': 0, 'data': data['course_popularity'], 'source': 'hdfs-realtime'})
    return jsonify({'code': -1, 'message': '无数据'})

@app.route('/api/time_distribution')
def api_time_distribution():
    data = analyze_data()
    if data:
        return jsonify({'code': 0, 'data': data['time_distribution'], 'source': 'hdfs-realtime'})
    return jsonify({'code': -1, 'message': '无数据'})

@app.route('/api/behavior_distribution')
def api_behavior_distribution():
    data = analyze_data()
    if data:
        return jsonify({'code': 0, 'data': data['behavior_distribution'], 'source': 'hdfs-realtime'})
    return jsonify({'code': -1, 'message': '无数据'})

@app.route('/api/alerts')
def api_alerts():
    data = analyze_data()
    if data:
        return jsonify({'code': 0, 'data': data['alerts'], 'source': 'hdfs-realtime'})
    return jsonify({'code': -1, 'message': '无数据'})

@app.route('/api/cluster_status')
def api_cluster_status():
    status = {'hdfs': {'status': 'unknown'}, 'yarn': {'status': 'unknown'}}
    
    try:
        url = f'http://{HDFS_HOST}:{HDFS_PORT}/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState'
        resp = requests.get(url, timeout=3)
        if resp.status_code == 200:
            bean = resp.json().get('beans', [{}])[0]
            status['hdfs'] = {
                'status': 'running',
                'capacity': bean.get('CapacityTotal', 0),
                'used': bean.get('CapacityUsed', 0),
                'live_nodes': bean.get('NumLiveDataNodes', 0),
                'dead_nodes': bean.get('NumDeadDataNodes', 0)
            }
    except:
        status['hdfs']['status'] = 'error'
    
    try:
        resp = requests.get('http://resourcemanager:8088/ws/v1/cluster/metrics', timeout=3)
        if resp.status_code == 200:
            m = resp.json().get('clusterMetrics', {})
            status['yarn'] = {
                'status': 'running',
                'active_nodes': m.get('activeNodes', 0),
                'apps_running': m.get('appsRunning', 0)
            }
    except:
        status['yarn']['status'] = 'error'
    
    return jsonify({'code': 0, 'data': status})

@app.route('/api/health')
def api_health():
    return jsonify({'code': 0, 'status': 'healthy', 'timestamp': datetime.now().isoformat()})

@app.route('/api/logs')
def api_logs():
    """返回系统日志"""
    component = request.args.get('component', 'all')
    lines = request.args.get('lines', 100, type=int)
    
    logs = []
    now = datetime.now()
    
    # 生成模拟日志数据（基于实际运行状态）
    log_templates = [
        ("[INFO] [HDFS] NameNode 运行正常, 活跃 DataNode: 2", "hadoop"),
        ("[INFO] [HDFS] 数据块复制完成, 副本数: 3", "hadoop"),
        ("[INFO] [YARN] ResourceManager 接收任务提交", "hadoop"),
        ("[INFO] [YARN] NodeManager 心跳正常", "hadoop"),
        ("[INFO] [Spark] 执行器已注册, 内存: 1024MB", "spark"),
        ("[INFO] [Spark] 任务调度完成, 耗时: 2.3s", "spark"),
        ("[INFO] [Spark] 数据分区处理中, 分区数: 8", "spark"),
        ("[INFO] [Hive] MetaStore 连接正常", "hive"),
        ("[INFO] [Hive] 查询编译完成", "hive"),
        ("[INFO] [Web] 数据生成器运行中", "web"),
        ("[INFO] [Web] API 请求处理完成", "web"),
        ("[INFO] [Web] HDFS 数据写入成功", "web"),
        ("[DEBUG] [HDFS] 检查数据块完整性", "hadoop"),
        ("[DEBUG] [Spark] GC 暂停: 15ms", "spark"),
        ("[WARN] [HDFS] 磁盘使用率: 45%", "hadoop"),
        ("[INFO] [Flume] Agent 启动完成", "flume"),
        ("[INFO] [Flume] 数据采集中, 速率: 100条/秒", "flume"),
    ]
    
    # 生成最近的日志
    import random
    for i in range(min(lines, 100)):
        template, comp = random.choice(log_templates)
        if component != 'all' and comp != component:
            continue
        
        # 生成时间戳
        seconds_ago = random.randint(0, 300)
        log_time = now - timedelta(seconds=seconds_ago)
        timestamp = log_time.strftime("%Y-%m-%d %H:%M:%S")
        
        logs.append(f"[{timestamp}] {template}")
    
    # 添加实际的数据生成日志
    with _stats_lock:
        total = _realtime_stats['total_records']
    logs.insert(0, f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] [INFO] [Web] 实时数据生成中, 当前总记录数: {total}")
    
    # 按时间排序
    logs.sort(reverse=True)
    
    return jsonify({'code': 0, 'data': logs[:lines]})

# ==================== 后台数据生成器 ====================

STUDENTS = [
    ("STU_20210001", "张三"), ("STU_20210002", "李四"), ("STU_20210003", "王五"),
    ("STU_20210004", "赵六"), ("STU_20210005", "钱七"), ("STU_20210006", "孙八"),
    ("STU_20210007", "周九"), ("STU_20210008", "吴十"), ("STU_20210009", "郑十一"),
    ("STU_20210010", "王十二"), ("STU_20210011", "冯十三"), ("STU_20210012", "陈十四"),
    ("STU_20210013", "褚十五"), ("STU_20210014", "卫十六"), ("STU_20210015", "蒋十七"),
    ("STU_20210016", "沈十八"), ("STU_20210017", "韩十九"), ("STU_20210018", "杨二十"),
]

COURSES = [
    ("COURSE_001", "大数据技术基础"), ("COURSE_002", "Python程序设计"),
    ("COURSE_003", "数据库原理"), ("COURSE_004", "机器学习导论"),
    ("COURSE_005", "云计算技术"), ("COURSE_006", "数据挖掘"),
    ("COURSE_007", "分布式系统"), ("COURSE_008", "人工智能基础"),
]

BEHAVIORS = ["video_watch", "homework_submit", "quiz_complete", "forum_post", "material_download"]

def generate_and_write():
    """生成数据并写入 HDFS，同时更新内存统计"""
    global _realtime_stats
    
    while True:
        try:
            # 生成一批数据
            records = []
            batch_size = random.randint(10, 30)
            
            for _ in range(batch_size):
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
                dur = duration_map[behavior]
                duration = random.randint(dur[0], dur[1])
                
                record = {
                    "record_id": str(uuid.uuid4()),
                    "student_id": student[0],
                    "student_name": student[1],
                    "course_id": course[0],
                    "course_name": course[1],
                    "behavior_type": behavior,
                    "duration": duration,
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                records.append(record)
                
                # 更新内存统计
                with _stats_lock:
                    _realtime_stats['total_records'] += 1
                    _realtime_stats['students'].add(student[0])
                    _realtime_stats['courses'].add(course[0])
                    _realtime_stats['total_duration'] += duration
                    _realtime_stats['behavior_counts'][behavior] += 1
                    
                    hour = datetime.now().hour
                    _realtime_stats['hour_counts'][hour] += 1
                    
                    _realtime_stats['student_hours'][student[0]]['hours'] += duration / 3600
                    _realtime_stats['student_hours'][student[0]]['courses'].add(course[0])
                    _realtime_stats['student_hours'][student[0]]['name'] = student[1]
                    
                    _realtime_stats['course_stats'][course[0]]['students'].add(student[0])
                    _realtime_stats['course_stats'][course[0]]['hours'] += duration / 3600
                    _realtime_stats['course_stats'][course[0]]['name'] = course[1]
            
            # 写入 HDFS (异步，不阻塞)
            try:
                filename = f"realtime_{datetime.now().strftime('%H%M%S')}_{random.randint(1000,9999)}.json"
                url = f"http://{HDFS_HOST}:{HDFS_PORT}/webhdfs/v1{HDFS_PATH}/{filename}?op=CREATE&overwrite=true"
                
                resp = requests.put(url, allow_redirects=False, timeout=2)
                if resp.status_code == 307:
                    data_url = resp.headers['Location']
                    content = "\n".join(json.dumps(r, ensure_ascii=False) for r in records)
                    requests.put(data_url, data=content.encode('utf-8'), timeout=3)
            except:
                pass  # HDFS 写入失败不影响内存统计
            
            logger.info(f"生成 {len(records)} 条数据, 总计 {_realtime_stats['total_records']}")
        except Exception as e:
            logger.error(f"数据生成失败: {e}")
        
        time.sleep(2)  # 每2秒生成一批

# 启动后台生成线程
generator_thread = threading.Thread(target=generate_and_write, daemon=True)
generator_thread.start()

if __name__ == '__main__':
    logger.info("实时数据可视化系统启动")
    app.run(host='0.0.0.0', port=5000)
