# ============================================
# 学习行为数据生成器
# 生成模拟的大学生线上学习行为日志
# ============================================

import json
import random
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from .logger import setup_logger, LogContext

class LearningBehaviorGenerator:
    """学习行为数据生成器"""
    
    # 行为类型枚举
    BEHAVIOR_TYPES = [
        'video_watch',       # 视频观看
        'homework_submit',   # 作业提交
        'quiz_complete',     # 测验完成
        'forum_post',        # 论坛发帖
        'material_download'  # 资料下载
    ]
    
    # 设备类型
    DEVICE_TYPES = ['PC', 'Mobile', 'Tablet']
    
    # 模拟学生数据
    STUDENTS = [
        {'id': f'STU_2021{str(i).zfill(4)}', 'name': name}
        for i, name in enumerate([
            '张三', '李四', '王五', '赵六', '钱七', '孙八', '周九', '吴十',
            '郑十一', '王十二', '冯十三', '陈十四', '褚十五', '卫十六',
            '蒋十七', '沈十八', '韩十九', '杨二十', '朱二一', '秦二二',
            '尤二三', '许二四', '何二五', '吕二六', '施二七', '张二八',
            '孔二九', '曹三十', '严三一', '华三二', '金三三', '魏三四',
            '陶三五', '姜三六', '戚三七', '谢三八', '邹三九', '喻四十'
        ], 1)
    ]
    
    # 模拟课程数据
    COURSES = [
        {'id': 'COURSE_001', 'name': '大数据技术基础', 'chapters': 12},
        {'id': 'COURSE_002', 'name': 'Python程序设计', 'chapters': 15},
        {'id': 'COURSE_003', 'name': '数据库原理', 'chapters': 10},
        {'id': 'COURSE_004', 'name': '机器学习导论', 'chapters': 14},
        {'id': 'COURSE_005', 'name': '云计算技术', 'chapters': 11},
        {'id': 'COURSE_006', 'name': '数据挖掘', 'chapters': 13},
        {'id': 'COURSE_007', 'name': '分布式系统', 'chapters': 9},
        {'id': 'COURSE_008', 'name': '人工智能基础', 'chapters': 16},
    ]
    
    def __init__(self, log_file: str = None):
        """
        初始化数据生成器
        
        Args:
            log_file: 日志文件路径
        """
        self.logger = setup_logger('DataGenerator', log_file)
        self.logger.info("数据生成器初始化完成")
        self.logger.info(f"学生数量: {len(self.STUDENTS)}")
        self.logger.info(f"课程数量: {len(self.COURSES)}")
        self.logger.info(f"行为类型: {', '.join(self.BEHAVIOR_TYPES)}")
    
    def _generate_ip(self) -> str:
        """生成随机IP地址"""
        return f"192.168.{random.randint(1, 254)}.{random.randint(1, 254)}"
    
    def _generate_duration(self, behavior_type: str) -> int:
        """
        根据行为类型生成合理的持续时长（秒）
        
        Args:
            behavior_type: 行为类型
            
        Returns:
            持续时长（秒）
        """
        duration_ranges = {
            'video_watch': (300, 3600),      # 5分钟到1小时
            'homework_submit': (600, 7200),   # 10分钟到2小时
            'quiz_complete': (300, 1800),     # 5分钟到30分钟
            'forum_post': (60, 600),          # 1分钟到10分钟
            'material_download': (10, 120)    # 10秒到2分钟
        }
        min_dur, max_dur = duration_ranges.get(behavior_type, (60, 600))
        return random.randint(min_dur, max_dur)
    
    def _generate_score(self, behavior_type: str) -> Optional[float]:
        """
        生成得分（仅适用于测验和作业）
        
        Args:
            behavior_type: 行为类型
            
        Returns:
            得分或None
        """
        if behavior_type in ['quiz_complete', 'homework_submit']:
            # 生成正态分布的分数，均值75，标准差15
            score = random.gauss(75, 15)
            return round(max(0, min(100, score)), 1)
        return None
    
    def generate_single_log(self, timestamp: datetime = None) -> Dict:
        """
        生成单条学习行为日志
        
        Args:
            timestamp: 时间戳，如果为None则使用当前时间
            
        Returns:
            日志记录字典
        """
        if timestamp is None:
            timestamp = datetime.now()
        
        student = random.choice(self.STUDENTS)
        course = random.choice(self.COURSES)
        behavior_type = random.choice(self.BEHAVIOR_TYPES)
        
        log_entry = {
            'student_id': student['id'],
            'student_name': student['name'],
            'course_id': course['id'],
            'course_name': course['name'],
            'behavior_type': behavior_type,
            'duration': self._generate_duration(behavior_type),
            'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'ip_address': self._generate_ip(),
            'device_type': random.choice(self.DEVICE_TYPES),
            'chapter_id': f"CH_{str(random.randint(1, course['chapters'])).zfill(3)}",
            'score': self._generate_score(behavior_type)
        }
        
        return log_entry
    
    def generate_logs(self, count: int, 
                      start_time: datetime = None,
                      end_time: datetime = None) -> List[Dict]:
        """
        生成指定数量的学习行为日志
        
        Args:
            count: 生成日志条数
            start_time: 开始时间，默认为7天前
            end_time: 结束时间，默认为当前时间
            
        Returns:
            日志记录列表
        """
        if end_time is None:
            end_time = datetime.now()
        if start_time is None:
            start_time = end_time - timedelta(days=7)
        
        with LogContext(self.logger, f"生成 {count} 条学习行为日志"):
            logs = []
            time_range = (end_time - start_time).total_seconds()
            
            for i in range(count):
                # 生成随机时间戳
                random_seconds = random.uniform(0, time_range)
                timestamp = start_time + timedelta(seconds=random_seconds)
                
                # 模拟学习时间分布（白天更多）
                hour = timestamp.hour
                if 8 <= hour <= 22:  # 白天时间
                    if random.random() < 0.3:  # 30%概率跳过
                        random_seconds = random.uniform(0, time_range)
                        timestamp = start_time + timedelta(seconds=random_seconds)
                
                log_entry = self.generate_single_log(timestamp)
                logs.append(log_entry)
                
                # 每1000条记录一次进度
                if (i + 1) % 1000 == 0:
                    self.logger.debug(f"已生成 {i + 1}/{count} 条日志")
            
            # 按时间排序
            logs.sort(key=lambda x: x['timestamp'])
            
            self.logger.info(f"日志生成完成，共 {len(logs)} 条")
            self.logger.info(f"时间范围: {start_time.strftime('%Y-%m-%d %H:%M:%S')} 至 {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # 统计行为类型分布
            behavior_counts = {}
            for log in logs:
                bt = log['behavior_type']
                behavior_counts[bt] = behavior_counts.get(bt, 0) + 1
            
            self.logger.info("行为类型分布:")
            for bt, cnt in sorted(behavior_counts.items()):
                self.logger.info(f"  - {bt}: {cnt} ({cnt/len(logs)*100:.1f}%)")
            
            return logs
    
    def write_to_file(self, logs: List[Dict], filepath: str, 
                      format: str = 'json') -> str:
        """
        将日志写入文件
        
        Args:
            logs: 日志记录列表
            filepath: 输出文件路径
            format: 输出格式，'json' 或 'csv'
            
        Returns:
            实际写入的文件路径
        """
        # 确保目录存在
        dir_path = os.path.dirname(filepath)
        if dir_path and not os.path.exists(dir_path):
            os.makedirs(dir_path)
            self.logger.info(f"创建目录: {dir_path}")
        
        with LogContext(self.logger, f"写入日志到 {filepath}"):
            if format.lower() == 'json':
                # JSON Lines格式，每行一条记录
                with open(filepath, 'w', encoding='utf-8') as f:
                    for log in logs:
                        f.write(json.dumps(log, ensure_ascii=False) + '\n')
            
            elif format.lower() == 'csv':
                import csv
                with open(filepath, 'w', encoding='utf-8', newline='') as f:
                    if logs:
                        writer = csv.DictWriter(f, fieldnames=logs[0].keys())
                        writer.writeheader()
                        writer.writerows(logs)
            
            else:
                raise ValueError(f"不支持的格式: {format}")
            
            file_size = os.path.getsize(filepath)
            self.logger.info(f"文件写入完成: {filepath}")
            self.logger.info(f"文件大小: {file_size / 1024:.2f} KB")
            self.logger.info(f"记录数量: {len(logs)}")
            
            return filepath
    
    def generate_and_save(self, count: int, output_dir: str,
                          filename_prefix: str = 'learning_behavior',
                          format: str = 'json') -> str:
        """
        生成日志并保存到文件
        
        Args:
            count: 生成日志条数
            output_dir: 输出目录
            filename_prefix: 文件名前缀
            format: 输出格式
            
        Returns:
            输出文件路径
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{filename_prefix}_{timestamp}.{format}"
        filepath = os.path.join(output_dir, filename)
        
        logs = self.generate_logs(count)
        return self.write_to_file(logs, filepath, format)


if __name__ == '__main__':
    # 测试代码
    generator = LearningBehaviorGenerator()
    
    # 生成10条测试日志
    logs = generator.generate_logs(10)
    for log in logs:
        print(json.dumps(log, ensure_ascii=False, indent=2))
