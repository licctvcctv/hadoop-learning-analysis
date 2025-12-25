# ============================================
# Property 4: 学习时长统计正确性测试
# 验证学生总学习时长 = 该学生所有duration字段累加和
# ============================================

import pytest
from hypothesis import given, strategies as st, settings, assume
from collections import defaultdict
from functools import reduce
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from data_generator.generator import LearningBehaviorGenerator


def aggregate_by_student(logs):
    """使用reduce实现的聚合函数 - 模拟Spark的reduceByKey"""
    result = {}
    for log in logs:
        sid = log['student_id']
        if sid not in result:
            result[sid] = {
                'total_duration': 0,
                'courses': set(),
                'behaviors': defaultdict(int),
                'scores': []
            }
        result[sid]['total_duration'] += log['duration']
        result[sid]['courses'].add(log['course_id'])
        result[sid]['behaviors'][log['behavior_type']] += 1
        if log['score'] is not None:
            result[sid]['scores'].append(log['score'])
    return result


def simple_sum_duration(logs, student_id):
    """简单的过滤求和 - 作为对照实现"""
    return sum(log['duration'] for log in logs if log['student_id'] == student_id)


class TestStudyDurationProperties:
    """学习时长统计属性测试"""
    
    @given(st.integers(min_value=10, max_value=500))
    @settings(max_examples=50)
    def test_aggregation_equals_filter_sum(self, count):
        """
        Property 4.1: 聚合结果等于过滤求和结果
        
        验证两种不同的计算方式得到相同结果
        """
        generator = LearningBehaviorGenerator()
        logs = generator.generate_logs(count)
        
        # 方法1: 聚合
        aggregated = aggregate_by_student(logs)
        
        # 方法2: 过滤求和
        for student_id, data in aggregated.items():
            filter_sum = simple_sum_duration(logs, student_id)
            
            assert data['total_duration'] == filter_sum, \
                f"学生 {student_id}: 聚合={data['total_duration']}, 过滤求和={filter_sum}"
    
    @given(st.integers(min_value=10, max_value=500))
    @settings(max_examples=50)
    def test_total_duration_is_sum_of_parts(self, count):
        """
        Property 4.2: 总时长等于各行为类型时长之和
        
        验证按行为类型分解后的时长总和等于总时长
        """
        generator = LearningBehaviorGenerator()
        logs = generator.generate_logs(count)
        
        # 按学生和行为类型统计
        student_behavior_durations = defaultdict(lambda: defaultdict(int))
        student_total = defaultdict(int)
        
        for log in logs:
            student_behavior_durations[log['student_id']][log['behavior_type']] += log['duration']
            student_total[log['student_id']] += log['duration']
        
        for student_id in student_total:
            parts_sum = sum(student_behavior_durations[student_id].values())
            total = student_total[student_id]
            
            assert parts_sum == total, \
                f"学生 {student_id}: 分项和={parts_sum}, 总计={total}"
    
    @given(st.integers(min_value=10, max_value=500))
    @settings(max_examples=50)
    def test_duration_additivity(self, count):
        """
        Property 4.3: 时长的可加性
        
        将日志分成两部分，各部分时长之和应等于总时长
        """
        generator = LearningBehaviorGenerator()
        logs = generator.generate_logs(count)
        
        # 分成两部分
        mid = len(logs) // 2
        part1 = logs[:mid]
        part2 = logs[mid:]
        
        # 分别计算
        total_all = sum(log['duration'] for log in logs)
        total_part1 = sum(log['duration'] for log in part1)
        total_part2 = sum(log['duration'] for log in part2)
        
        assert total_all == total_part1 + total_part2, \
            f"可加性失败: {total_all} != {total_part1} + {total_part2}"
    
    @given(st.integers(min_value=10, max_value=500))
    @settings(max_examples=50)
    def test_course_count_is_unique_count(self, count):
        """
        Property 4.4: 课程数等于去重后的课程ID数量
        """
        generator = LearningBehaviorGenerator()
        logs = generator.generate_logs(count)
        
        aggregated = aggregate_by_student(logs)
        
        for student_id, data in aggregated.items():
            # 使用集合去重
            unique_courses = set(
                log['course_id'] for log in logs 
                if log['student_id'] == student_id
            )
            
            assert data['courses'] == unique_courses, \
                f"学生 {student_id} 课程集合不一致"
    
    @given(st.integers(min_value=10, max_value=500))
    @settings(max_examples=50)
    def test_average_score_precision(self, count):
        """
        Property 4.5: 平均分计算精度
        
        验证平均分计算的数值精度
        """
        generator = LearningBehaviorGenerator()
        logs = generator.generate_logs(count)
        
        aggregated = aggregate_by_student(logs)
        
        for student_id, data in aggregated.items():
            scores = data['scores']
            if not scores:
                continue
            
            # 方法1: 直接计算
            avg1 = sum(scores) / len(scores)
            
            # 方法2: 使用reduce
            avg2 = reduce(lambda a, b: a + b, scores) / len(scores)
            
            # 精度误差应该很小
            assert abs(avg1 - avg2) < 1e-10, \
                f"学生 {student_id} 平均分计算精度问题: {avg1} vs {avg2}"
    
    @given(st.integers(min_value=10, max_value=500))
    @settings(max_examples=50)
    def test_behavior_count_consistency(self, count):
        """
        Property 4.6: 行为计数一致性
        
        各行为类型计数之和等于该学生的总记录数
        """
        generator = LearningBehaviorGenerator()
        logs = generator.generate_logs(count)
        
        aggregated = aggregate_by_student(logs)
        
        for student_id, data in aggregated.items():
            # 行为计数之和
            behavior_total = sum(data['behaviors'].values())
            
            # 该学生的记录数
            record_count = sum(1 for log in logs if log['student_id'] == student_id)
            
            assert behavior_total == record_count, \
                f"学生 {student_id}: 行为计数和={behavior_total}, 记录数={record_count}"
    
    @given(st.integers(min_value=100, max_value=500))
    @settings(max_examples=30)
    def test_order_independence(self, count):
        """
        Property 4.7: 计算结果与日志顺序无关
        """
        generator = LearningBehaviorGenerator()
        logs = generator.generate_logs(count)
        
        # 原始顺序计算
        result1 = aggregate_by_student(logs)
        
        # 反转顺序计算
        reversed_logs = list(reversed(logs))
        result2 = aggregate_by_student(reversed_logs)
        
        # 结果应该一致
        for student_id in result1:
            assert result1[student_id]['total_duration'] == result2[student_id]['total_duration'], \
                f"学生 {student_id} 时长计算与顺序相关"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
