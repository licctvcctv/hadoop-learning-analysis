# ============================================
# Property 5: 课程热度排名正确性测试
# 验证学习人数多的课程排名靠前
# ============================================

import pytest
from hypothesis import given, strategies as st, settings, assume
from collections import defaultdict
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from data_generator.generator import LearningBehaviorGenerator


def calculate_course_metrics(logs):
    """计算课程指标"""
    course_students = defaultdict(set)
    course_durations = defaultdict(int)
    course_behaviors = defaultdict(int)
    
    for log in logs:
        course_id = log['course_id']
        course_students[course_id].add(log['student_id'])
        course_durations[course_id] += log['duration']
        course_behaviors[course_id] += 1
    
    return {
        course_id: {
            'student_count': len(students),
            'total_duration': course_durations[course_id],
            'behavior_count': course_behaviors[course_id],
            'avg_duration': course_durations[course_id] / len(students) if students else 0
        }
        for course_id, students in course_students.items()
    }


def rank_courses(metrics, key='student_count'):
    """按指定指标排名"""
    return sorted(
        metrics.items(),
        key=lambda x: (-x[1][key], x[0])  # 降序，相同时按ID排序保证稳定性
    )


class TestCoursePopularityProperties:
    """课程热度排名属性测试"""
    
    @given(st.integers(min_value=50, max_value=500))
    @settings(max_examples=50)
    def test_ranking_order_correctness(self, count):
        """
        Property 5.1: 排名顺序正确性
        
        排名靠前的课程学习人数 >= 排名靠后的课程
        """
        generator = LearningBehaviorGenerator()
        logs = generator.generate_logs(count)
        
        metrics = calculate_course_metrics(logs)
        ranked = rank_courses(metrics, 'student_count')
        
        # 验证排名顺序
        for i in range(len(ranked) - 1):
            course_a, metrics_a = ranked[i]
            course_b, metrics_b = ranked[i + 1]
            
            assert metrics_a['student_count'] >= metrics_b['student_count'], \
                f"排名错误: {course_a}({metrics_a['student_count']}人) " \
                f"排在 {course_b}({metrics_b['student_count']}人) 前面但人数更少"
    
    @given(st.integers(min_value=50, max_value=500))
    @settings(max_examples=50)
    def test_student_count_equals_unique_students(self, count):
        """
        Property 5.2: 学习人数等于去重后的学生数
        
        验证学习人数统计的正确性
        """
        generator = LearningBehaviorGenerator()
        logs = generator.generate_logs(count)
        
        metrics = calculate_course_metrics(logs)
        
        for course_id, course_metrics in metrics.items():
            # 使用不同方法计算
            unique_students = len(set(
                log['student_id'] for log in logs 
                if log['course_id'] == course_id
            ))
            
            assert course_metrics['student_count'] == unique_students, \
                f"课程 {course_id}: 统计人数={course_metrics['student_count']}, " \
                f"实际去重人数={unique_students}"
    
    @given(st.integers(min_value=50, max_value=500))
    @settings(max_examples=50)
    def test_total_duration_consistency(self, count):
        """
        Property 5.3: 总时长一致性
        
        所有课程的总时长之和等于所有日志的时长之和
        """
        generator = LearningBehaviorGenerator()
        logs = generator.generate_logs(count)
        
        metrics = calculate_course_metrics(logs)
        
        # 按课程汇总的总时长
        course_total = sum(m['total_duration'] for m in metrics.values())
        
        # 所有日志的总时长
        log_total = sum(log['duration'] for log in logs)
        
        assert course_total == log_total, \
            f"时长不一致: 课程汇总={course_total}, 日志总计={log_total}"
    
    @given(st.integers(min_value=50, max_value=500))
    @settings(max_examples=50)
    def test_behavior_count_consistency(self, count):
        """
        Property 5.4: 行为数一致性
        
        所有课程的行为数之和等于日志总数
        """
        generator = LearningBehaviorGenerator()
        logs = generator.generate_logs(count)
        
        metrics = calculate_course_metrics(logs)
        
        # 按课程汇总的行为数
        course_behaviors = sum(m['behavior_count'] for m in metrics.values())
        
        assert course_behaviors == len(logs), \
            f"行为数不一致: 课程汇总={course_behaviors}, 日志数={len(logs)}"
    
    @given(st.integers(min_value=50, max_value=500))
    @settings(max_examples=50)
    def test_average_duration_calculation(self, count):
        """
        Property 5.5: 人均时长计算正确性
        
        人均时长 = 总时长 / 学习人数
        """
        generator = LearningBehaviorGenerator()
        logs = generator.generate_logs(count)
        
        metrics = calculate_course_metrics(logs)
        
        for course_id, course_metrics in metrics.items():
            if course_metrics['student_count'] == 0:
                continue
            
            expected_avg = course_metrics['total_duration'] / course_metrics['student_count']
            actual_avg = course_metrics['avg_duration']
            
            assert abs(expected_avg - actual_avg) < 0.01, \
                f"课程 {course_id}: 期望人均时长={expected_avg}, 实际={actual_avg}"
    
    @given(st.integers(min_value=100, max_value=500))
    @settings(max_examples=30)
    def test_ranking_stability(self, count):
        """
        Property 5.6: 排名稳定性
        
        相同数据多次排名结果一致
        """
        generator = LearningBehaviorGenerator()
        logs = generator.generate_logs(count)
        
        # 多次计算排名
        metrics1 = calculate_course_metrics(logs)
        ranked1 = rank_courses(metrics1)
        
        metrics2 = calculate_course_metrics(logs)
        ranked2 = rank_courses(metrics2)
        
        # 排名应该一致
        assert [c[0] for c in ranked1] == [c[0] for c in ranked2], \
            "相同数据的排名结果不稳定"
    
    @given(st.integers(min_value=50, max_value=500))
    @settings(max_examples=50)
    def test_all_courses_ranked(self, count):
        """
        Property 5.7: 所有课程都被排名
        
        日志中出现的所有课程都应该在排名中
        """
        generator = LearningBehaviorGenerator()
        logs = generator.generate_logs(count)
        
        # 日志中的所有课程
        all_courses = set(log['course_id'] for log in logs)
        
        # 排名中的课程
        metrics = calculate_course_metrics(logs)
        ranked_courses = set(metrics.keys())
        
        assert all_courses == ranked_courses, \
            f"课程不一致: 日志中={all_courses}, 排名中={ranked_courses}"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
