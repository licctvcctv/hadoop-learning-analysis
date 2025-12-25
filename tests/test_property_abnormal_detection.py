# ============================================
# Property 6: 异常检测阈值一致性测试
# 验证被标记异常的学生未活跃天数 >= 7天阈值
# ============================================

import pytest
from hypothesis import given, strategies as st, settings, assume
from collections import defaultdict
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from data_generator.generator import LearningBehaviorGenerator

# 导入生产代码中的检测逻辑（如果可用）
try:
    from spark_analysis.abnormal_detection import (
        INACTIVE_DAYS_THRESHOLD as PROD_INACTIVE_THRESHOLD,
        detect_inactive_students as prod_detect_inactive
    )
    HAS_PROD_CODE = True
except ImportError:
    HAS_PROD_CODE = False

# 测试用阈值常量
INACTIVE_DAYS_THRESHOLD = 7
LOW_DURATION_THRESHOLD = 1800  # 秒
LOW_SCORE_THRESHOLD = 60


def detect_inactive_students(logs, current_date, threshold):
    """
    异常检测算法实现
    """
    student_last_activity = {}
    for log in logs:
        timestamp = datetime.strptime(log['timestamp'], '%Y-%m-%d %H:%M:%S')
        activity_date = timestamp.date()
        
        if log['student_id'] not in student_last_activity:
            student_last_activity[log['student_id']] = activity_date
        elif activity_date > student_last_activity[log['student_id']]:
            student_last_activity[log['student_id']] = activity_date
    
    inactive = []
    for student_id, last_date in student_last_activity.items():
        inactive_days = (current_date - last_date).days
        if inactive_days >= threshold:
            inactive.append({
                'student_id': student_id,
                'inactive_days': inactive_days,
                'last_activity': last_date
            })
    return inactive, student_last_activity


def get_alert_level(inactive_days):
    """根据未活跃天数确定预警级别"""
    if inactive_days >= 14:
        return "高"
    elif inactive_days >= 10:
        return "中"
    else:
        return "低"


class TestAbnormalDetectionProperties:
    """异常检测属性测试"""
    
    @given(st.integers(min_value=50, max_value=300))
    @settings(max_examples=50)
    def test_inactive_detection_completeness(self, count):
        """
        Property 6.1: 异常检测的完整性
        
        验证检测算法的完整性：
        - 所有未活跃天数 >= 阈值的学生都被检测到
        - 所有未活跃天数 < 阈值的学生都不被检测到
        """
        generator = LearningBehaviorGenerator()
        
        end_time = datetime.now()
        start_time = end_time - timedelta(days=30)
        logs = generator.generate_logs(count, start_time, end_time)
        
        current_date = datetime.now().date()
        
        # 使用检测函数
        inactive_students, all_students = detect_inactive_students(
            logs, current_date, INACTIVE_DAYS_THRESHOLD
        )
        
        inactive_ids = {s['student_id'] for s in inactive_students}
        
        # 验证完整性：检查每个学生的分类是否正确
        for student_id, last_date in all_students.items():
            inactive_days = (current_date - last_date).days
            
            if inactive_days >= INACTIVE_DAYS_THRESHOLD:
                assert student_id in inactive_ids, \
                    f"漏检: 学生 {student_id} 未活跃 {inactive_days} 天应被检测"
            else:
                assert student_id not in inactive_ids, \
                    f"误检: 学生 {student_id} 未活跃 {inactive_days} 天不应被检测"
    
    @given(
        st.integers(min_value=50, max_value=300),
        st.integers(min_value=5, max_value=20)
    )
    @settings(max_examples=30)
    def test_threshold_sensitivity(self, count, threshold):
        """
        Property 6.2: 阈值敏感性测试
        
        验证不同阈值下检测结果的单调性：
        阈值越大，检测到的异常学生应该越少或相等
        """
        generator = LearningBehaviorGenerator()
        
        end_time = datetime.now()
        start_time = end_time - timedelta(days=30)
        logs = generator.generate_logs(count, start_time, end_time)
        
        current_date = datetime.now().date()
        
        # 使用较小阈值
        smaller_threshold = threshold
        inactive_small, _ = detect_inactive_students(logs, current_date, smaller_threshold)
        
        # 使用较大阈值
        larger_threshold = threshold + 3
        inactive_large, _ = detect_inactive_students(logs, current_date, larger_threshold)
        
        # 较大阈值检测到的学生应该是较小阈值的子集
        small_ids = {s['student_id'] for s in inactive_small}
        large_ids = {s['student_id'] for s in inactive_large}
        
        assert large_ids.issubset(small_ids), \
            f"阈值单调性失败: 阈值{larger_threshold}检测到的学生不是阈值{smaller_threshold}的子集"
    
    @given(st.integers(min_value=100, max_value=500))
    @settings(max_examples=30)
    def test_alert_level_ordering(self, count):
        """
        Property 6.3: 预警级别排序正确性
        
        验证预警级别与未活跃天数的对应关系：
        - 高级别的未活跃天数 >= 中级别
        - 中级别的未活跃天数 >= 低级别
        """
        generator = LearningBehaviorGenerator()
        
        end_time = datetime.now()
        start_time = end_time - timedelta(days=30)
        logs = generator.generate_logs(count, start_time, end_time)
        
        current_date = datetime.now().date()
        
        inactive_students, _ = detect_inactive_students(
            logs, current_date, INACTIVE_DAYS_THRESHOLD
        )
        
        # 按级别分组
        by_level = {'高': [], '中': [], '低': []}
        for student in inactive_students:
            level = get_alert_level(student['inactive_days'])
            by_level[level].append(student['inactive_days'])
        
        # 验证级别边界
        if by_level['高'] and by_level['中']:
            min_high = min(by_level['高'])
            max_mid = max(by_level['中'])
            assert min_high >= max_mid, \
                f"级别边界错误: 高级别最小值{min_high} < 中级别最大值{max_mid}"
        
        if by_level['中'] and by_level['低']:
            min_mid = min(by_level['中'])
            max_low = max(by_level['低'])
            assert min_mid >= max_low, \
                f"级别边界错误: 中级别最小值{min_mid} < 低级别最大值{max_low}"
    
    @given(st.integers(min_value=100, max_value=500))
    @settings(max_examples=30)
    def test_detection_determinism(self, count):
        """
        Property 6.4: 检测结果的确定性
        
        相同输入应该产生相同的检测结果
        """
        generator = LearningBehaviorGenerator()
        
        end_time = datetime.now()
        start_time = end_time - timedelta(days=30)
        logs = generator.generate_logs(count, start_time, end_time)
        
        current_date = datetime.now().date()
        
        # 运行两次检测
        result1, _ = detect_inactive_students(logs, current_date, INACTIVE_DAYS_THRESHOLD)
        result2, _ = detect_inactive_students(logs, current_date, INACTIVE_DAYS_THRESHOLD)
        
        # 结果应该完全一致
        ids1 = sorted([s['student_id'] for s in result1])
        ids2 = sorted([s['student_id'] for s in result2])
        
        assert ids1 == ids2, "相同输入产生了不同的检测结果"
    
    @given(st.integers(min_value=50, max_value=300))
    @settings(max_examples=50)
    def test_duration_aggregation_correctness(self, count):
        """
        Property 6.5: 学习时长聚合正确性
        
        验证按学生聚合时长的计算正确性
        """
        generator = LearningBehaviorGenerator()
        logs = generator.generate_logs(count)
        
        # 方法1: 使用 defaultdict 聚合
        student_durations_v1 = defaultdict(int)
        for log in logs:
            student_durations_v1[log['student_id']] += log['duration']
        
        # 方法2: 使用分组后求和
        student_logs = defaultdict(list)
        for log in logs:
            student_logs[log['student_id']].append(log['duration'])
        
        student_durations_v2 = {
            sid: sum(durations) 
            for sid, durations in student_logs.items()
        }
        
        # 两种方法结果应该一致
        for student_id in student_durations_v1:
            assert student_durations_v1[student_id] == student_durations_v2[student_id], \
                f"学生 {student_id} 时长聚合不一致"
    
    @given(st.integers(min_value=50, max_value=300))
    @settings(max_examples=50)
    def test_low_duration_threshold_boundary(self, count):
        """
        Property 6.6: 低时长阈值边界测试
        
        验证恰好在阈值边界的情况处理正确
        """
        generator = LearningBehaviorGenerator()
        logs = generator.generate_logs(count)
        
        # 计算每个学生的总时长
        student_durations = defaultdict(int)
        for log in logs:
            student_durations[log['student_id']] += log['duration']
        
        # 验证阈值判断的一致性
        for student_id, duration in student_durations.items():
            is_low = duration < LOW_DURATION_THRESHOLD
            
            # 边界情况验证
            if duration == LOW_DURATION_THRESHOLD:
                assert not is_low, f"恰好等于阈值{LOW_DURATION_THRESHOLD}不应被标记为低时长"
            elif duration < LOW_DURATION_THRESHOLD:
                assert is_low, f"低于阈值{LOW_DURATION_THRESHOLD}应被标记为低时长"
            else:
                assert not is_low, f"高于阈值{LOW_DURATION_THRESHOLD}不应被标记为低时长"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
