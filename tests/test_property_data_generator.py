# ============================================
# Property 2: 日志格式完整性测试
# 验证生成的日志包含所有必需字段
# 验证行为类型在有效枚举范围内
# ============================================

import pytest
from hypothesis import given, strategies as st, settings
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from data_generator.generator import LearningBehaviorGenerator


class TestDataGeneratorProperties:
    """数据生成器属性测试"""
    
    # 必需字段列表
    REQUIRED_FIELDS = [
        'student_id',
        'student_name', 
        'course_id',
        'course_name',
        'behavior_type',
        'duration',
        'timestamp',
        'ip_address',
        'device_type',
        'chapter_id',
        'score'
    ]
    
    # 有效行为类型
    VALID_BEHAVIOR_TYPES = [
        'video_watch',
        'homework_submit',
        'quiz_complete',
        'forum_post',
        'material_download'
    ]
    
    # 有效设备类型
    VALID_DEVICE_TYPES = ['PC', 'Mobile', 'Tablet']
    
    @pytest.fixture
    def generator(self):
        """创建数据生成器实例"""
        return LearningBehaviorGenerator()
    
    @given(st.integers(min_value=1, max_value=100))
    @settings(max_examples=50)
    def test_log_contains_all_required_fields(self, count):
        """
        Property 2.1: 生成的每条日志必须包含所有必需字段
        """
        generator = LearningBehaviorGenerator()
        logs = generator.generate_logs(count)
        
        for log in logs:
            for field in self.REQUIRED_FIELDS:
                assert field in log, f"日志缺少必需字段: {field}"
    
    @given(st.integers(min_value=1, max_value=100))
    @settings(max_examples=50)
    def test_behavior_type_in_valid_range(self, count):
        """
        Property 2.2: 行为类型必须在有效枚举范围内
        """
        generator = LearningBehaviorGenerator()
        logs = generator.generate_logs(count)
        
        for log in logs:
            assert log['behavior_type'] in self.VALID_BEHAVIOR_TYPES, \
                f"无效的行为类型: {log['behavior_type']}"
    
    @given(st.integers(min_value=1, max_value=100))
    @settings(max_examples=50)
    def test_device_type_in_valid_range(self, count):
        """
        Property 2.3: 设备类型必须在有效枚举范围内
        """
        generator = LearningBehaviorGenerator()
        logs = generator.generate_logs(count)
        
        for log in logs:
            assert log['device_type'] in self.VALID_DEVICE_TYPES, \
                f"无效的设备类型: {log['device_type']}"
    
    @given(st.integers(min_value=1, max_value=100))
    @settings(max_examples=50)
    def test_duration_is_positive(self, count):
        """
        Property 2.4: 持续时长必须为正整数
        """
        generator = LearningBehaviorGenerator()
        logs = generator.generate_logs(count)
        
        for log in logs:
            assert isinstance(log['duration'], int), "duration必须是整数"
            assert log['duration'] > 0, f"duration必须为正数: {log['duration']}"
    
    @given(st.integers(min_value=1, max_value=100))
    @settings(max_examples=50)
    def test_student_id_format(self, count):
        """
        Property 2.5: 学生ID格式必须正确 (STU_开头)
        """
        generator = LearningBehaviorGenerator()
        logs = generator.generate_logs(count)
        
        for log in logs:
            assert log['student_id'].startswith('STU_'), \
                f"学生ID格式错误: {log['student_id']}"
    
    @given(st.integers(min_value=1, max_value=100))
    @settings(max_examples=50)
    def test_course_id_format(self, count):
        """
        Property 2.6: 课程ID格式必须正确 (COURSE_开头)
        """
        generator = LearningBehaviorGenerator()
        logs = generator.generate_logs(count)
        
        for log in logs:
            assert log['course_id'].startswith('COURSE_'), \
                f"课程ID格式错误: {log['course_id']}"
    
    @given(st.integers(min_value=1, max_value=100))
    @settings(max_examples=50)
    def test_score_valid_range(self, count):
        """
        Property 2.7: 得分必须在0-100范围内或为None
        """
        generator = LearningBehaviorGenerator()
        logs = generator.generate_logs(count)
        
        for log in logs:
            score = log['score']
            if score is not None:
                assert 0 <= score <= 100, f"得分超出范围: {score}"
    
    @given(st.integers(min_value=1, max_value=100))
    @settings(max_examples=50)
    def test_timestamp_format(self, count):
        """
        Property 2.8: 时间戳格式必须正确 (YYYY-MM-DD HH:MM:SS)
        """
        import re
        generator = LearningBehaviorGenerator()
        logs = generator.generate_logs(count)
        
        pattern = r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
        for log in logs:
            assert re.match(pattern, log['timestamp']), \
                f"时间戳格式错误: {log['timestamp']}"
    
    @given(st.integers(min_value=1, max_value=100))
    @settings(max_examples=50)
    def test_ip_address_format(self, count):
        """
        Property 2.9: IP地址格式必须正确
        """
        import re
        generator = LearningBehaviorGenerator()
        logs = generator.generate_logs(count)
        
        pattern = r'^192\.168\.\d{1,3}\.\d{1,3}$'
        for log in logs:
            assert re.match(pattern, log['ip_address']), \
                f"IP地址格式错误: {log['ip_address']}"
    
    @given(st.integers(min_value=1, max_value=200))
    @settings(max_examples=50)
    def test_generated_count_matches_requested(self, count):
        """
        Property 2.10: 生成的日志数量必须等于请求的数量
        """
        generator = LearningBehaviorGenerator()
        logs = generator.generate_logs(count)
        
        assert len(logs) == count, \
            f"生成数量不匹配: 请求 {count}, 实际 {len(logs)}"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
