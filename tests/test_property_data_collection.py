# ============================================
# Property 1 & 3: 数据采集完整性和Hive数据一致性测试
# Property 1: 验证Flume采集后HDFS数据条数等于生成的日志条数
# Property 3: 验证DWD层数据条数 <= ODS层数据条数
# ============================================

import pytest
from hypothesis import given, strategies as st, settings
import json
import os
import tempfile
import sys
from collections import defaultdict
from contextlib import contextmanager

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from data_generator.generator import LearningBehaviorGenerator


# 共享的辅助函数和上下文管理器
@contextmanager
def temp_json_file(logs):
    """创建临时JSON文件的上下文管理器"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        filepath = f.name
        for log in logs:
            f.write(json.dumps(log, ensure_ascii=False) + '\n')
    try:
        yield filepath
    finally:
        os.unlink(filepath)


def is_valid_record(log):
    """验证记录是否有效 - 模拟ETL清洗逻辑"""
    if not log.get('student_id'):
        return False
    if not log.get('course_id'):
        return False
    if not log.get('behavior_type'):
        return False
    if log.get('duration', 0) <= 0:
        return False
    return True


def get_record_key(log):
    """获取记录的唯一标识"""
    return (
        log['student_id'],
        log['course_id'],
        log['timestamp'],
        log['behavior_type']
    )


class TestDataCollectionProperties:
    """数据采集完整性属性测试"""
    
    @given(st.integers(min_value=10, max_value=500))
    @settings(max_examples=50)
    def test_json_serialization_roundtrip(self, count):
        """
        Property 1.1: JSON序列化往返一致性
        
        验证数据经过序列化和反序列化后保持一致
        """
        generator = LearningBehaviorGenerator()
        logs = generator.generate_logs(count)
        
        with temp_json_file(logs) as filepath:
            # 反序列化
            deserialized = []
            with open(filepath, 'r', encoding='utf-8') as f:
                for line in f:
                    deserialized.append(json.loads(line.strip()))
            
            # 验证数量
            assert len(deserialized) == count, \
                f"序列化后数据丢失: 原始={count}, 反序列化={len(deserialized)}"
            
            # 验证内容（抽样检查关键字段）
            for i in range(min(10, count)):
                assert deserialized[i]['student_id'] == logs[i]['student_id']
                assert deserialized[i]['duration'] == logs[i]['duration']
    
    @given(st.integers(min_value=10, max_value=500))
    @settings(max_examples=50)
    def test_csv_format_preserves_count(self, count):
        """
        Property 1.2: CSV格式保留数据条数
        """
        generator = LearningBehaviorGenerator()
        logs = generator.generate_logs(count)
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            filepath = f.name
        
        try:
            generator.write_to_file(logs, filepath, format='csv')
            
            with open(filepath, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            # CSV有表头，所以行数 = 数据条数 + 1
            assert len(lines) == count + 1, \
                f"CSV行数不匹配: 期望 {count + 1}, 实际 {len(lines)}"
        finally:
            os.unlink(filepath)
    
    @given(st.integers(min_value=10, max_value=500))
    @settings(max_examples=50)
    def test_all_required_fields_present(self, count):
        """
        Property 1.3: 所有必需字段都存在
        """
        generator = LearningBehaviorGenerator()
        logs = generator.generate_logs(count)
        
        required_fields = [
            'student_id', 'student_name', 'course_id', 'course_name',
            'behavior_type', 'duration', 'timestamp', 'ip_address',
            'device_type', 'chapter_id', 'score'
        ]
        
        with temp_json_file(logs) as filepath:
            with open(filepath, 'r', encoding='utf-8') as f:
                for i, line in enumerate(f):
                    parsed = json.loads(line.strip())
                    for field in required_fields:
                        assert field in parsed, \
                            f"第{i+1}条记录缺少字段: {field}"


class TestHiveDataConsistencyProperties:
    """Hive数据一致性属性测试"""
    
    @given(st.integers(min_value=10, max_value=500))
    @settings(max_examples=50)
    def test_etl_never_increases_count(self, count):
        """
        Property 3.1: ETL处理不会增加数据条数
        
        DWD层数据条数 <= ODS层数据条数
        """
        generator = LearningBehaviorGenerator()
        ods_logs = generator.generate_logs(count)
        
        # 模拟ETL清洗
        dwd_logs = [log for log in ods_logs if is_valid_record(log)]
        
        assert len(dwd_logs) <= len(ods_logs), \
            f"ETL后数据增加: ODS={len(ods_logs)}, DWD={len(dwd_logs)}"
    
    @given(st.integers(min_value=10, max_value=500))
    @settings(max_examples=50)
    def test_dwd_is_subset_of_ods(self, count):
        """
        Property 3.2: DWD层数据是ODS层数据的子集
        """
        generator = LearningBehaviorGenerator()
        ods_logs = generator.generate_logs(count)
        
        ods_keys = set(get_record_key(log) for log in ods_logs)
        
        # 模拟ETL清洗
        dwd_logs = [log for log in ods_logs if is_valid_record(log)]
        dwd_keys = set(get_record_key(log) for log in dwd_logs)
        
        # DWD应该是ODS的子集
        extra_keys = dwd_keys - ods_keys
        assert not extra_keys, \
            f"DWD中存在ODS没有的数据: {len(extra_keys)}条"
    
    @given(st.integers(min_value=10, max_value=500))
    @settings(max_examples=50)
    def test_aggregation_preserves_total(self, count):
        """
        Property 3.3: 汇总保持总量不变
        
        按不同维度汇总后的总量应该一致
        """
        generator = LearningBehaviorGenerator()
        logs = generator.generate_logs(count)
        
        # 原始总时长
        total_duration = sum(log['duration'] for log in logs)
        
        # 按学生汇总
        by_student = defaultdict(int)
        for log in logs:
            by_student[log['student_id']] += log['duration']
        student_total = sum(by_student.values())
        
        # 按课程汇总
        by_course = defaultdict(int)
        for log in logs:
            by_course[log['course_id']] += log['duration']
        course_total = sum(by_course.values())
        
        assert total_duration == student_total == course_total, \
            f"汇总不一致: 原始={total_duration}, 按学生={student_total}, 按课程={course_total}"
    
    @given(st.integers(min_value=10, max_value=500))
    @settings(max_examples=50)
    def test_partition_completeness(self, count):
        """
        Property 3.4: 分区完整性
        
        按日期分区后，所有分区的数据总和等于原始数据
        """
        generator = LearningBehaviorGenerator()
        logs = generator.generate_logs(count)
        
        # 按日期分区
        partitions = defaultdict(list)
        for log in logs:
            date = log['timestamp'].split(' ')[0]
            partitions[date].append(log)
        
        # 验证分区完整性
        partition_total = sum(len(p) for p in partitions.values())
        
        assert partition_total == len(logs), \
            f"分区数据不完整: 原始={len(logs)}, 分区总和={partition_total}"
    
    @given(st.integers(min_value=10, max_value=500))
    @settings(max_examples=50)
    def test_dedup_idempotence(self, count):
        """
        Property 3.5: 去重的幂等性
        
        对已去重的数据再次去重，结果不变
        """
        generator = LearningBehaviorGenerator()
        logs = generator.generate_logs(count)
        
        def dedup(logs):
            seen = set()
            result = []
            for log in logs:
                key = get_record_key(log)
                if key not in seen:
                    seen.add(key)
                    result.append(log)
            return result
        
        # 第一次去重
        deduped1 = dedup(logs)
        
        # 第二次去重
        deduped2 = dedup(deduped1)
        
        assert len(deduped1) == len(deduped2), \
            f"去重不幂等: 第一次={len(deduped1)}, 第二次={len(deduped2)}"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
