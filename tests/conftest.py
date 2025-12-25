# ============================================
# pytest 配置和共享 fixtures
# ============================================

import pytest
import sys
import os

# 添加src目录到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


@pytest.fixture
def sample_students():
    """示例学生数据"""
    return [
        {'id': 'STU_20210001', 'name': '张三'},
        {'id': 'STU_20210002', 'name': '李四'},
        {'id': 'STU_20210003', 'name': '王五'},
    ]


@pytest.fixture
def sample_courses():
    """示例课程数据"""
    return [
        {'id': 'COURSE_001', 'name': '大数据技术基础', 'chapters': 12},
        {'id': 'COURSE_002', 'name': 'Python程序设计', 'chapters': 15},
    ]


@pytest.fixture
def valid_behavior_types():
    """有效的行为类型列表"""
    return [
        'video_watch',
        'homework_submit',
        'quiz_complete',
        'forum_post',
        'material_download'
    ]
