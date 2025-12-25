# ============================================
# 数据生成器模块
# 基于Hadoop的大学生线上课程学习行为数据存储与分析系统
# ============================================

from .generator import LearningBehaviorGenerator
from .logger import setup_logger

__all__ = ['LearningBehaviorGenerator', 'setup_logger']
__version__ = '1.0.0'
