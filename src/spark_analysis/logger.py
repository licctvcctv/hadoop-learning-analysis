# ============================================
# Spark分析任务日志模块
# 统一日志格式：[时间戳] [级别] [模块] [消息]
# ============================================

import logging
import os
from datetime import datetime

def setup_spark_logger(name: str, log_file: str = None) -> logging.Logger:
    """
    设置Spark任务的日志记录器
    
    Args:
        name: 日志记录器名称
        log_file: 日志文件路径
        
    Returns:
        配置好的日志记录器
    """
    log_level = os.environ.get('LOG_LEVEL', 'INFO')
    
    level_map = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARN': logging.WARNING,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR
    }
    
    logger = logging.getLogger(name)
    logger.setLevel(level_map.get(log_level.upper(), logging.INFO))
    
    if logger.handlers:
        return logger
    
    formatter = logging.Formatter(
        '[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    if log_file:
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


class SparkJobContext:
    """Spark任务上下文管理器"""
    
    def __init__(self, logger: logging.Logger, job_name: str):
        self.logger = logger
        self.job_name = job_name
        self.start_time = None
        self.record_count = 0
    
    def __enter__(self):
        self.start_time = datetime.now()
        self.logger.info(f"========== 开始执行: {self.job_name} ==========")
        self.logger.info(f"开始时间: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        end_time = datetime.now()
        elapsed = (end_time - self.start_time).total_seconds()
        
        if exc_type is None:
            self.logger.info(f"处理数据量: {self.record_count} 条")
            self.logger.info(f"结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            self.logger.info(f"执行耗时: {elapsed:.2f} 秒")
            self.logger.info(f"执行状态: 成功")
            self.logger.info(f"========== 完成: {self.job_name} ==========")
        else:
            self.logger.error(f"执行状态: 失败")
            self.logger.error(f"错误信息: {exc_val}")
            self.logger.info(f"========== 失败: {self.job_name} ==========")
        
        return False
    
    def set_record_count(self, count: int):
        self.record_count = count
