# ============================================
# 日志配置模块
# 统一日志格式：[时间戳] [级别] [模块] [消息]
# ============================================

import logging
import os
from datetime import datetime

def setup_logger(name: str, log_file: str = None, level: str = None) -> logging.Logger:
    """
    设置统一格式的日志记录器
    
    Args:
        name: 日志记录器名称（模块名）
        log_file: 日志文件路径，如果为None则只输出到控制台
        level: 日志级别，默认从环境变量LOG_LEVEL获取
        
    Returns:
        配置好的日志记录器
    """
    # 获取日志级别
    if level is None:
        level = os.environ.get('LOG_LEVEL', 'INFO')
    
    level_map = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARN': logging.WARNING,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }
    log_level = level_map.get(level.upper(), logging.INFO)
    
    # 创建日志记录器
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    
    # 避免重复添加handler
    if logger.handlers:
        return logger
    
    # 统一日志格式
    formatter = logging.Formatter(
        '[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # 文件处理器
    if log_file:
        # 确保日志目录存在
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


class LogContext:
    """日志上下文管理器，用于记录操作耗时"""
    
    def __init__(self, logger: logging.Logger, operation: str):
        self.logger = logger
        self.operation = operation
        self.start_time = None
    
    def __enter__(self):
        self.start_time = datetime.now()
        self.logger.info(f"开始 {self.operation}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = (datetime.now() - self.start_time).total_seconds()
        if exc_type is None:
            self.logger.info(f"完成 {self.operation}，耗时 {elapsed:.2f}s")
        else:
            self.logger.error(f"失败 {self.operation}，耗时 {elapsed:.2f}s，错误: {exc_val}")
        return False
