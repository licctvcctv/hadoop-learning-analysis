#!/usr/bin/env python3
# ============================================
# 数据生成器命令行工具
# ============================================

import argparse
import os
import sys
from datetime import datetime, timedelta

# 添加项目路径以支持多种运行方式
# 1. 从项目根目录: python -m src.data_generator.cli
# 2. 从 src 目录: python -m data_generator.cli
# 3. 直接运行: python cli.py
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.dirname(current_dir)
project_dir = os.path.dirname(src_dir)

# 尝试不同的导入方式
try:
    from .generator import LearningBehaviorGenerator
    from .logger import setup_logger
except ImportError:
    try:
        from data_generator.generator import LearningBehaviorGenerator
        from data_generator.logger import setup_logger
    except ImportError:
        sys.path.insert(0, src_dir)
        from data_generator.generator import LearningBehaviorGenerator
        from data_generator.logger import setup_logger


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='大学生线上课程学习行为数据生成器',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
示例:
  # 生成1000条日志到默认目录
  python cli.py -n 1000
  
  # 生成5000条日志到指定目录，CSV格式
  python cli.py -n 5000 -o /data/logs -f csv
  
  # 生成最近30天的日志
  python cli.py -n 10000 -d 30
  
  # 调试模式
  python cli.py -n 100 --debug
        '''
    )
    
    parser.add_argument(
        '-n', '--count',
        type=int,
        default=1000,
        help='生成日志条数 (默认: 1000)'
    )
    
    parser.add_argument(
        '-o', '--output',
        type=str,
        default='./data/logs',
        help='输出目录 (默认: ./data/logs)'
    )
    
    parser.add_argument(
        '-f', '--format',
        type=str,
        choices=['json', 'csv'],
        default='json',
        help='输出格式 (默认: json)'
    )
    
    parser.add_argument(
        '-d', '--days',
        type=int,
        default=7,
        help='生成最近N天的数据 (默认: 7)'
    )
    
    parser.add_argument(
        '-p', '--prefix',
        type=str,
        default='learning_behavior',
        help='文件名前缀 (默认: learning_behavior)'
    )
    
    parser.add_argument(
        '-l', '--log-file',
        type=str,
        default=None,
        help='日志文件路径 (默认: 输出到控制台)'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='启用调试模式'
    )
    
    return parser.parse_args()


def main():
    """主函数"""
    args = parse_args()
    
    # 设置日志级别
    if args.debug:
        os.environ['LOG_LEVEL'] = 'DEBUG'
    
    # 设置日志文件
    log_file = args.log_file
    if log_file is None and os.path.exists('./logs/data-generator'):
        log_file = './logs/data-generator/generator.log'
    
    logger = setup_logger('CLI', log_file)
    
    logger.info("=" * 50)
    logger.info("学习行为数据生成器启动")
    logger.info("=" * 50)
    logger.info(f"参数配置:")
    logger.info(f"  - 生成数量: {args.count}")
    logger.info(f"  - 输出目录: {args.output}")
    logger.info(f"  - 输出格式: {args.format}")
    logger.info(f"  - 时间范围: 最近 {args.days} 天")
    logger.info(f"  - 文件前缀: {args.prefix}")
    
    try:
        # 创建生成器
        generator = LearningBehaviorGenerator(log_file)
        
        # 计算时间范围
        end_time = datetime.now()
        start_time = end_time - timedelta(days=args.days)
        
        # 生成日志
        logs = generator.generate_logs(
            count=args.count,
            start_time=start_time,
            end_time=end_time
        )
        
        # 保存到文件
        # 判断output是目录还是文件路径
        if os.path.isdir(args.output) or args.output.endswith('/'):
            # 如果是目录，生成带时间戳的文件名
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{args.prefix}_{timestamp}.{args.format}"
            filepath = os.path.join(args.output, filename)
        else:
            # 如果是文件路径，直接使用
            filepath = args.output
        
        output_path = generator.write_to_file(logs, filepath, args.format)
        
        logger.info("=" * 50)
        logger.info("数据生成完成!")
        logger.info(f"输出文件: {output_path}")
        logger.info("=" * 50)
        
        print(f"\n✅ 成功生成 {len(logs)} 条学习行为日志")
        print(f"📁 输出文件: {output_path}")
        
    except Exception as e:
        logger.error(f"数据生成失败: {str(e)}", exc_info=True)
        print(f"\n❌ 错误: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()
