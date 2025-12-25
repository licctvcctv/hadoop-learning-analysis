#!/bin/bash
# 自动数据管道：生成数据 + 分析 循环执行

echo "=========================================="
echo "自动数据管道启动"
echo "=========================================="

while true; do
    echo ""
    echo "[$(date '+%H:%M:%S')] 生成新数据..."
    
    # 生成 100 条新数据
    /spark/bin/spark-submit --master spark://spark-master:7077 \
        --conf spark.executor.memory=512m \
        /opt/spark-apps/realtime_generator.py 100 1 3 2>/dev/null | grep -E "(批次|累计)"
    
    echo "[$(date '+%H:%M:%S')] 运行分析..."
    
    # 运行分析
    /spark/bin/spark-submit --master spark://spark-master:7077 \
        --conf spark.executor.memory=512m \
        /opt/spark-apps/analyze_data.py 2>/dev/null | grep -E "(读取到|行为数|完成)"
    
    echo "[$(date '+%H:%M:%S')] 等待 20 秒..."
    sleep 20
done
