#!/bin/bash

# 创建logs目录（如果不存在）
mkdir -p logs

# 清空之前的日志文件
> ./logs/abort_ratio_2PL.log

# 定义要运行的测试次数
NUM_TESTS=30

# 定义最大并行数
MAX_PARALLEL=5

# 计数器
count=0

# 运行测试的函数
run_test() {
    local test_num=$1
    echo "开始执行测试 #${test_num}..."
    ./build/test/spectrum_test --gtest_filter=*TwoPhaseLockingCrossChainProTest.CanExecute
    echo "测试 #${test_num} 完成"
}

# 并行执行测试
for ((i=1; i<=NUM_TESTS; i++)); do
    # 检查当前运行的进程数
    while [ $(jobs -p | wc -l) -ge $MAX_PARALLEL ]; do
        sleep 1
    done
    
    # 在后台运行测试
    run_test $i &
    
    echo "启动测试 #${i}"
    
    # 每启动MAX_PARALLEL个测试后等待一组完成
    if [ $((i % MAX_PARALLEL)) -eq 0 ]; then
        wait
        echo "完成一组测试"
    fi
done

# 等待所有剩余的测试完成
wait

echo "所有测试完成！"

# 分析结果
echo "测试结果统计："
echo "总测试次数：$NUM_TESTS"
echo "查看详细结果请检查 ./logs/abort_ratio_2PL.log 文件" 