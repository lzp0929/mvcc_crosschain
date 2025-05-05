#include <gtest/gtest.h>
#include <spectrum/protocol/occ_crosschain.hpp>
#include <spectrum/workload/smallbank.hpp>
#include <spectrum/common/statistics.hpp>
#include <spectrum/common/thread-util.hpp>
#include <thread>
#include <chrono>
#include <glog/logging.h>
#include <fmt/core.h>

using namespace spectrum;
using namespace std::chrono;

// 基本功能测试
TEST(OCCCrossChainTest, BasicTest) {
    // 初始化工作负载和统计
    Smallbank workload(1000, 0.0);
    Statistics statistics;
    
    // 创建OCC跨链协议实例
    OCCCrossChain protocol(
        workload, statistics,
        4,  // 执行器数量
        8,  // 表分区数
        EVMType::STRAWMAN,
        200,  // 每个区块的交易数量
        2.0,  // 区块间隔(秒)
        0.1   // 跨链交易比例
    );
    
    // 启动协议
    protocol.Start();
    
    // 等待几个区块完成
    std::this_thread::sleep_for(seconds(10));
    
    // 停止协议
    protocol.Stop();
      // 打印统计信息
    LOG(INFO) << "\n\n===== 交易执行完成 =====";
    std::string stats = statistics.Print();
    LOG(INFO) << "\n<<< 统计信息输出 >>>";
    LOG(INFO) << stats;
    LOG(INFO) << "<<< 统计信息结束 >>>";
    
}

