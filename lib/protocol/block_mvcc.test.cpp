#include <spectrum/protocol/block_mvcc.hpp>
#include <spectrum/workload/smallbank.hpp>
#include <spectrum/common/statistics.hpp>
#include <thread>
#include <chrono>
#include <gtest/gtest.h>
#include <glog/logging.h>

using namespace spectrum;
using namespace std::chrono;

// 测试 BlockMVCC 协议的基本功能
TEST(BlockMVCC, BasicFunctionality) {
    // 创建工作负载（使用 Smallbank，100个元素，Zipf参数0.9）
    Smallbank workload(100, 0.9);
    
    // 创建统计对象
    Statistics statistics;
    
    // 创建 BlockMVCC 协议实例
    // 参数: workload, statistics, 执行器数量, 表分区数, EVM类型, 区块大小, 区块间隔
    BlockMVCC protocol(
        workload, statistics, 
        2,       // 4个执行器线程
        16,      // 16个表分区
        EVMType::STRAWMAN,
        200,      // 每个区块5个交易（用小值便于测试观察）
        milliseconds(2000) // 1秒区块间隔
    );
    
    // 启动协议
    LOG(INFO) << "启动 BlockMVCC 协议...";
    protocol.Start();
    
    // 运行一段时间
    LOG(INFO) << "BlockMVCC 协议运行中...";
    std::this_thread::sleep_for(seconds(70));
    
    // 停止协议
    LOG(INFO) << "停止 BlockMVCC 协议...";
    protocol.Stop();
    
    // // 验证统计结果
    // LOG(INFO) << "总交易数: " << statistics.tx_count;
    // LOG(INFO) << "执行次数: " << statistics.execute_count;
    // LOG(INFO) << "提交次数: " << statistics.commit_count;
    // LOG(INFO) << "中止次数: " << statistics.abort_count;
       // 打印统计信息
    LOG(INFO) << "\n\n===== 交易执行完成 =====";
    std::string stats = statistics.Print();
    LOG(INFO) << "\n<<< 统计信息输出 >>>";
    LOG(INFO) << stats;
    LOG(INFO) << "<<< 统计信息结束 >>>";
    // // 简单验证：确保有交易被执行
    // EXPECT_GT(statistics.tx_count, 0);
    
    // // 验证区块控制功能：
    // // 由于区块大小为5，间隔1秒，10秒内应该不超过 10 * 5 = 50 个交易
    // // 但由于多线程和各种开销，这里只做一个宽松的检查
    // EXPECT_LT(statistics.tx_count, 100);
}

// // 测试不同区块大小和间隔对吞吐量的影响
// TEST(BlockMVCC, BlockSizeAndInterval) {
//     Smallbank workload(100, 0.5);
    
//     // 测试参数组合
//     struct TestConfig {
//         size_t block_size;
//         milliseconds block_interval;
//         std::string description;
//     };
    
//     std::vector<TestConfig> configs = {
//         {10, milliseconds(1000), "小区块(10), 长间隔(1000ms)"},
//         {50, milliseconds(1000), "中区块(50), 长间隔(1000ms)"},
//         {10, milliseconds(200), "小区块(10), 短间隔(200ms)"},
//         {50, milliseconds(200), "中区块(50), 短间隔(200ms)"}
//     };
    
//     for (const auto& config : configs) {
//         Statistics statistics;
        
//         BlockMVCC protocol(
//             workload, statistics, 
//             4, 16, EVMType::STRAWMAN,
//             config.block_size, config.block_interval
//         );
        
//         LOG(INFO) << "测试配置: " << config.description;
        
//         protocol.Start();
//         std::this_thread::sleep_for(seconds(5));
//         protocol.Stop();
        
//         LOG(INFO) << "配置 [" << config.description << "] 结果:";
//         LOG(INFO) << " - 总交易数: " << statistics.tx_count;
//         LOG(INFO) << " - 提交数: " << statistics.commit_count;
//         LOG(INFO) << " - 中止数: " << statistics.abort_count;
//         LOG(INFO) << " - 提交率: " << (statistics.commit_count * 100.0 / std::max(size_t(1), statistics.tx_count)) << "%";
//         LOG(INFO) << "-----------------------------";
//     }
    
//     // 这是一个观察性测试，无需断言
//     SUCCEED();
// } 