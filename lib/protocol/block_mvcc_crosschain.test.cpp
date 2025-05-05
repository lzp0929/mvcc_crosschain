#include <spectrum/protocol/block_mvcc_crosschain.hpp>
#include <spectrum/workload/smallbank.hpp>
#include <spectrum/workload/ycsb.hpp>
#include <spectrum/common/statistics.hpp>
#include <gtest/gtest.h>
#include <string>
#include <memory>
#include <filesystem>
#include <fstream>
#include <ctime>
#include <iomanip>

namespace spectrum {

constexpr size_t NUM_ACCOUNTS = 1000000;
constexpr size_t NUM_THREADS = 4;
constexpr double ZIPF_EXPONENT = 1.2;
constexpr size_t RUN_SECONDS = 38;

TEST(BlockMVCCCrossChainTest, CanExecute) {
    // // 创建日志目录
    // std::filesystem::path log_path = "./logs";
    // if (!std::filesystem::exists(log_path)) {
    //     std::filesystem::create_directory(log_path);
    // }

    // // 获取当前时间作为标记
    // auto now = std::chrono::system_clock::now();
    // auto now_time = std::chrono::system_clock::to_time_t(now);
    // std::stringstream timestamp;
    // timestamp << std::put_time(std::localtime(&now_time), "%Y-%m-%d %H:%M:%S");

    // // 以追加模式打开日志文件
    // std::ofstream block_log("./logs/block_spe_abort_rate.log", std::ios::out | std::ios::app);
    // if (!block_log.is_open()) {
    //     LOG(ERROR) << "无法创建日志文件 ./logs/block_spe_abort_rate.log";
    //     return;
    // }

    // Smallbank workload(NUM_ACCOUNTS, ZIPF_EXPONENT);
    YCSB workload(NUM_ACCOUNTS, ZIPF_EXPONENT);
    Statistics statistics;
    
    // 创建协议实例（每区块200笔交易，区块间隔2秒，10%跨链交易率）
    BlockMVCCCrossChain protocol(
        workload, statistics, NUM_THREADS, 9973, EVMType::COPYONWRITE, 
        200, 5.0, 0.2
    );
    
    // 运行协议一段时间
    LOG(INFO) << "开始测试区块MVCC跨链协议...";
    protocol.Start();
    std::this_thread::sleep_for(std::chrono::seconds(RUN_SECONDS));
    protocol.Stop();
    
    // 打印统计信息
    LOG(INFO) << "\n\n===== 交易执行完成 =====";
    std::string stats = statistics.Print();
    LOG(INFO) << "\n<<< 统计信息输出 >>>";
    LOG(INFO) << stats;
    LOG(INFO) << "<<< 统计信息结束 >>>";
    
    // 打印所有区块信息
    LOG(INFO) << "\n<<< 区块信息输出 >>>";
    std::string block_info = protocol.GetAllBlocksInfo();
    LOG(INFO) << block_info;
    LOG(INFO) << "<<< 区块信息结束 >>>";
    
    // // 将区块信息写入日志文件，添加时间戳
    // block_log << "\n\n========== 测试时间: " << timestamp.str() << " ==========\n";
    // block_log << "测试参数:\n";
    // block_log << "账户数量: " << NUM_ACCOUNTS << "\n";
    // block_log << "线程数量: " << NUM_THREADS << "\n";
    // block_log << "Zipf指数: " << ZIPF_EXPONENT << "\n";
    // block_log << "跨链比例: " << 0.0   << "\n";
    // block_log << "运行时间: " << RUN_SECONDS << "秒\n";
    // block_log << "===== 区块信息输出 =====\n";
    // block_log << block_info;
    // block_log << "===== 区块信息结束 =====\n";
    // block_log << "================================\n";
    
    // // 关闭日志文件
    // block_log.close();
}

// TEST(BlockMVCCCrossChainTest, TestDifferentBlockSizes) {
//     // 测试不同区块大小的影响
//     Smallbank workload1(NUM_ACCOUNTS, ZIPF_EXPONENT);
//     Smallbank workload2(NUM_ACCOUNTS, ZIPF_EXPONENT);
    
//     Statistics stats_small_block;
//     Statistics stats_large_block;
    
//     // 创建协议实例（不同区块大小）
//     BlockMVCCCrossChain small_block_protocol(
//         workload1, stats_small_block, NUM_THREADS, 16, EVMType::STRAWMAN, 
//         100, 2.0, 0.3  // 每区块100笔交易
//     );
    
//     BlockMVCCCrossChain large_block_protocol(
//         workload2, stats_large_block, NUM_THREADS, 16, EVMType::STRAWMAN, 
//         300, 2.0, 0.3  // 每区块300笔交易
//     );
    
//     // 测试小区块配置
//     LOG(INFO) << "测试小区块配置（每区块100笔交易）...";
//     small_block_protocol.Start();
//     std::this_thread::sleep_for(std::chrono::seconds(RUN_SECONDS));
//     small_block_protocol.Stop();
    
//     // 测试大区块配置
//     LOG(INFO) << "测试大区块配置（每区块300笔交易）...";
//     large_block_protocol.Start();
//     std::this_thread::sleep_for(std::chrono::seconds(RUN_SECONDS));
//     large_block_protocol.Stop();
    
//     // 输出统计信息比较
//     LOG(INFO) << "\n\n===== 小区块配置统计 =====";
//     LOG(INFO) << stats_small_block.Print();
//     LOG(INFO) << "\n\n===== 大区块配置统计 =====";
//     LOG(INFO) << stats_large_block.Print();
    
//     // 比较性能指标
//     double throughput_small = stats_small_block.GetTxCommit() / static_cast<double>(RUN_SECONDS);
//     double throughput_large = stats_large_block.GetTxCommit() / static_cast<double>(RUN_SECONDS);
    
//     LOG(INFO) << "\n\n===== 性能比较 =====";
//     LOG(INFO) << "小区块吞吐量: " << throughput_small << " tps";
//     LOG(INFO) << "大区块吞吐量: " << throughput_large << " tps";
//     LOG(INFO) << "吞吐量比例(大/小): " << (throughput_large / throughput_small);
//     LOG(INFO) << "延迟比例(大/小): " 
//               << (stats_large_block.GetAverageLatency() / stats_small_block.GetAverageLatency());
//     LOG(INFO) << "中止率比例(大/小): " 
//               << ((stats_large_block.GetTxAbort() / static_cast<double>(stats_large_block.GetTxTotal())) /
//                  (stats_small_block.GetTxAbort() / static_cast<double>(stats_small_block.GetTxTotal())));
                 
//     // 验证执行了一些事务
//     ASSERT_GT(stats_small_block.GetTxTotal(), 0);
//     ASSERT_GT(stats_large_block.GetTxTotal(), 0);
// }

// TEST(BlockMVCCCrossChainTest, TestDifferentBlockIntervals) {
//     // 测试不同区块间隔的影响
//     Smallbank workload1(NUM_ACCOUNTS, ZIPF_EXPONENT);
//     Smallbank workload2(NUM_ACCOUNTS, ZIPF_EXPONENT);
    
//     Statistics stats_short_interval;
//     Statistics stats_long_interval;
    
//     // 创建协议实例（不同区块间隔）
//     BlockMVCCCrossChain short_interval_protocol(
//         workload1, stats_short_interval, NUM_THREADS, 16, EVMType::STRAWMAN, 
//         200, 1.0, 0.3  // 区块间隔1秒
//     );
    
//     BlockMVCCCrossChain long_interval_protocol(
//         workload2, stats_long_interval, NUM_THREADS, 16, EVMType::STRAWMAN, 
//         200, 4.0, 0.3  // 区块间隔4秒
//     );
    
//     // 测试短区块间隔配置
//     LOG(INFO) << "测试短区块间隔配置（1秒）...";
//     short_interval_protocol.Start();
//     std::this_thread::sleep_for(std::chrono::seconds(RUN_SECONDS));
//     short_interval_protocol.Stop();
    
//     // 测试长区块间隔配置
//     LOG(INFO) << "测试长区块间隔配置（4秒）...";
//     long_interval_protocol.Start();
//     std::this_thread::sleep_for(std::chrono::seconds(RUN_SECONDS));
//     long_interval_protocol.Stop();
    
//     // 输出统计信息比较
//     LOG(INFO) << "\n\n===== 短区块间隔配置统计 =====";
//     LOG(INFO) << stats_short_interval.Print();
//     LOG(INFO) << "\n\n===== 长区块间隔配置统计 =====";
//     LOG(INFO) << stats_long_interval.Print();
    
//     // 比较性能指标
//     double throughput_short = stats_short_interval.GetTxCommit() / static_cast<double>(RUN_SECONDS);
//     double throughput_long = stats_long_interval.GetTxCommit() / static_cast<double>(RUN_SECONDS);
    
//     LOG(INFO) << "\n\n===== 性能比较 =====";
//     LOG(INFO) << "短间隔吞吐量: " << throughput_short << " tps";
//     LOG(INFO) << "长间隔吞吐量: " << throughput_long << " tps";
//     LOG(INFO) << "吞吐量比例(短/长): " << (throughput_short / throughput_long);
//     LOG(INFO) << "延迟比例(短/长): " 
//               << (stats_short_interval.GetAverageLatency() / stats_long_interval.GetAverageLatency());
              
//     // 验证执行了一些事务
//     ASSERT_GT(stats_short_interval.GetTxTotal(), 0);
//     ASSERT_GT(stats_long_interval.GetTxTotal(), 0);
// }

} // namespace spectrum 