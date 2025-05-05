#include <spectrum/protocol/two_phase_locking_crosschain.hpp>
#include <spectrum/workload/smallbank.hpp>
#include <spectrum/common/statistics.hpp>
#include <gtest/gtest.h>
#include <string>
#include <memory>

namespace spectrum {

constexpr size_t NUM_ACCOUNTS = 100;
constexpr size_t NUM_THREADS = 4;
constexpr double ZIPF_EXPONENT = 0.8;
constexpr size_t RUN_SECONDS = 35;

TEST(TwoPhaseLockingCrossChainTest, CanExecute) {
    Smallbank workload(NUM_ACCOUNTS, ZIPF_EXPONENT);
    Statistics statistics;
    TwoPhaseLockingCrossChain protocol(workload, statistics, NUM_THREADS, EVMType::COPYONWRITE, 0.1);
    
    // 运行协议一段时间
    protocol.Start();
    std::this_thread::sleep_for(std::chrono::seconds(RUN_SECONDS));
    protocol.Stop();
    // 打印统计信息
    LOG(INFO) << "\n\n===== 交易执行完成 =====";
    std::string stats = statistics.Print();
    LOG(INFO) << "\n<<< 统计信息输出 >>>";
    LOG(INFO) << stats;
    LOG(INFO) << "<<< 统计信息结束 >>>";
}
    // 验证执行了一些事务
    // ASSERT_GT(statistics.GetTxTotal(), 0);
    // ASSERT_GT(statistics.GetTxCommit(), 0);
    
    // // 输出统计信息
    // LOG(INFO) << "执行统计：";
    // LOG(INFO) << "  总事务数: " << statistics.GetTxTotal();
    // LOG(INFO) << "  提交事务数: " << statistics.GetTxCommit();
    // LOG(INFO) << "  中止事务数: " << statistics.GetTxAbort();
    // LOG(INFO) << "  平均延迟(微秒): " << statistics.GetAverageLatency();
    // LOG(INFO) << "  吞吐量(tps): " << (statistics.GetTxCommit() / static_cast<double>(RUN_SECONDS));
}

// TEST(TwoPhaseLockingCrossChainTest, CompareWithRegularTPL) {
//     // 设置工作负载
//     Smallbank workload1(NUM_ACCOUNTS, ZIPF_EXPONENT);
//     Smallbank workload2(NUM_ACCOUNTS, ZIPF_EXPONENT);
    
//     // 设置统计信息收集器
//     Statistics stats_regular;
//     Statistics stats_cross_chain;
    
//     // 创建协议实例
//     TwoPhaseLocking regular_protocol(workload1, stats_regular, NUM_THREADS, EVMType::STRAWMAN);
//     TwoPhaseLockingCrossChain cross_chain_protocol(workload2, stats_cross_chain, NUM_THREADS, EVMType::STRAWMAN, 0.3);
    
//     // 运行常规两阶段锁协议
//     LOG(INFO) << "运行常规两阶段锁协议...";
//     regular_protocol.Start();
//     std::this_thread::sleep_for(std::chrono::seconds(RUN_SECONDS));
//     regular_protocol.Stop();
    
//     // 运行跨链两阶段锁协议
//     LOG(INFO) << "运行跨链两阶段锁协议（30%跨链事务）...";
//     cross_chain_protocol.Start();
//     std::this_thread::sleep_for(std::chrono::seconds(RUN_SECONDS));
//     cross_chain_protocol.Stop();
    
//     // 输出常规协议统计信息
//     LOG(INFO) << "常规两阶段锁统计：";
//     LOG(INFO) << "  总事务数: " << stats_regular.GetTxTotal();
//     LOG(INFO) << "  提交事务数: " << stats_regular.GetTxCommit();
//     LOG(INFO) << "  中止事务数: " << stats_regular.GetTxAbort();
//     LOG(INFO) << "  平均延迟(微秒): " << stats_regular.GetAverageLatency();
//     LOG(INFO) << "  吞吐量(tps): " << (stats_regular.GetTxCommit() / static_cast<double>(RUN_SECONDS));
    
//     // 输出跨链协议统计信息
//     LOG(INFO) << "跨链两阶段锁统计：";
//     LOG(INFO) << "  总事务数: " << stats_cross_chain.GetTxTotal();
//     LOG(INFO) << "  提交事务数: " << stats_cross_chain.GetTxCommit();
//     LOG(INFO) << "  中止事务数: " << stats_cross_chain.GetTxAbort();
//     LOG(INFO) << "  平均延迟(微秒): " << stats_cross_chain.GetAverageLatency();
//     LOG(INFO) << "  吞吐量(tps): " << (stats_cross_chain.GetTxCommit() / static_cast<double>(RUN_SECONDS));
    
//     // 比较两种协议的性能指标
//     LOG(INFO) << "性能比较：";
//     double throughput_regular = stats_regular.GetTxCommit() / static_cast<double>(RUN_SECONDS);
//     double throughput_cross_chain = stats_cross_chain.GetTxCommit() / static_cast<double>(RUN_SECONDS);
//     LOG(INFO) << "  吞吐量比例(跨链/常规): " << (throughput_cross_chain / throughput_regular);
//     LOG(INFO) << "  延迟比例(跨链/常规): " << (stats_cross_chain.GetAverageLatency() / stats_regular.GetAverageLatency());
//     LOG(INFO) << "  中止率比例(跨链/常规): " << 
//         (stats_cross_chain.GetTxAbort() / static_cast<double>(stats_cross_chain.GetTxTotal())) /
//         (stats_regular.GetTxAbort() / static_cast<double>(stats_regular.GetTxTotal()));
    
//     // 测试断言 - 这里我们不对性能进行严格断言，因为性能可能受到多种因素影响
//     ASSERT_GT(stats_regular.GetTxTotal(), 0);
//     ASSERT_GT(stats_cross_chain.GetTxTotal(), 0);
// }

 // namespace spectrum 