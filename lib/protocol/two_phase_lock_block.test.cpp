#include <gtest/gtest.h>
#include <spectrum/protocol/two_phase_lock_block.hpp>
#include <spectrum/workload/smallbank.hpp>
#include <spectrum/common/statistics.hpp>
#include <thread>
#include <chrono>
#include <string>
#include <memory>

namespace spectrum {

constexpr size_t NUM_ACCOUNTS = 500;
constexpr size_t NUM_THREADS = 4;
constexpr double ZIPF_EXPONENT = 0.8;
constexpr size_t RUN_SECONDS = 90;

TEST(TwoPhaseLockBlockTest, CanExecute) {
    Smallbank workload(NUM_ACCOUNTS, ZIPF_EXPONENT);
    Statistics statistics;
    
    // 创建协议实例（每区块200笔交易，区块间隔2秒，10%跨链交易率）
    TwoPhaseLockBlock protocol(
        workload, 
        statistics, 
        NUM_THREADS,
        EVMType::STRAWMAN,  // 使用STRAWMAN类型
        300,  // 每区块300笔交易
        2.0,  // 区块间隔2秒
        0.1   // 10%跨链交易率
    );
    
    // 运行协议一段时间
    LOG(INFO) << "开始测试TwoPhaseLockBlock协议...";
    protocol.Start();
    std::this_thread::sleep_for(std::chrono::seconds(RUN_SECONDS));
    protocol.Stop();
    
    // 打印统计信息
    LOG(INFO) << "\n\n===== 交易执行完成 =====";
    std::string stats = statistics.Print();
    LOG(INFO) << "\n<<< 统计信息输出 >>>";
    LOG(INFO) << stats;
    LOG(INFO) << "<<< 统计信息结束 >>>";
    
    // 打印区块信息
    LOG(INFO) << "\n<<< 所有区块信息 >>>";
    LOG(INFO) << protocol.GetAllBlocksInfo();
    LOG(INFO) << "<<< 区块信息结束 >>>";
}

}