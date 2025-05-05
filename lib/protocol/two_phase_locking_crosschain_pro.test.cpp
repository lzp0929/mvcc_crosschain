#include <spectrum/protocol/two_phase_locking_crosschain_pro.hpp>
#include <spectrum/workload/smallbank.hpp>
#include <spectrum/workload/ycsb.hpp>
#include <spectrum/common/statistics.hpp>
#include <gtest/gtest.h>
#include <string>
#include <memory>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <chrono>

namespace spectrum {

constexpr size_t NUM_ACCOUNTS = 1000000;
constexpr size_t NUM_THREADS = 4;
constexpr double ZIPF_EXPONENT = 1.2;
constexpr size_t RUN_SECONDS = 55;  // 增加运行时间到70秒

TEST(TwoPhaseLockingCrossChainProTest, CanExecute) {
    // 创建日志目录
    std::filesystem::path log_path = "./logs";
    if (!std::filesystem::exists(log_path)) {
        std::filesystem::create_directory(log_path);
    }

    // 以追加模式打开日志文件
    std::ofstream abort_log("./logs/abort_ratio_2PL.log", std::ios::out | std::ios::app);
    if (!abort_log.is_open()) {
        LOG(ERROR) << "无法创建日志文件 ./logs/abort_ratio_2PL.log";
        return;
    }

    // 获取当前时间作为标记
    auto now = std::chrono::system_clock::now();
    auto now_time = std::chrono::system_clock::to_time_t(now);
    std::stringstream timestamp;
    timestamp << std::put_time(std::localtime(&now_time), "%Y-%m-%d %H:%M:%S");

    LOG(INFO) << "开始测试 TwoPhaseLockingCrossChainPro...";
    
    YCSB workload(NUM_ACCOUNTS, ZIPF_EXPONENT);
    // Smallbank workload(NUM_ACCOUNTS, ZIPF_EXPONENT);
    Statistics statistics;
    TwoPhaseLockingCrossChainPro protocol(
        workload, 
        statistics, 
        NUM_THREADS, 
        EVMType::COPYONWRITE,
        200,        // txs_per_block
        10.0,       // block_interval_seconds
        0.2 
            // cross_chain_ratio
    );
    
    LOG(INFO) << "协议初始化完成，开始执行...";
    
    // 运行协议一段时间
    protocol.Start();
    LOG(INFO) << fmt::format("协议开始运行，将执行 {} 秒...", RUN_SECONDS);
    
    std::this_thread::sleep_for(std::chrono::seconds(RUN_SECONDS));
    LOG(INFO) << "执行时间到，准备停止协议...";
    
    protocol.Stop();
    LOG(INFO) << "协议已停止";
    
    // 打印统计信息
    LOG(INFO) << "\n\n===== 交易执行完成 =====";
    std::string stats = statistics.Print();
    LOG(INFO) << "\n<<< 统计信息输出 >>>";
    LOG(INFO) << stats;
    LOG(INFO) << "<<< 统计信息结束 >>>";

    // 打印区块信息
    LOG(INFO) << "\n\n===== 区块信息 =====";
    std::string block_info = protocol.GetAllBlocksInfo();
    LOG(INFO) << block_info;
    EXPECT_FALSE(block_info.empty()) << "区块信息不应为空";
    LOG(INFO) << "===== 区块信息结束 =====\n";

    // 将测试信息写入日志文件
    abort_log << "\n\n========== 测试时间: " << timestamp.str() << " ==========\n";
    abort_log << "测试参数:\n";
    abort_log << "账户数量: " << NUM_ACCOUNTS << "\n";
    abort_log << "线程数量: " << NUM_THREADS << "\n";
    abort_log << "Zipf指数: " << ZIPF_EXPONENT << "\n";
    abort_log << "运行时间: " << RUN_SECONDS << "秒\n";
    abort_log << "区块大小: " << 200 << "\n";
    abort_log << "区块间隔: " << 10.0 << "秒\n";
    abort_log << "跨链比例: " << 0.0 << "\n\n";
    
    // 写入统计信息
    abort_log << "统计信息:\n" << stats << "\n";
    
   
    
    abort_log.close();
}

} // namespace spectrum 