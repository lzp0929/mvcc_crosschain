#include <spectrum/protocol/cross_chain_myself.hpp>
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
constexpr size_t RUN_SECONDS = 33;

TEST(CrossChainMyselfTest, CanExecute) {
    // 创建日志目录
    std::filesystem::path log_path = "./logs";
    if (!std::filesystem::exists(log_path)) {
        std::filesystem::create_directory(log_path);
    }

    // 获取当前时间作为标记
    auto now = std::chrono::system_clock::now();
    auto now_time = std::chrono::system_clock::to_time_t(now);
    std::stringstream timestamp;
    timestamp << std::put_time(std::localtime(&now_time), "%Y-%m-%d %H:%M:%S");

    // 以追加模式打开日志文件
    std::ofstream composite_log("./logs/myself_abort_ratio.log", std::ios::out | std::ios::app);
    if (!composite_log.is_open()) {
        LOG(ERROR) << "无法创建日志文件 ./logs/myself_abort_ratio.log";
        return;
    }

    YCSB workload(NUM_ACCOUNTS, ZIPF_EXPONENT);
    // Smallbank workload(NUM_ACCOUNTS, ZIPF_EXPONENT);
    Statistics statistics;
    
    // 创建协议实例（每区块200笔交易，区块间隔2秒，25%跨链交易率）
    CrossChainMyself protocol(
        workload, statistics, NUM_THREADS, 16, EVMType::COPYONWRITE, 
        200, 2.0, 0.2
    );
    
    // 运行协议一段时间
    LOG(INFO) << "开始测试CrossChainMyself协议...";
    protocol.Start();
    std::this_thread::sleep_for(std::chrono::seconds(RUN_SECONDS));
    protocol.Stop();
    
    // 打印统计信息
    LOG(INFO) << "\n\n===== 交易执行完成 =====";
    std::string stats = statistics.Print();
    LOG(INFO) << "\n<<< 统计信息输出 >>>";
    LOG(INFO) << stats;
    LOG(INFO) << "<<< 统计信息结束 >>>";
    
    // 获取复合事务体信息
    LOG(INFO) << "\n<<< 所有区块的复合事务体信息 >>>";
    std::string composite_info = protocol.GetAllBlocksCompositeTxsInfo();
    LOG(INFO) << composite_info;
    LOG(INFO) << "<<< 复合事务体信息结束 >>>";

    // 将测试信息写入日志文件
    composite_log << "\n\n========== 测试时间: " << timestamp.str() << " ==========\n";
    composite_log << "测试参数:\n";
    composite_log << "跨链比例: " << 0.0   << "\n";
    composite_log << "账户数量: " << NUM_ACCOUNTS << "\n";
    composite_log << "线程数量: " << NUM_THREADS << "\n";
    composite_log << "Zipf指数: " << ZIPF_EXPONENT << "\n";
    composite_log << "运行时间: " << RUN_SECONDS << "秒\n\n";
    
    // 从复合事务体信息中提取并记录每个区块的基本统计信息
    std::istringstream iss(composite_info);
    std::string line;
    bool is_block_info = false;
    std::string current_block;
    
    while (std::getline(iss, line)) {
        if (line.find("区块") != std::string::npos && line.find("信息:") != std::string::npos) {
            is_block_info = true;
            current_block = line;
            composite_log << "\n" << line << "\n";
            continue;
        }
        
        if (is_block_info) {
            if (line.find("执行时间:") != std::string::npos ||
                line.find("交易数量:") != std::string::npos ||
                line.find("跨链交易数量:") != std::string::npos ||
                line.find("普通交易数量:") != std::string::npos ||
                line.find("中止交易数量:") != std::string::npos ||
                line.find("复合事务体数量:") != std::string::npos ||
                line.find("复合事务体总内存占用:") != std::string::npos) {
                composite_log << line << "\n";
            }
            
            if (line.find("------------------------") != std::string::npos) {
                is_block_info = false;
            }
        }
    }
    
    composite_log << "================================\n";
    composite_log.close();
}

} // namespace spectrum 