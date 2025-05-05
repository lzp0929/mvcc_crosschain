#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include "spectrum_cross_chain.hpp"
#include <spectrum/workload/simple_xy.hpp>
#include <spectrum/common/glog-prefix.hpp>
#include <chrono>
#include <thread>

namespace {

using namespace spectrum;
using namespace std::chrono_literals;

// 测试跨链交易的基本功能
TEST(SpectrumCrossChain, BasicFunctionality) {
    google::InstallPrefixFormatter(PrefixFormatter);
    
    // 创建统计对象
    auto statistics = Statistics();
    // 启动协议
    LOG(INFO) << "启动跨链协议2...";
    // 创建SimpleXY工作负载
    auto workload = SimpleXY();
    // 启动协议
    LOG(INFO) << "启动跨链协议1...";
    // 创建SpectrumCrossChain协议实例
    auto protocol = SpectrumCrossChain(workload, statistics, 2, 32, EVMType::COPYONWRITE);
    
    // 启动协议
    LOG(INFO) << "启动跨链协议...";
    protocol.Start();
    // 让协议运行一段时间处理交易
    LOG(INFO) << "协议运行10秒...";
    std::this_thread::sleep_for(3s);
    
    // 停止协议
    LOG(INFO) << "停止跨链协议...";
    protocol.Stop();
    
       // 打印统计信息
    LOG(INFO) << "\n\n===== 交易执行完成 =====";
    std::string stats = statistics.Print();
    LOG(INFO) << "\n<<< 统计信息输出 >>>";
    LOG(INFO) << stats;
    LOG(INFO) << "<<< 统计信息结束 >>>";
    
    
}



} // namespace 