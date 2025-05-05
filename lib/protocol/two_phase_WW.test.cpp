#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <spectrum/protocol/two_phase_WW.hpp>
#include <spectrum/workload/simple_xy.hpp>
#include <spectrum/workload/smallbank.hpp>
#include <chrono>
#include <thread>

namespace {

using namespace spectrum;
using namespace std::chrono_literals;

// 测试Wound-Wait两阶段锁的基本功能
TEST(TwoPhaseLockingWW, BasicFunctionality) {
    // 创建统计对象
    auto statistics = Statistics();
    
    // 创建Smallbank工作负载
    auto workload = Smallbank(500, 1.2);  // 500个账户,Zipf系数1.2
    
    // 创建TwoPhaseLockingWW协议实例
    auto protocol = TwoPhaseLockingWW(workload, statistics, 4, EVMType::COPYONWRITE);
    
    // 启动协议
    LOG(INFO) << "启动Wound-Wait两阶段锁协议...";
    protocol.Start();
    
    // 让协议运行一段时间处理交易
    LOG(INFO) << "协议运行60秒...";
    std::this_thread::sleep_for(3s);
    
    // 停止协议
    LOG(INFO) << "停止Wound-Wait两阶段锁协议...";
    protocol.Stop();
    
    // 打印统计信息
    LOG(INFO) << "\n\n===== 交易执行完成 =====";
    std::string stats = statistics.Print();
    LOG(INFO) << "\n<<< 统计信息输出 >>>";
    LOG(INFO) << stats;
    LOG(INFO) << "<<< 统计信息结束 >>>";
}

} // namespace 