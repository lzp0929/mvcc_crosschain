#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <spectrum/protocol/two_phase_locking.hpp>
#include <spectrum/workload/simple_xy.hpp>
#include <spectrum/workload/smallbank.hpp>
#include <spectrum/common/glog-prefix.hpp>
#include <chrono>
#include <thread>

namespace {

using namespace spectrum;
using namespace std::chrono_literals;

// // 测试两阶段锁的基本功能
// TEST(TwoPhaseLocking, BasicFunctionality) {
//     // google::InstallPrefixFormatter(PrefixFormatter);
    
//     // 创建统计对象
//     auto statistics = Statistics();
    
//     // 创建SimpleXY工作负载
//     auto workload = Smallbank(500,1);;
    
//     // 创建TwoPhaseLocking协议实例
//     auto protocol = TwoPhaseLocking(workload, statistics,4, EVMType::COPYONWRITE);
    
//     // 启动协议
//     DLOG(INFO) << "启动两阶段锁协议...";
//     protocol.Start();
    
//     // 让协议运行一段时间处理交易
//     DLOG(INFO) << "协议运行5秒...";
//     std::this_thread::sleep_for(60s);
    
//     // 停止协议
//     DLOG(INFO) << "停止两阶段锁协议...";
//     protocol.Stop();
    
//     // 打印统计信息
//     DLOG(INFO) << "\n\n===== 交易执行完成 =====";
//     std::string stats = statistics.Print();
//     DLOG(INFO) << "\n<<< 统计信息输出 >>>";
//     DLOG(INFO) << stats;
//     DLOG(INFO) << "<<< 统计信息结束 >>>";
// }

} // namespace 