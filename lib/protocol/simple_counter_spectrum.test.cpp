#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <spectrum/protocol/spectrum.hpp>
#include <spectrum/transaction/evm-transaction.hpp>
#include <spectrum/common/glog-prefix.hpp>
#include <spectrum/common/hex.hpp>
#include "../workload/simple_counter.hpp"
#include <evmc/evmc.h>
#include <iomanip>

// 添加命令行参数控制日志级别
DEFINE_int32(vlog_level, 0, "日志详细级别 (0-3)");
DEFINE_bool(show_protocol_detail, false, "显示协议执行细节");

namespace {

using namespace spectrum;
using namespace std::chrono_literals;

// 前向声明辅助函数
uint64_t QueryCounterValue(SimpleCounter& workload);

// 测试使用SimpleCounter运行Spectrum协议
TEST(Spectrum, SimpleCounterTest) {
    google::InstallPrefixFormatter(PrefixFormatter);
    
    // 设置glog详细级别，使DLOG可见
    if (FLAGS_show_protocol_detail) {
        google::SetVLOGLevel("*", FLAGS_vlog_level);
        FLAGS_v = FLAGS_vlog_level;
    }
    
    // 创建统计对象收集性能数据
    auto statistics = Statistics();
    
    // 创建SimpleCounter工作负载
    auto workload = SimpleCounter();
    
    // 创建Spectrum协议实例
    // 参数: 工作负载, 统计对象, 线程数, 队列大小, EVM类型
    auto protocol = Spectrum(workload, statistics, 1, 32, EVMType::COPYONWRITE);
    
    // 启动协议
    protocol.Start();
    
    // 运行一段时间
    std::this_thread::sleep_for(2ms);  // 运行1秒
    
    // 停止协议
    protocol.Stop();
    
    // 打印统计信息
    std::string stats = statistics.Print();
    LOG(INFO) << "\n\n==================================";
    LOG(INFO) << "SimpleCounter测试统计信息:";
    LOG(INFO) << stats;
    LOG(INFO) << "==================================\n";
}



} 