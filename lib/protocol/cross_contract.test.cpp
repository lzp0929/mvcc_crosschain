#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <spectrum/protocol/spectrum.hpp>
#include <spectrum/transaction/evm-transaction.hpp>
#include <spectrum/common/glog-prefix.hpp>
#include <spectrum/common/hex.hpp>
#include <spectrum/workload/cross_contract.hpp>
#include <evmc/evmc.h>
#include <iomanip>

// 添加命令行参数控制日志级别
DEFINE_int32(vlog_level, 0, "日志详细级别 (0-3)");
DEFINE_bool(show_protocol_detail, false, "显示协议执行细节");

namespace {

using namespace spectrum;
using namespace std::chrono_literals;

// 测试使用CrossContract运行Spectrum协议
TEST(Spectrum, CrossContractTest) {
    google::InstallPrefixFormatter(PrefixFormatter);
    
    // 设置glog详细级别，使DLOG可见
    if (FLAGS_show_protocol_detail) {
        google::SetVLOGLevel("*", FLAGS_vlog_level);
        FLAGS_v = FLAGS_vlog_level;
    }
    
    // 创建统计对象收集性能数据
    auto statistics = Statistics();
    
    // 创建CrossContract工作负载
    auto workload = CrossContract();
    
    // 创建Spectrum协议实例
    // 参数: 工作负载, 统计对象, 线程数, 队列大小, EVM类型
    auto protocol = Spectrum(workload, statistics, 1, 32, EVMType::COPYONWRITE);
    
    // 启动协议
    protocol.Start();
    
    // 运行一段时间
    std::this_thread::sleep_for(3ms);  // 运行5秒
    
    // 停止协议
    protocol.Stop();
    
    // 打印统计信息
    std::string stats = statistics.Print();
    LOG(INFO) << "\n\n==================================";
    LOG(INFO) << "CrossContract测试统计信息:";
    LOG(INFO) << stats;
    LOG(INFO) << "==================================\n";
    
    // 注意：由于使用EVM驱动方式，我们无法直接访问状态变量的值
    // 实际应用中可以通过查询交易收据或使用视图函数获取状态
    LOG(INFO) << "合约状态: EVM自动计算存储位置，测试框架中无法直接查看变量值";
}

// 比较不同并发控制方式
TEST(Spectrum, CrossContractCompareCC) {
    google::InstallPrefixFormatter(PrefixFormatter);
    
    // 设置glog详细级别，使DLOG可见
    if (FLAGS_show_protocol_detail) {
        google::SetVLOGLevel("*", FLAGS_vlog_level);
        FLAGS_v = FLAGS_vlog_level;
    }
    
    // 测试参数
    const int duration_ms = 2000;  // 每个测试运行2秒
    const int threads = 4;
    const int queue_size = 32;
    
    std::vector<EVMType> evm_types = {
        EVMType::STRAWMAN,     // 传统锁
        EVMType::COPYONWRITE   // MVCC
    };
    
    std::vector<std::string> type_names = {
        "STRAWMAN (传统锁)",
        "COPYONWRITE (MVCC)"
    };
    
    for (size_t i = 0; i < evm_types.size(); i++) {
        LOG(INFO) << "\n\n=================================================";
        LOG(INFO) << "测试 " << type_names[i] << " 开始";
        
        auto statistics = Statistics();
        auto workload = CrossContract();
        workload.SetEVMType(evm_types[i]);
        
        auto protocol = Spectrum(workload, statistics, threads, queue_size, evm_types[i]);
        
        // 启动并运行
        protocol.Start();
        std::this_thread::sleep_for(std::chrono::milliseconds(duration_ms));
        protocol.Stop();
        
        // 打印结果
        LOG(INFO) << "\n===== " << type_names[i] << " 测试结果 =====";
        std::string stats = statistics.Print();
        LOG(INFO) << "\n<<<统计信息开始>>>";
        LOG(INFO) << stats;
        LOG(INFO) << "<<<统计信息结束>>>";
        
        LOG(INFO) << "=================================================\n";
    }
}

} // namespace 