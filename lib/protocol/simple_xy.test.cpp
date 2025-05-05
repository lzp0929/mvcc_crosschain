#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <spectrum/protocol/spectrum.hpp>
#include <spectrum/transaction/evm-transaction.hpp>
#include <spectrum/workload/simple_xy.hpp>
#include <spectrum/common/glog-prefix.hpp>

namespace {

using namespace spectrum;
using namespace std::chrono_literals;

TEST(Spectrum, SimpleXYFunctionCall) {
    google::InstallPrefixFormatter(PrefixFormatter);
    
    // 创建统计对象
    auto statistics = Statistics();
    
    // 创建SimpleXY工作负载 - 这会提供函数间调用交易
    auto workload = SimpleXY();
    
    // 创建Spectrum协议实例
    auto protocol = Spectrum(workload, statistics, 2, 32, EVMType::COPYONWRITE);
    
    // 启动协议
    protocol.Start();
    
    // 让协议运行一段时间处理交易
    std::this_thread::sleep_for(15ms);
    
    // 停止协议
    protocol.Stop();
    
    // 打印统计信息
    LOG(INFO) << "\n\n===== 交易执行完成 =====";
    std::string stats = statistics.Print();
    LOG(INFO) << "\n<<< 统计信息输出 >>>";
    LOG(INFO) << stats;
    LOG(INFO) << "<<< 统计信息结束 >>>";
}

} // namespace 