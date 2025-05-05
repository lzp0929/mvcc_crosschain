#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <spectrum/protocol/spectrum.hpp>
#include <spectrum/transaction/evm-transaction.hpp>
#include <spectrum/workload/ycsb.hpp>
#include <spectrum/workload/smallbank.hpp>
#include <spectrum/common/glog-prefix.hpp>

namespace {

#define TX(CODE, INPUT) Transaction(EVMType::BASIC, evmc::address{0}, evmc::address{1}, std::span{(CODE)}, std::span<uint8_t>{(INPUT)})

using namespace spectrum;
using namespace std::chrono_literals;

TEST(Spectrum, JustRunYCSB) {
    google::InstallPrefixFormatter(PrefixFormatter);
    auto statistics = Statistics();
    auto workload = Smallbank(1000000, 0.0);
    auto protocol = Spectrum(workload, statistics, 36, 9973, EVMType::COPYONWRITE);
    protocol.Start();
    std::this_thread::sleep_for(2s);
    protocol.Stop();
        // 打印统计信息
    LOG(INFO) << "\n\n===== 交易执行完成 =====";
    std::string stats = statistics.Print();
    LOG(INFO) << "\n<<< 统计信息输出 >>>";
    LOG(INFO) << stats;
    LOG(INFO) << "<<< 统计信息结束 >>>";

}

}