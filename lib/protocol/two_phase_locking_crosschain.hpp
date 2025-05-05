#pragma once
#include <functional>
#include <spectrum/common/lock-util.hpp>
#include <spectrum/transaction/evm-hash.hpp>
#include <spectrum/transaction/evm-transaction.hpp>
#include <spectrum/workload/abstraction.hpp>
#include <spectrum/protocol/abstraction.hpp>
#include <spectrum/common/statistics.hpp>
#include <tuple>
#include <list>
#include <unordered_map>
#include <optional>
#include <atomic>
#include <memory>
#include <chrono>
#include <shared_mutex>
#include <thread>
#include <vector>
#include <random>
#include <glog/logging.h>
#include <evmc/evmc.h>

namespace spectrum {

using KeyHasher = spectrum::KeyHasher;
using Statistics = spectrum::Statistics;
using SpinLock = spectrum::SpinLock;
using namespace std::chrono;

#define K std::tuple<evmc::address, evmc::bytes32>

/// @brief 交易类型枚举
enum class TxType {
    REGULAR,      // 普通交易
    CROSS_CHAIN   // 跨链交易
};

/// @brief 两阶段锁跨链事务
struct TPLCCTransaction: public Transaction {
    size_t  id;
    TxType  type;           // 交易类型
    std::atomic<bool>   committed{false};
    std::chrono::time_point<std::chrono::steady_clock> start_time;
    
    // 读写集类型定义
    struct ReadItem {
        K key;
        evmc::bytes32 value;
    };
    
    struct WriteItem {
        K key;
        evmc::bytes32 value;
    };
    
    // 读写集
    std::vector<ReadItem> read_set;
    std::vector<WriteItem> write_set;
    
    // 事务阶段标志
    bool growing_phase{true};
    bool shrinking_phase{false};
    
    // 构造函数
    explicit TPLCCTransaction(Transaction&& inner, size_t id, TxType type);
};

/// @brief 两阶段锁跨链表项
struct TPLCCEntry {
    evmc::bytes32 value = evmc::bytes32{0};
    std::unique_ptr<std::shared_mutex> mutex = std::make_unique<std::shared_mutex>();
};

/// @brief 两阶段锁跨链表
class TPLCCTable {
private:
    SpinLock table_lock;  // 全局表锁，使用SpinLock替代mutex
    std::unordered_map<K, TPLCCEntry, KeyHasher> table;

public:
    // 直接读写操作
    bool Get(const K& key, evmc::bytes32& value, TPLCCTransaction& tx);
    bool Put(const K& key, const evmc::bytes32& value, TPLCCTransaction& tx);
    
    // 读取阶段：记录到读集中
    evmc::bytes32 ReadForSet(const K& key, TPLCCTransaction& tx);
    
    // 写入阶段：记录到写集中
    void WriteForSet(const K& key, const evmc::bytes32& value, TPLCCTransaction& tx);
    
    // 获取所有读写锁
    bool AcquireLocks(TPLCCTransaction& tx);
    
    // 验证读集
    bool ValidateReadSet(TPLCCTransaction& tx);
    
    // 提交事务并释放锁（普通交易）
    void CommitAndRelease(TPLCCTransaction& tx);
    
    // 提交事务并延迟释放锁（跨链交易）
    void CommitAndDelayRelease(TPLCCTransaction& tx);
    
    // 放弃事务并释放锁
    void AbortAndRelease(TPLCCTransaction& tx);
    
    // 辅助方法：释放所有锁
    void ReleaseLocks(TPLCCTransaction& tx);
};

/// @brief 两阶段锁跨链协议
class TwoPhaseLockingCrossChain: public Protocol {
private:
    Workload& workload;
    size_t num_threads;
    Statistics& statistics;
    TPLCCTable table;
    std::atomic<bool> stop_flag{false};
    std::atomic<size_t> last_executed{1};
    std::vector<std::thread> executors;
    
    // 跨链交易比例 (0.0-1.0)
    double cross_chain_ratio{0.1};
    
    // 随机数生成器
    std::mt19937 rng{std::random_device{}()};
    std::uniform_real_distribution<double> dist{0.0, 1.0};

public:
    TwoPhaseLockingCrossChain(Workload& workload, Statistics& statistics, 
                              size_t num_threads, EVMType evm_type, 
                              double cross_chain_ratio = 0.1);
    void Start() override;
    void Stop() override;
};

#undef K

} // namespace spectrum 