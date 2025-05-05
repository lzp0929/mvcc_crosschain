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
#include <glog/logging.h>

namespace spectrum {

using KeyHasher = spectrum::KeyHasher;
using Statistics = spectrum::Statistics;
using SpinLock = spectrum::SpinLock;

#define K std::tuple<evmc::address, evmc::bytes32>

/// @brief 两阶段锁事务
struct TwoPhaseTransaction: public Transaction {
    size_t      id;
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
    
    explicit TwoPhaseTransaction(Transaction&& inner, size_t id);
};

/// @brief 两阶段锁表项
struct TPLEntry {
    evmc::bytes32 value = evmc::bytes32{0};
    std::unique_ptr<std::shared_mutex> mutex;  // 使用 unique_ptr 管理 shared_mutex
    
    TPLEntry() : mutex(std::make_unique<std::shared_mutex>()) {}
    TPLEntry(TPLEntry&&) = default;
    TPLEntry& operator=(TPLEntry&&) = default;
};

/// @brief 两阶段锁表
class TPLTable {
private:
    SpinLock table_lock;  // 全局表锁，使用SpinLock替代mutex
    std::unordered_map<K, TPLEntry, KeyHasher> table;

public:
    TPLTable() = default;
    
    // 直接读写操作
    bool Get(const K& key, evmc::bytes32& value, TwoPhaseTransaction& tx);
    bool Put(const K& key, const evmc::bytes32& value, TwoPhaseTransaction& tx);
    
    // 读取阶段：记录到读集中
    evmc::bytes32 ReadForSet(const K& key, TwoPhaseTransaction& tx);
    
    // 写入阶段：记录到写集中
    void WriteForSet(const K& key, const evmc::bytes32& value, TwoPhaseTransaction& tx);
    
    // 获取所有读写锁
    bool AcquireLocks(TwoPhaseTransaction& tx);
    
    // 验证读集
    bool ValidateReadSet(TwoPhaseTransaction& tx);
    
    // 提交事务并释放锁
    void CommitAndRelease(TwoPhaseTransaction& tx);
    
    // 放弃事务并释放锁
    void AbortAndRelease(TwoPhaseTransaction& tx);
    
    // 辅助方法：释放所有锁
    void ReleaseLocks(TwoPhaseTransaction& tx);
};

/// @brief 两阶段锁协议
class TwoPhaseLocking: public Protocol {
private:
    Workload& workload;
    size_t num_threads;
    Statistics& statistics;
    TPLTable table;
    std::atomic<bool> stop_flag{false};
    std::atomic<size_t> last_executed{1};
    std::vector<std::thread> executors;

public:
    TwoPhaseLocking(Workload& workload, Statistics& statistics, 
                    size_t num_threads, EVMType evm_type);
    void Start() override;
    void Stop() override;
};

#undef K

} // namespace spectrum 

