#pragma once

#include <spectrum/protocol/abstraction.hpp>
#include <spectrum/common/statistics.hpp>
#include <spectrum/workload/abstraction.hpp>
#include <spectrum/common/lock-util.hpp>
#include <evmc/evmc.hpp>
#include <glog/logging.h>
#include <atomic>
#include <shared_mutex>
#include <unordered_map>
#include <queue>
#include <vector>
#include <chrono>
#include <map>
#include <thread>
#include <mutex>

namespace spectrum {

using K = std::tuple<evmc::address, evmc::bytes32>;
using KeyHasher = spectrum::KeyHasher;
using Statistics = spectrum::Statistics;
using SpinLock = spectrum::SpinLock;

// 定义锁模式
enum class LockMode {
    SHARED,
    EXCLUSIVE
};

// 定义等待请求结构
struct WaitRequest {
    size_t tx_id;
    LockMode mode;
    std::chrono::steady_clock::time_point request_time;
    
    bool operator<(const WaitRequest& other) const {
        return request_time > other.request_time;
    }
};

// 定义锁条目类
class LockEntry {
public:
    bool canAcquire(size_t tx_id, LockMode mode);
    bool addHolder(size_t tx_id, LockMode mode);
    void removeHolder(size_t tx_id);
    std::vector<size_t> getConflictingTxs(size_t tx_id);

    std::map<size_t, LockMode> holders;
    std::priority_queue<WaitRequest> wait_queue;
};

// 定义锁管理器类
class WWLockManager {
public:
    static constexpr size_t NUM_SEGMENTS = 1024;
    
    bool acquireLock(size_t tx_id, const K& key, LockMode mode);
    void releaseLocks(size_t tx_id);

private:
    std::vector<SpinLock> segment_locks{NUM_SEGMENTS};
    std::vector<std::unordered_map<K, LockEntry, KeyHasher>> lock_tables{NUM_SEGMENTS};
    
    size_t getSegment(const K& key) {
        return KeyHasher()(key) % NUM_SEGMENTS;
    }
};

// 定义操作类型
struct Operation {
    enum class Type {
        GET,
        PUT
    };
    Type type;
    K key;
    evmc::bytes32 value;
};

// 事务类
class WWTransaction : public Transaction {
public:
    WWTransaction(std::unique_ptr<Transaction>&& inner, size_t id);
    
    const size_t id;
    std::atomic<bool> is_aborted{false};
    std::atomic<bool> is_committed{false};
    std::chrono::steady_clock::time_point start_time;
    
    std::vector<Operation> operations;
    std::unordered_map<K, evmc::bytes32, KeyHasher> read_set;
    std::unordered_map<K, evmc::bytes32, KeyHasher> write_set;

    bool Read(const K& key);
    bool Write(const K& key, const evmc::bytes32& value);
    bool Commit();
    void Abort();
};

// 两阶段锁定 WW 协议类
class TwoPhaseLockingWW {
public:
    TwoPhaseLockingWW(
        Workload& workload,
        Statistics& statistics,
        size_t num_threads,
        EVMType evm_type
    );
    
    void Start();
    void Stop();

private:
    Workload& workload;
    Statistics& statistics;
    size_t num_threads;
    std::atomic<bool> stop_flag{false};
    std::atomic<size_t> last_executed{0};
    
    WWLockManager lock_manager;
    std::vector<std::thread> executors;
    
    std::mutex retry_mutex;
    std::unordered_map<size_t, size_t> retry_map;
    
    void ExecutorThread(size_t thread_id);
    bool ExecuteTransaction(WWTransaction* tx);
    void AbortTransaction(WWTransaction* tx);
    bool HandleRetry(size_t tx_id);
};

} // namespace spectrum 