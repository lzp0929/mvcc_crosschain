#pragma once

#include <spectrum/workload/abstraction.hpp>
#include <spectrum/common/lock-util.hpp>
#include <spectrum/protocol/abstraction.hpp>
#include <spectrum/transaction/evm-hash.hpp>
#include <list>
#include <atomic>
#include <tuple>
#include <vector>
#include <unordered_set>
#include <thread>
#include <barrier>
#include <chrono>

namespace spectrum {

// 一些缩写以防止过长的名称
#define K std::tuple<evmc::address, evmc::bytes32>
#define V CrossChainVersionList
#define T CrossChainTransaction

using namespace std::chrono;

// 交易类型枚举
enum class TransactionType {
    REGULAR,     // 普通交易
    CROSS_CHAIN  // 跨链交易
};

struct CrossChainPutTuple {
    K               key;
    evmc::bytes32   value;
    bool            is_committed;
};

struct CrossChainGetTuple {
    K               key;
    evmc::bytes32   value;
    size_t          version;
    size_t          tuples_put_len;
    size_t          checkpoint_id;
};

struct CrossChainTransaction: public Transaction {
    size_t      id;
    size_t      should_wait{0};
    TransactionType type;  // 交易类型
    SpinLock            rerun_keys_mu;
    std::vector<K>      rerun_keys;
    std::atomic<bool>   berun_flag{false};
    time_point<steady_clock> start_time;
    std::chrono::steady_clock::time_point ready_time; // 跨链交易完成时间
    bool delay_completed{false}; // 是否已完成延迟
    std::vector<CrossChainGetTuple> tuples_get{};
    std::vector<CrossChainPutTuple> tuples_put{};
    
    CrossChainTransaction(Transaction&& inner, size_t id, TransactionType type = TransactionType::REGULAR);
    bool HasWAR();
    void SetWAR(const K& key, size_t cause_id);
    TransactionType GetType() const { return type; }
    void SetReadyTime(); // 设置跨链交易完成时间
    bool IsReadyForFinalize() const; // 检查是否可以finalize
    void SetDelayCompleted() { delay_completed = true; }
};

struct CrossChainEntry {
    evmc::bytes32   value;
    size_t          version;
    // 存储原始指针，因为当事务销毁时，它总是从表中移除自身
    std::unordered_set<T*>  readers;
};

struct CrossChainVersionList {
    T*          tx = nullptr;
    std::list<CrossChainEntry> entries;
    // 读取默认值的读取器
    std::unordered_set<T*>  readers_default;
};

struct CrossChainTable: private Table<K, V, KeyHasher> {
    CrossChainTable(size_t partitions);
    void Get(T* tx, const K& k, evmc::bytes32& v, size_t& version);
    void Put(T* tx, const K& k, const evmc::bytes32& v);
    void RegretGet(T* tx, const K& k, size_t version);
    void RegretPut(T* tx, const K& k);
    void ClearGet(T* tx, const K& k, size_t version);
    void ClearPut(T* tx, const K& k);
};

using CrossChainQueue = LockPriorityQueue<T>;
class CrossChainExecutor;

class SpectrumCrossChain: public Protocol {
private:
    size_t              num_executors;
    Workload&           workload;
    CrossChainTable     table;
    Statistics&         statistics;
    std::atomic<size_t> last_executed{1};
    std::atomic<size_t> last_finalized{0};
    std::atomic<bool>   stop_flag{false};
    std::vector<std::thread>    executors{};
    std::barrier<std::function<void()>> stop_latch;
    friend class CrossChainExecutor;

public:
    SpectrumCrossChain(Workload& workload, Statistics& statistics, size_t num_executors, size_t table_partitions, EVMType evm_type);
    void Start() override;
    void Stop() override;
};

class CrossChainExecutor {
private:
    Workload&               workload;
    CrossChainTable&        table;
    Statistics&             statistics;
    std::atomic<size_t>&    last_executed;
    std::atomic<size_t>&    last_finalized;
    std::atomic<bool>&      stop_flag;
    CrossChainQueue         queue;
    std::unique_ptr<T>      tx{nullptr};
    std::barrier<std::function<void()>>& stop_latch;

public:
    CrossChainExecutor(SpectrumCrossChain& spectrum);
    void Finalize();
    void Generate();
    void ReExecute();
    void Run();
};

#undef T
#undef V
#undef K

} // namespace spectrum 