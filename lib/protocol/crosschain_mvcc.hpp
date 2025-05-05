#pragma once

#include <spectrum/workload/abstraction.hpp>
#include <spectrum/common/lock-util.hpp>
#include <spectrum/protocol/abstraction.hpp>
#include <spectrum/transaction/evm-hash.hpp>
#include <spectrum/common/statistics.hpp>
#include <list>
#include <atomic>
#include <tuple>
#include <vector>
#include <unordered_set>
#include <unordered_map>
#include <thread>
#include <barrier>
#include <chrono>
#include <string>
#include <fmt/core.h>

namespace spectrum {

// 一些缩写以防止过长的名称
#define K std::tuple<evmc::address, evmc::bytes32>
#define V CrosschainMVCCVersionList
#define T CrosschainMVCCTransaction

using namespace std::chrono;

// 读集中的项，记录了读取的键、值及写入该值的事务ID
struct ReadSetEntry {
    K key;
    evmc::bytes32 value;
    size_t writer_tx_id;  // 记录是哪个事务写入的版本
    std::string toString() const {
        auto key_hash = KeyHasher()(key) % 1000;
        return fmt::format("键:{} 值:{} 写入事务ID:{}", 
                           key_hash, 
                           hex(value), 
                           writer_tx_id);
    }
};

struct CrosschainMVCCPutTuple {
    K               key;
    evmc::bytes32   value;
    bool            is_committed;
};

struct CrosschainMVCCGetTuple {
    K               key;
    evmc::bytes32   value;
    size_t          version;
    size_t          tuples_put_len;
    size_t          checkpoint_id;
    size_t          writer_tx_id;  // 增加了写入者ID
};

struct CrosschainMVCCTransaction: public Transaction {
    size_t      id;
    size_t      should_wait{0};
    SpinLock            rerun_keys_mu;
    std::vector<K>      rerun_keys;
    std::atomic<bool>   berun_flag{false};
    time_point<steady_clock> start_time;
    std::vector<CrosschainMVCCGetTuple> tuples_get{};
    std::vector<CrosschainMVCCPutTuple> tuples_put{};
    std::vector<ReadSetEntry> read_set{}; // 最终读集记录
    
    CrosschainMVCCTransaction(Transaction&& inner, size_t id);
    bool HasWAR();
    void SetWAR(const K& key, size_t cause_id);
    void RecordReadSet(); // 记录最终读集
};

struct CrosschainMVCCEntry {
    evmc::bytes32   value;
    size_t          version;
    size_t          writer_tx_id; // 写入该版本的事务ID
    // 存储原始指针，因为当事务销毁时，它总是从表中移除自身
    std::unordered_set<T*>  readers;
};

struct CrosschainMVCCVersionList {
    T*          tx = nullptr;
    std::list<CrosschainMVCCEntry> entries;
    // 读取默认值的读取器
    std::unordered_set<T*>  readers_default;
};

struct CrosschainMVCCTable: private Table<K, V, KeyHasher> {
    CrosschainMVCCTable(size_t partitions);
    void Get(T* tx, const K& k, evmc::bytes32& v, size_t& version, size_t& writer_tx_id);
    void Put(T* tx, const K& k, const evmc::bytes32& v);
    void RegretGet(T* tx, const K& k, size_t version);
    void RegretPut(T* tx, const K& k);
    void ClearGet(T* tx, const K& k, size_t version);
    void ClearPut(T* tx, const K& k);
};

using CrosschainMVCCQueue = LockPriorityQueue<T>;
class CrosschainMVCCExecutor;

class CrosschainMVCC: public Protocol {
private:
    size_t              num_executors;
    Workload&           workload;
    CrosschainMVCCTable table;
    Statistics&         statistics;
    std::atomic<size_t> last_executed{1};
    std::atomic<size_t> last_finalized{0};
    std::atomic<bool>   stop_flag{false};
    std::vector<std::thread>    executors{};
    std::barrier<std::function<void()>> stop_latch;
    friend class CrosschainMVCCExecutor;

public:
    CrosschainMVCC(Workload& workload, Statistics& statistics, size_t num_executors, size_t table_partitions, EVMType evm_type);
    void Start() override;
    void Stop() override;
};

class CrosschainMVCCExecutor {
private:
    Workload&               workload;
    CrosschainMVCCTable&    table;
    Statistics&             statistics;
    std::atomic<size_t>&    last_executed;
    std::atomic<size_t>&    last_finalized;
    std::atomic<bool>&      stop_flag;
    CrosschainMVCCQueue     queue;
    std::unique_ptr<T>      tx{nullptr};
    std::barrier<std::function<void()>>& stop_latch;

public:
    CrosschainMVCCExecutor(CrosschainMVCC& spectrum);
    void Finalize();
    void Generate();
    void ReExecute();
    void Run();
};

#undef T
#undef V
#undef K

} // namespace spectrum 