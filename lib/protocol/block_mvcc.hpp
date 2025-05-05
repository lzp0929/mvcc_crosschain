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
#include <condition_variable>
#include <chrono>

namespace spectrum {

// some shorthands to prevent prohibitively long names
#define K std::tuple<evmc::address, evmc::bytes32>
#define V BlockMVCCVersionList
#define T BlockMVCCTransaction

using namespace std::chrono;

struct BlockMVCCPutTuple {
    K               key;
    evmc::bytes32   value;
    bool            is_committed;
};

struct BlockMVCCGetTuple {
    K               key;
    evmc::bytes32   value;
    size_t          version;
    size_t          tuples_put_len;
    size_t          checkpoint_id;
};

struct BlockMVCCTransaction: public Transaction {
    size_t      id;
    size_t      should_wait{0};
    SpinLock            rerun_keys_mu;
    std::vector<K>      rerun_keys;
    std::atomic<bool>   berun_flag{false};
    time_point<steady_clock>            start_time;
    std::vector<BlockMVCCGetTuple>      tuples_get{};
    std::vector<BlockMVCCPutTuple>      tuples_put{};
    BlockMVCCTransaction(Transaction&& inner, size_t id);
    bool HasWAR();
    void SetWAR(const K& key, size_t cause_id);
};

struct BlockMVCCEntry {
    evmc::bytes32   value;
    size_t          version;
    // we store raw pointers here because when a transaction is destructed, it always removes itself from table.
    std::unordered_set<T*>  readers;
};

struct BlockMVCCVersionList {
    T*          tx = nullptr;
    std::list<BlockMVCCEntry> entries;
    // readers that read default value
    std::unordered_set<T*>  readers_default;
};

struct BlockMVCCTable: private Table<K, V, KeyHasher> {
    BlockMVCCTable(size_t partitions);
    void Get(T* tx, const K& k, evmc::bytes32& v, size_t& version);
    void Put(T* tx, const K& k, const evmc::bytes32& v);
    void RegretGet(T* tx, const K& k, size_t version);
    void RegretPut(T* tx, const K& k);
    void ClearGet(T* tx, const K& k, size_t version);
    void ClearPut(T* tx, const K& k);
};

using BlockMVCCQueue = LockPriorityQueue<T>;
class BlockMVCCExecutor;

class BlockMVCC: public Protocol {
private:
    // 基本参数
    size_t              num_executors;
    Workload&           workload;
    BlockMVCCTable      table;
    Statistics&         statistics;
    
    // 事务计数和状态
    std::atomic<size_t> last_executed{1};
    std::atomic<size_t> last_finalized{0};
    std::atomic<bool>   stop_flag{false};
    
    // 批处理控制
    std::atomic<size_t> tx_count_in_block{0};     // 当前区块中的交易数量
    std::atomic<bool>   block_generation_paused{false}; // 是否暂停生成
    std::mutex          block_mutex;              // 区块同步互斥锁
    std::condition_variable block_cv;             // 区块同步条件变量
    size_t              block_size{200};          // 每个区块的事务数量
    milliseconds        block_interval{2000};     // 区块间隔(毫秒)
    
    // 执行器线程
    std::vector<std::thread> executors{};
    std::barrier<std::function<void()>> stop_latch;
    
    // 区块控制线程
    std::thread block_controller;
    
    // 区块控制器函数
    void BlockControllerTask();
    
    friend class BlockMVCCExecutor;

public:
    BlockMVCC(Workload& workload, Statistics& statistics, 
             size_t num_executors, size_t table_partitions, 
             EVMType evm_type, 
             size_t block_size = 200, 
             milliseconds block_interval = milliseconds(2000));
    void Start() override;
    void Stop() override;
};

class BlockMVCCExecutor {
private:
    Workload&               workload;
    BlockMVCCTable&         table;
    Statistics&             statistics;
    std::atomic<size_t>&    last_executed;
    std::atomic<size_t>&    last_finalized;
    std::atomic<bool>&      stop_flag;
    std::atomic<size_t>&    tx_count_in_block;
    std::atomic<bool>&      block_generation_paused;
    std::mutex&             block_mutex;
    std::condition_variable& block_cv;
    
    BlockMVCCQueue          queue;
    std::unique_ptr<T>      tx{nullptr};
    std::barrier<std::function<void()>>& stop_latch;

public:
    BlockMVCCExecutor(BlockMVCC& block_mvcc);
    void Finalize();
    void Generate();
    void ReExecute();
    void Run();
};

#undef T
#undef V
#undef K

} // namespace spectrum 