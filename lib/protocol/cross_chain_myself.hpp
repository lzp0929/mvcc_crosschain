#pragma once

// 标准库头文件
#include <list>
#include <atomic>
#include <tuple>
#include <vector>
#include <unordered_set>
#include <unordered_map>
#include <thread>
#include <chrono>
#include <memory>
#include <string>
#include <mutex>
#include <condition_variable>
#include <random>
#include <barrier>
#include <fstream>

// 第三方库头文件
#include <evmc/evmc.h>
#include <glog/logging.h>
#include <fmt/core.h>

// 项目内部头文件
#include <spectrum/workload/abstraction.hpp>
#include <spectrum/common/lock-util.hpp>
#include <spectrum/protocol/abstraction.hpp>
#include <spectrum/transaction/evm-hash.hpp>
#include <spectrum/common/statistics.hpp>

namespace spectrum {

// 基本类型定义
using K = std::tuple<evmc::address, evmc::bytes32>;
using namespace std::chrono;

// 自定义屏障同步类
class SimpleBarrier {
public:
    explicit SimpleBarrier(size_t count) : count_(count), waiting_(0), generation_(0) {}
    
    void arrive_and_wait() {
        std::unique_lock<std::mutex> lock(mutex_);
        size_t gen = generation_;
        
        if (++waiting_ == count_) {
            generation_++;
            waiting_ = 0;
            cv_.notify_all();
        } else {
            cv_.wait(lock, [this, gen] { return gen != generation_; });
        }
    }
    
private:
    std::mutex mutex_;
    std::condition_variable cv_;
    size_t count_;
    size_t waiting_;
    size_t generation_;
};

// 一些缩写以防止过长的名称
#define V MySelfVersionList
#define T MySelfTransaction

// 交易类型枚举
enum class MySelfTxType {
    REGULAR,     // 普通交易
    CROSS_CHAIN  // 跨链交易
};

// 添加交易记录结构体，用于存储前100笔交易数据
struct TransactionRecord {
    size_t tx_id;
    time_point<steady_clock> start_time;
    time_point<steady_clock> finish_time;
    MySelfTxType type;
};

// 复合事务体中的写集条目
struct WriteSetEntry {
    K key;
    evmc::bytes32 value;
    size_t writer_tx_id;  // 写入该值的事务ID
};

// 复合事务体
struct CompositeTransaction {
    size_t composite_id;  // 复合事务体ID
    std::vector<size_t> tx_list;  // 包含的交易ID列表
    std::vector<WriteSetEntry> write_set;  // 最终写集
    std::unordered_set<size_t> dependent_composites;  // 依赖的复合事务体ID列表
    bool is_cross_chain;  // 是否包含跨链交易
    
    CompositeTransaction(size_t id, bool is_cross_chain = false)
        : composite_id(id), is_cross_chain(is_cross_chain) {}
};

// 区块的复合事务体记录
struct BlockCompositeTxs {
    size_t block_id;
    std::vector<std::shared_ptr<CompositeTransaction>> composites;
    std::string dependency_graph;
    time_point<steady_clock> block_start_time{steady_clock::now()};
    time_point<steady_clock> block_end_time;
    size_t total_memory_usage{0};  // 添加内存使用统计
    size_t tx_count{0};            // 区块内总交易数
    size_t cross_chain_tx_count{0}; // 跨链交易数
    size_t regular_tx_count{0};     // 普通交易数
    size_t aborted_tx_count{0};      // 添加中止交易计数
};

struct MySelfPutTuple {
    K key;
    evmc::bytes32 value;
    bool is_committed;
};

struct MySelfGetTuple {
    K key;
    evmc::bytes32 value;
    size_t version;
    size_t writer_tx_id;  // 记录写入该版本的事务ID
    size_t tuples_put_len;
    size_t checkpoint_id;
};

struct MySelfTransaction: public Transaction {
    size_t id;
    size_t block_id;  // 所属区块ID
    size_t should_wait{0};
    MySelfTxType type;
    SpinLock rerun_keys_mu;
    std::vector<K> rerun_keys;
    std::atomic<bool> berun_flag{false};
    std::atomic<bool> completed{false};  // 添加完成标记
    time_point<steady_clock> start_time;
    time_point<steady_clock> finalize_time;   // 交易完成时间
    bool is_finalized{false};                 // 是否已完成
    std::vector<MySelfGetTuple> tuples_get{};
    std::vector<MySelfPutTuple> tuples_put{};
    std::shared_ptr<CompositeTransaction> composite_tx{nullptr};  // 所属的复合事务体
    
    MySelfTransaction(Transaction&& inner, size_t id, size_t block_id, MySelfTxType type = MySelfTxType::REGULAR);
    bool HasWAR();
    void SetWAR(const K& key, size_t cause_id);
    MySelfTxType GetType() const { return type; }
    
    // 获取实际完成时间（考虑跨链延迟）
    time_point<steady_clock> GetActualFinishTime() const {
        if (type == MySelfTxType::CROSS_CHAIN) {
            return finalize_time + seconds(2);
        }
        return finalize_time;
    }
    
    // 获取交易延迟（微秒）
    int64_t GetLatency() const {
        return duration_cast<microseconds>(GetActualFinishTime() - start_time).count();
    }
};

struct MySelfEntry {
    evmc::bytes32 value;
    size_t version;
    size_t writer_tx_id;  // 写入该版本的事务ID
    std::unordered_set<T*> readers;
};

struct MySelfVersionList {
    T* tx = nullptr;
    std::list<MySelfEntry> entries;
    std::unordered_set<T*> readers_default;
};

struct MySelfTable: private Table<K, V, KeyHasher> {
    MySelfTable(size_t partitions);
    void Get(T* tx, const K& k, evmc::bytes32& v, size_t& version, size_t& writer_tx_id);
    void Put(T* tx, const K& k, const evmc::bytes32& v);
    void RegretGet(T* tx, const K& k, size_t version);
    void RegretPut(T* tx, const K& k);
    void ClearGet(T* tx, const K& k, size_t version);
    void ClearPut(T* tx, const K& k);
};

using MySelfQueue = LockPriorityQueue<T>;

// 前向声明
class MySelfExecutor;

class CrossChainMyself: public Protocol {
private:
    // 执行参数
    size_t              num_executors;
    Workload&           workload;
    MySelfTable         table;
    Statistics&         statistics;
    std::atomic<size_t> last_executed{1};
    std::atomic<size_t> last_finalized{0};
    std::atomic<bool>   stop_flag{false};
    std::atomic<bool>   is_running{false};
    std::atomic<size_t> current_block_id{1};
    std::atomic<size_t> txs_in_current_block{0};
    std::atomic<bool>   block_paused{false};
    std::mutex         block_mutex;
    std::condition_variable block_cv;
    std::vector<std::thread> executors;
    std::thread        block_manager;
    size_t            txs_per_block;
    double           block_interval_seconds;
    double           cross_chain_ratio;
    
    // 使用 SimpleBarrier 替代 std::barrier
    SimpleBarrier stop_barrier;
    
    // 复合事务体管理
    std::atomic<size_t> next_composite_id{1};
    SpinLock composite_lock;
    std::unordered_map<size_t, std::shared_ptr<CompositeTransaction>> composite_txs;
    
    // 区块记录管理
    std::mutex block_records_mutex;  // 保护区块记录的互斥锁
    
    // 添加统计前100笔交易的相关变量
    std::atomic<bool> first_tx_started{false};
    steady_clock::time_point first_tx_time{steady_clock::now()};  // 初始化为当前时间
    steady_clock::time_point latest_finish_time{steady_clock::now()};  // 初始化为当前时间
    steady_clock::time_point latest_cross_chain_finish_time{steady_clock::now()};  // 初始化为当前时间
    std::atomic<size_t> completed_tx_count{0};
    std::ofstream latency_log;
    std::mutex latency_log_mutex;
    
    // 添加存储前100笔交易记录的容器
    std::vector<TransactionRecord> tx_records;
    
    // 区块的复合事务体记录
    std::vector<BlockCompositeTxs> block_records;
    
    friend class MySelfExecutor;
    
public:
    CrossChainMyself(Workload& workload, Statistics& statistics,
                    size_t num_executors, size_t table_partitions,
                    EVMType evm_type, size_t txs_per_block,
                    double block_interval_seconds, double cross_chain_ratio);
                    
    void Start() override;
    void Stop() override;
    
    // 区块控制任务
    void BlockControllerTask();
    
    // 复合事务体管理方法
    std::shared_ptr<CompositeTransaction> CreateCompositeTransaction(bool is_cross_chain);
    void AddTransactionToComposite(MySelfTransaction* tx);
    std::vector<std::shared_ptr<CompositeTransaction>> GetDependentCompositeTxs(MySelfTransaction* tx);
    
    // 区块完成处理
    void OnBlockComplete(size_t block_id);
    
    // 获取复合事务体信息
    std::string GetBlockCompositeTxsInfo(size_t block_id) const;
    std::string GetAllBlocksCompositeTxsInfo() const;
};

class MySelfExecutor {
private:
    CrossChainMyself& protocol;
    Workload& workload;
    MySelfTable& table;
    Statistics& statistics;
    std::atomic<size_t>& last_executed;
    std::atomic<size_t>& last_finalized;
    std::atomic<bool>& stop_flag;
    std::atomic<size_t>& current_block_id;
    std::atomic<bool>& block_paused;
    std::mutex& block_mutex;
    std::condition_variable& block_cv;
    std::atomic<size_t>& txs_in_current_block;
    double cross_chain_ratio;
    SimpleBarrier& stop_barrier;
    
    MySelfQueue queue;
    std::unique_ptr<T> tx{nullptr};
    
    // 随机数生成
    std::random_device rd;
    std::mt19937 rng{rd()};
    std::uniform_real_distribution<double> dist{0.0, 1.0};
    
public:
    MySelfExecutor(CrossChainMyself& protocol);
    void Finalize();
    void Generate();
    void ReExecute();
    void Run();
};

#undef T
#undef V
} // namespace spectrum 
 // namespace spectrum 