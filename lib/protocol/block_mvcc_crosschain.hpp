#pragma once

#include <spectrum/protocol/abstraction.hpp>
#include <spectrum/common/statistics.hpp>
#include <spectrum/common/lock-util.hpp>
#include <spectrum/workload/abstraction.hpp>
#include <evmc/evmc.h>
#include <chrono>
#include <atomic>
#include <random>
#include <shared_mutex>
#include <thread>
#include <list>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <optional>
#include <mutex>
#include <condition_variable>
#include <glog/logging.h>
#include <memory>
#include <sstream>
#include <fmt/format.h>
#include <fstream>
#include <spectrum/transaction/evm-hash.hpp>  // 替代 key-hash.hpp

namespace spectrum {

using Statistics = spectrum::Statistics;
using namespace std::chrono;

// 定义键类型
using K = std::tuple<evmc::address, evmc::bytes32>;

// 交易类型
enum class BlockTxType {
    REGULAR,      // 普通交易
    CROSS_CHAIN   // 跨链交易
};

// 读写条目
struct BlockMVCCTuple {
    K key;
    evmc::bytes32 value;
};

// 区块MVCC跨链事务
class BlockMVCCTransaction : public Transaction {
public:
    BlockMVCCTransaction(Transaction&& inner, size_t id, size_t block_id, BlockTxType type);
    
    bool HasWAR();
    void SetWAR(const K& key, size_t cause_id);
    
    // 跨链交易相关
    void SetReadyTime();
    bool IsReadyForFinalize() const;
    void SetDelayCompleted() { delay_completed = true; }
    BlockTxType GetType() const { return type; }
    
    // 事务信息
    size_t id;
    size_t block_id;
    BlockTxType type;
    SpinLock rerun_keys_mu;
    std::vector<K> rerun_keys;
    size_t should_wait{0};
    std::atomic<bool> berun_flag{false};
    std::atomic<bool> is_aborted{false};  // 添加中止标记
    
    // 时间相关
    time_point<steady_clock> start_time;
    time_point<steady_clock> ready_time;
    bool delay_completed{false};
    
    // 读写集
    struct GetTuple {
        K key;
        evmc::bytes32 value;
        size_t version;
        size_t tuples_put_len;
        size_t checkpoint_id;
    };
    
    struct PutTuple {
        K key;
        evmc::bytes32 value;
        bool is_committed;
    };
    
    std::vector<GetTuple> tuples_get;
    std::vector<PutTuple> tuples_put;
};

// 版本条目
struct BlockMVCCEntry {
    evmc::bytes32 value;
    size_t version;
    std::unordered_set<BlockMVCCTransaction*> readers;
};

// 版本列表
struct BlockMVCCVersionList {
    BlockMVCCTransaction* tx = nullptr;
    std::list<BlockMVCCEntry> entries;
    std::unordered_set<BlockMVCCTransaction*> readers_default;
};

// 区块MVCC跨链表
class BlockMVCCTable {
public:
    BlockMVCCTable(size_t partitions);
    
    void Get(BlockMVCCTransaction* tx, const K& k, evmc::bytes32& v, size_t& version);
    void Put(BlockMVCCTransaction* tx, const K& k, const evmc::bytes32& v);
    void RegretGet(BlockMVCCTransaction* tx, const K& k, size_t version);
    void RegretPut(BlockMVCCTransaction* tx, const K& k);
    void ClearGet(BlockMVCCTransaction* tx, const K& k, size_t version);
    void ClearPut(BlockMVCCTransaction* tx, const K& k);
    
private:
    Table<K, BlockMVCCVersionList, KeyHasher> table;
};

// 自定义屏障同步类（代替std::barrier）
class SimpleBarrier {
public:
    SimpleBarrier(size_t count) : count_(count), waiting_(0), generation_(0) {}
    
    void arrive_and_wait() {
        std::unique_lock<std::mutex> lock(mutex_);
        size_t gen = generation_;
        
        if (++waiting_ == count_) {
            // 当前批次的所有线程都已到达
            generation_++;
            waiting_ = 0;
            // 唤醒所有等待的线程
            cv_.notify_all();
        } else {
            // 等待当前批次完成
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

// 执行器类
class BlockMVCCExecutor;

// 区块MVCC跨链协议实现
class BlockMVCCCrossChain : public Protocol {
public:
    BlockMVCCCrossChain(
        Workload& workload, 
        Statistics& statistics, 
        size_t num_executors,
        size_t table_partitions,
        EVMType evm_type,
        size_t txs_per_block = 200,
        double block_interval_seconds = 2.0,
        double cross_chain_ratio = 0.1
    );
    
    // 协议接口实现
    void Start() override;
    void Stop() override;
    
    // 区块控制任务
    void BlockControllerTask();
    
    // 区块信息查询接口
    std::string GetBlockInfo(size_t block_id) const;
    std::string GetAllBlocksInfo() const;
    
    // 添加区块信息结构体
    struct BlockInfo {
        size_t block_id;
        size_t tx_count{0};           // 区块内交易总数
        size_t cross_chain_tx_count{0}; // 跨链交易数量
        size_t regular_tx_count{0};     // 普通交易数量
        size_t aborted_tx_count{0};     // 中止交易数量
    };

private:
    // 添加区块记录列表
    std::vector<BlockInfo> block_records;
    mutable std::mutex block_records_mutex;
    
    // 活跃交易列表
    std::vector<std::shared_ptr<BlockMVCCTransaction>> active_transactions;
    std::mutex transactions_mutex;
    
    // 区块完成处理
    void OnBlockComplete(size_t block_id);
    
    // 工作负载和统计
    Workload& workload;
    Statistics& statistics;
    
    // 执行参数
    size_t num_executors;
    size_t txs_per_block;
    double block_interval_seconds;
    double cross_chain_ratio;
    
    // 区块和交易状态
    std::atomic<size_t> current_block_id{1};
    std::atomic<size_t> txs_in_current_block{0};
    std::atomic<size_t> last_executed{1};
    std::atomic<size_t> last_finalized{0};
    std::atomic<bool> stop_flag{false};
    
    // 区块暂停控制
    std::atomic<bool> block_paused{false};
    std::mutex block_mutex;
    std::condition_variable block_cv;
    
    // 执行组件
    BlockMVCCTable table;
    std::vector<std::thread> executors;
    std::thread block_manager;
    SimpleBarrier stop_barrier;
    
    // 添加统计前100笔交易的相关变量
    std::atomic<bool> first_tx_started{false};
    steady_clock::time_point first_tx_time;
    steady_clock::time_point latest_finish_time;
    steady_clock::time_point latest_cross_chain_finish_time;
    std::atomic<size_t> completed_tx_count{0};
    std::ofstream latency_log;
    std::mutex latency_log_mutex;
    
    friend class BlockMVCCExecutor;
};

// 执行器类
class BlockMVCCExecutor {
public:
    BlockMVCCExecutor(BlockMVCCCrossChain& protocol);
    
    void Run();
    
private:
    // 主要操作
    void Generate();
    void ReExecute();
    void Finalize();
    
    // 协议引用
    BlockMVCCCrossChain& protocol;
    Workload& workload;
    BlockMVCCTable& table;
    Statistics& statistics;
    
    // 共享状态引用
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
    
    // 本地状态
    std::unique_ptr<BlockMVCCTransaction> tx{nullptr};
    
    // 随机数生成
    std::random_device rd;
    std::mt19937 rng{rd()};
    std::uniform_real_distribution<double> dist{0.0, 1.0};
};
} // namespace spectrum     