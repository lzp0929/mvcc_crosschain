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
#include <mutex>
#include <thread>
#include <vector>
#include <random>
#include <condition_variable>
#include <glog/logging.h>
#include <evmc/evmc.h>
#include <sstream>

namespace spectrum {

using KeyHasher = spectrum::KeyHasher;
using Statistics = spectrum::Statistics;
using SpinLock = spectrum::SpinLock;
using namespace std::chrono;

// 定义键类型
using K = std::tuple<evmc::address, evmc::bytes32>;

// 跨链锁延迟时间
constexpr auto CROSS_CHAIN_LOCK_DELAY = std::chrono::seconds(2);

/// @brief 交易类型枚举
enum class TxType {
    REGULAR,      // 普通交易
    CROSS_CHAIN   // 跨链交易
};

/// @brief 事务结构
class TPLBlockTransaction : public Transaction {
public:
    using ReadSet = std::unordered_map<K, evmc::bytes32, KeyHasher>;
    using WriteSet = std::unordered_map<K, evmc::bytes32, KeyHasher>;
    
    TPLBlockTransaction(Transaction&& tx, size_t id, size_t block_id, TxType type);
    
    const size_t id;        // 事务ID
    const size_t block_id;  // 区块ID
    const TxType type;      // 事务类型
    std::atomic<bool> committed{false};  // 是否已提交
    std::chrono::time_point<steady_clock> start_time;  // 开始时间
    std::chrono::time_point<steady_clock> ready_time;  // 跨链交易完成时间
    bool delay_completed{false};  // 是否完成延迟
    
    ReadSet read_set_;      // 读集
    WriteSet write_set_;    // 写集
    bool growing_phase = true;  // 是否处于增长阶段
    
    // 跨链交易相关
    void SetReadyTime();
    bool IsReadyForFinalize() const;
    void SetDelayCompleted() { delay_completed = true; }
    TxType GetType() const { return type; }
    
    // 禁用拷贝和移动
    TPLBlockTransaction(const TPLBlockTransaction&) = delete;
    TPLBlockTransaction& operator=(const TPLBlockTransaction&) = delete;
    TPLBlockTransaction(TPLBlockTransaction&&) = delete;
    TPLBlockTransaction& operator=(TPLBlockTransaction&&) = delete;
};

/// @brief 两阶段锁表项
struct TPLBlockEntry {
    evmc::bytes32 value = evmc::bytes32{0};
    std::unique_ptr<std::shared_mutex> mutex;
    
    TPLBlockEntry() : mutex(std::make_unique<std::shared_mutex>()) {}
    
    TPLBlockEntry(TPLBlockEntry&& other) noexcept
        : value(other.value)
        , mutex(std::move(other.mutex)) {}
    
    TPLBlockEntry& operator=(TPLBlockEntry&& other) noexcept {
        if (this != &other) {
            value = other.value;
            mutex = std::move(other.mutex);
        }
        return *this;
    }
    
    TPLBlockEntry(const TPLBlockEntry&) = delete;
    TPLBlockEntry& operator=(const TPLBlockEntry&) = delete;
};

/// @brief 两阶段锁表
class TPLBlockTable {
private:
    static constexpr size_t NUM_SEGMENTS = 64;
    std::array<SpinLock, NUM_SEGMENTS> segment_locks;
    std::array<std::unordered_map<K, TPLBlockEntry, KeyHasher>, NUM_SEGMENTS> segments;

    size_t GetSegment(const K& key) const {
        return KeyHasher()(key) % NUM_SEGMENTS;
    }

public:
    TPLBlockTable() = default;
    
    bool Get(const K& key, evmc::bytes32& value, TPLBlockTransaction& tx);
    bool Put(const K& key, const evmc::bytes32& value, TPLBlockTransaction& tx);
    evmc::bytes32 ReadForSet(const K& key, TPLBlockTransaction& tx);
    void WriteForSet(const K& key, const evmc::bytes32& value, TPLBlockTransaction& tx);
    bool AcquireLocks(TPLBlockTransaction& tx);
    bool ValidateReadSet(TPLBlockTransaction& tx);
    void CommitAndRelease(TPLBlockTransaction& tx);
    void CommitAndDelayRelease(TPLBlockTransaction& tx);
    void AbortAndRelease(TPLBlockTransaction& tx);
};

// 自定义屏障同步类
class SimpleBarrier {
public:
    SimpleBarrier(size_t count) : count_(count), waiting_(0), generation_(0) {}
    
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

/// @brief 区块两阶段锁协议
class TwoPhaseLockBlock: public Protocol {
public:
    TwoPhaseLockBlock(
        Workload& workload, 
        Statistics& statistics, 
        size_t num_threads,
        EVMType evm_type,
        size_t txs_per_block = 200,
        double block_interval_seconds = 2.0,
        double cross_chain_ratio = 0.1
    );
    
    void Start() override;
    void Stop() override;
    
    // 区块信息查询接口
    std::string GetBlockInfo(size_t block_id);
    std::string GetAllBlocksInfo();

private:
    // 区块信息记录结构
    struct BlockInfo {
        size_t block_id;
        size_t tx_count{0};
        size_t cross_chain_tx_count{0};
        size_t regular_tx_count{0};
        size_t aborted_tx_count{0};
        steady_clock::time_point block_start_time;
        steady_clock::time_point block_end_time;
        double total_latency{0.0};
        double avg_latency{0.0};
    };
    
    // 区块记录列表
    std::vector<BlockInfo> block_records;
    mutable std::mutex block_records_mutex;
    
    // 活跃交易列表
    std::vector<std::shared_ptr<TPLBlockTransaction>> active_transactions;
    std::mutex transactions_mutex;
    
    // 工作负载和统计
    Workload& workload;
    Statistics& statistics;
    TPLBlockTable table;
    
    // 执行参数
    size_t num_threads;
    size_t txs_per_block;
    double block_interval_seconds;
    double cross_chain_ratio;
    
    // 区块和交易状态
    std::atomic<size_t> current_block_id{1};
    std::atomic<size_t> txs_in_current_block{0};
    std::atomic<size_t> last_executed{1};
    std::atomic<bool> stop_flag{false};
    
    // 区块暂停控制
    std::atomic<bool> block_paused{false};
    std::mutex block_mutex;
    std::condition_variable block_cv;
    
    // 执行组件
    std::vector<std::thread> executors;
    std::thread block_manager;
    SimpleBarrier stop_barrier;
    
    // 私有方法
    void BlockControllerTask();  // 区块控制任务
    void OnBlockComplete(size_t block_id);  // 区块完成处理
    
    // 随机数生成器
    std::mt19937 rng{std::random_device{}()};
    std::uniform_real_distribution<double> dist{0.0, 1.0};
};

} // namespace spectrum 