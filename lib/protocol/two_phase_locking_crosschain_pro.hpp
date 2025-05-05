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
#include <condition_variable>
#include <thread>
#include <vector>
#include <random>
#include <glog/logging.h>
#include <fmt/core.h>
#include <evmc/evmc.h>
#include <fstream>

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
class TPLCCProTransaction : public Transaction {
public:
    using ReadSet = std::unordered_map<K, evmc::bytes32, KeyHasher>;
    using WriteSet = std::unordered_map<K, evmc::bytes32, KeyHasher>;
    
    // 声明构造函数，但不在头文件中定义
    TPLCCProTransaction(Transaction&& tx, size_t id, TxType type);
    
    const size_t id;        // 事务ID
    const TxType type;      // 事务类型
    std::atomic<bool> committed{false};  // 是否已提交
    std::chrono::time_point<steady_clock> start_time;  // 开始时间
    std::chrono::time_point<steady_clock> actual_finish_time;  // 实际完成时间（包含锁释放延迟）
    
    ReadSet read_set_;      // 读集
    WriteSet write_set_;    // 写集
    bool growing_phase = true;  // 是否处于增长阶段
    
    // 禁用拷贝
    TPLCCProTransaction(const TPLCCProTransaction&) = delete;
    TPLCCProTransaction& operator=(const TPLCCProTransaction&) = delete;
    
    // 允许移动
    TPLCCProTransaction(TPLCCProTransaction&&) = default;
    TPLCCProTransaction& operator=(TPLCCProTransaction&&) = default;
};

/// @brief 两阶段锁跨链表项
struct TPLCCProEntry {
    evmc::bytes32 value = evmc::bytes32{0};
    std::unique_ptr<std::shared_mutex> mutex;
    
    // 默认构造函数
    TPLCCProEntry() : mutex(std::make_unique<std::shared_mutex>()) {}
    
    // 移动构造函数
    TPLCCProEntry(TPLCCProEntry&& other) noexcept
        : value(other.value)
        , mutex(std::move(other.mutex)) {}
    
    // 移动赋值运算符
    TPLCCProEntry& operator=(TPLCCProEntry&& other) noexcept {
        if (this != &other) {
            value = other.value;
            mutex = std::move(other.mutex);
        }
        return *this;
    }
    
    // 禁用拷贝
    TPLCCProEntry(const TPLCCProEntry&) = delete;
    TPLCCProEntry& operator=(const TPLCCProEntry&) = delete;
};

/// @brief 优化版两阶段锁跨链表
class TPLCCProTable {
private:
    // 分段锁，用于保护表的不同部分
    static constexpr size_t NUM_SEGMENTS = 64;
    std::array<SpinLock, NUM_SEGMENTS> segment_locks;
    std::array<std::unordered_map<K, TPLCCProEntry, KeyHasher>, NUM_SEGMENTS> segments;

    // 获取键所属的分段
    size_t GetSegment(const K& key) const {
        return KeyHasher()(key) % NUM_SEGMENTS;
    }

public:
    TPLCCProTable() = default;
    
    // 读取操作
    bool Get(const K& key, evmc::bytes32& value, TPLCCProTransaction& tx);
    
    // 写入操作
    bool Put(const K& key, const evmc::bytes32& value, TPLCCProTransaction& tx);
    
    // 读取阶段：记录到读集中
    evmc::bytes32 ReadForSet(const K& key, TPLCCProTransaction& tx);
    
    // 写入阶段：记录到写集中
    void WriteForSet(const K& key, const evmc::bytes32& value, TPLCCProTransaction& tx);
    
    // 获取所有读写锁
    bool AcquireLocks(TPLCCProTransaction& tx);
    
    // 验证读集
    bool ValidateReadSet(TPLCCProTransaction& tx);
    
    // 提交事务并释放锁（普通交易）
    void CommitAndRelease(TPLCCProTransaction& tx);
    
    // 提交事务并延迟释放锁（跨链交易）
    void CommitAndDelayRelease(TPLCCProTransaction& tx);
    
    // 放弃事务并释放锁
    void AbortAndRelease(TPLCCProTransaction& tx);
};

/// @brief 优化版两阶段锁跨链协议
class TwoPhaseLockingCrossChainPro: public Protocol {
private:
    // 交易记录结构
    struct TransactionRecord {
        size_t tx_id;
        steady_clock::time_point start_time;
        steady_clock::time_point finish_time;
        size_t latency;  // 微秒
        TxType type;
        size_t block_id;
        bool is_aborted;
    };
    
    // 前100笔交易的记录
    std::vector<TransactionRecord> tx_records;
    std::mutex tx_records_mutex;
    
    // 区块信息记录结构
    struct BlockInfo {
        size_t block_id;
        size_t tx_count{0};  // 区块内交易数量
        size_t aborted_tx_count{0};  // 中止的交易数量
        steady_clock::time_point block_start_time;
        steady_clock::time_point block_end_time;
    };

    // 区块记录列表
    std::vector<BlockInfo> block_records;
    mutable std::mutex block_records_mutex;
    std::atomic<size_t> current_block_id{1};
    std::atomic<size_t> txs_in_current_block{0};
    std::atomic<size_t> aborted_txs_in_current_block{0};  // 添加中止交易计数器
    size_t txs_per_block{200};  // 每个区块的交易数量
    double block_interval_seconds{2.0};  // 区块间隔时间

    Workload& workload;
    size_t num_threads;
    Statistics& statistics;
    TPLCCProTable table;
    std::atomic<bool> stop_flag{false};
    std::atomic<size_t> last_executed{1};
    std::vector<std::thread> executors;
    std::thread block_manager;  // 添加区块管理线程
    
    // 区块暂停控制
    std::atomic<bool> block_paused{false};
    std::mutex block_mutex;
    std::condition_variable block_cv;
    
    // 跨链交易比例 (0.0-1.0)
    double cross_chain_ratio{0.1};
    
    // 随机数生成器
    std::mt19937 rng{std::random_device{}()};
    std::uniform_real_distribution<double> dist{0.0, 1.0};

    // 添加统计前100笔交易的相关变量
    std::atomic<bool> first_tx_started{false};
    steady_clock::time_point first_tx_time;
    steady_clock::time_point latest_finish_time;
    steady_clock::time_point latest_cross_chain_finish_time;
    std::atomic<size_t> completed_tx_count{0};
    std::ofstream latency_log;
    std::mutex latency_log_mutex;

public:
    TwoPhaseLockingCrossChainPro(
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
    
    // 区块控制任务
    void BlockControllerTask();
    
    // 区块信息查询接口
    std::string GetBlockInfo(size_t block_id);
    std::string GetAllBlocksInfo();
    std::string GetExecutionStatistics();
    
    // 区块完成处理
    void OnBlockComplete(size_t block_id);
};

#undef K
} // namespace spectrum 
