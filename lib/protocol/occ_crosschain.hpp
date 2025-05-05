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
#include <deque>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <optional>
#include <mutex>
#include <condition_variable>
#include <glog/logging.h>
#include <memory>
#include <queue>

namespace spectrum {

using Statistics = spectrum::Statistics;
using namespace std::chrono;

// 定义键类型
using K = std::tuple<evmc::address, evmc::bytes32>;

// 交易类型
enum class OCCTxType {
    REGULAR,      // 普通交易
    CROSS_CHAIN   // 跨链交易
};

// 读写条目
struct OCCReadItem {
    K key;
    evmc::bytes32 value;
    size_t version;
};

struct OCCWriteItem {
    K key;
    evmc::bytes32 value;
    bool is_committed;
};

// OCC跨链事务
class OCCTransaction : public Transaction {
public:
    OCCTransaction(Transaction&& inner, size_t id, size_t block_id, OCCTxType type);
    
    // 跨链交易相关
    void SetReadyTime();
    bool IsReadyForFinalize() const;
    void SetDelayCompleted() { delay_completed = true; }
    OCCTxType GetType() const { return type; }
    
    // 事务信息
    size_t id;
    size_t block_id;
    OCCTxType type;
    
    // 跨链交易相关状态
    std::atomic<bool> is_cross_chain_ready{false};
    bool delay_completed{false};
    steady_clock::time_point ready_time;
    steady_clock::time_point start_time;
    
    // 读写集
    std::vector<OCCReadItem> read_set;
    std::vector<OCCWriteItem> write_set;
    
    // 验证阶段标记
    std::atomic<bool> is_validated{false};
    std::atomic<bool> is_aborted{false};
};

// 版本条目
struct OCCVersionItem {
    evmc::bytes32 value;
    size_t version;
    size_t writer_tx_id;
};

// 版本管理器
class OCCVersionManager {
public:
    OCCVersionManager(size_t partitions);
    
    // 版本操作
    std::optional<OCCVersionItem> GetVersion(const K& key);
    bool ValidateReadSet(OCCTransaction* tx);
    void CommitWriteSet(OCCTransaction* tx);
    
private:
    struct VersionList {
        std::shared_mutex mutex;
        std::list<OCCVersionItem> versions;
    };
    
    Table<K, VersionList, KeyHasher> version_table;
};

// 执行器类前向声明
class OCCExecutor;

// OCC跨链协议实现
class OCCCrossChain : public Protocol {
public:
    OCCCrossChain(
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
    
    // 工作负载和统计
    Workload& workload;
    Statistics& statistics;
    
    // 执行参数
    size_t num_executors;
    size_t txs_per_block;
    double block_interval_seconds;
    double cross_chain_ratio;
    
    // 版本管理
    OCCVersionManager version_manager;
    
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
    
    // 跨链交易管理
    std::deque<std::shared_ptr<OCCTransaction>> pending_cross_chain_txs;
    std::mutex pending_mutex;
    std::vector<std::shared_ptr<OCCTransaction>> active_transactions;
    std::mutex transactions_mutex;
    
    // 执行组件
    std::vector<std::thread> executors;
    std::thread block_manager;
    
    // 私有方法
    void BlockControllerTask();  // 区块控制任务
    void ProcessPendingTransactions();  // 处理待处理的跨链交易
    void OnBlockComplete(size_t block_id);  // 区块完成处理
    
    friend class OCCExecutor;
};

// 执行器类
class OCCExecutor {
public:
    static constexpr size_t MAX_RETRY_COUNT = 5;  // 最大重试次数
    
    OCCExecutor(OCCCrossChain& protocol);
    
    void Run();
    
private:
    // 主要操作
    void Generate();
    bool Validate();
    void Commit();
    void Abort();
    
    // 协议引用
    OCCCrossChain& protocol;
    Workload& workload;
    OCCVersionManager& version_manager;
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
    
    // 本地状态
    std::unique_ptr<OCCTransaction> tx{nullptr};
    
    // 随机数生成
    std::random_device rd;
    std::mt19937 rng{rd()};
    std::uniform_real_distribution<double> dist{0.0, 1.0};
};

} // namespace spectrum 