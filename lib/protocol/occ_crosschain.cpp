#include "evmc/evmc.hpp"
#include <spectrum/protocol/occ_crosschain.hpp>
#include <spectrum/common/lock-util.hpp>
#include <spectrum/common/hex.hpp>
#include <spectrum/common/thread-util.hpp>
#include <functional>
#include <thread>
#include <chrono>
#include <glog/logging.h>
#include <ranges>
#include <fmt/core.h>

namespace spectrum {

using namespace std::chrono;

// OCCTransaction实现
OCCTransaction::OCCTransaction(Transaction&& inner, size_t id, size_t block_id, OCCTxType type) :
    Transaction{std::move(inner)},
    id{id},
    block_id{block_id},
    type{type},
    start_time{std::chrono::steady_clock::now()},
    is_cross_chain_ready{false},
    delay_completed{false}
{
    LOG(INFO) << fmt::format("创建交易 ID={} 区块={} 类型={}", 
                          id, block_id, type == OCCTxType::REGULAR ? "普通" : "跨链");
}

void OCCTransaction::SetReadyTime() {
    ready_time = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    LOG(INFO) << fmt::format("跨链交易 ID={} 设置延迟完成时间，将在2秒后完成", id);
}

bool OCCTransaction::IsReadyForFinalize() const {
    // 如果不是跨链交易，或者已经完成延迟等待，则直接返回true
    if (type != OCCTxType::CROSS_CHAIN || delay_completed) {
        return true;
    }
    
    // 检查是否已经到达完成时间
    bool ready = std::chrono::steady_clock::now() >= ready_time;
    if (ready) {
        LOG(INFO) << fmt::format("跨链交易 ID={} 已到达延迟完成时间，可以finalize", id);
    }
    return ready;
}

// OCCVersionManager实现
OCCVersionManager::OCCVersionManager(size_t partitions) : version_table(partitions) {}

std::optional<OCCVersionItem> OCCVersionManager::GetVersion(const K& key) {
    std::optional<OCCVersionItem> result;
    version_table.Put(key, [&](VersionList& list) {
        std::shared_lock lock(list.mutex);
        if (!list.versions.empty()) {
            const auto& latest = list.versions.back();
            result = OCCVersionItem{
                .value = latest.value,
                .version = latest.version,
                .writer_tx_id = latest.writer_tx_id
            };
        }
    });
    return result;
}

bool OCCVersionManager::ValidateReadSet(OCCTransaction* tx) {
    for (const auto& read_item : tx->read_set) {
        bool is_valid = false;
        version_table.Put(read_item.key, [&](VersionList& list) {
            std::shared_lock lock(list.mutex);
            // 检查版本是否仍然有效
            for (const auto& ver : list.versions) {
                if (ver.version == read_item.version) {
                    is_valid = true;
                    break;
                }
            }
        });
        if (!is_valid) {
            return false;
        }
    }
    return true;
}

void OCCVersionManager::CommitWriteSet(OCCTransaction* tx) {
    for (const auto& write_item : tx->write_set) {
        version_table.Put(write_item.key, [&](VersionList& list) {
            std::unique_lock lock(list.mutex);
            list.versions.push_back(OCCVersionItem{
                .value = write_item.value,
                .version = tx->id,
                .writer_tx_id = tx->id
            });
        });
    }
}

// OCCCrossChain实现
OCCCrossChain::OCCCrossChain(
    Workload& workload, Statistics& statistics,
    size_t num_executors, size_t table_partitions,
    EVMType evm_type, size_t txs_per_block,
    double block_interval_seconds, double cross_chain_ratio
) :
    workload{workload},
    statistics{statistics},
    num_executors{num_executors},
    txs_per_block{txs_per_block},
    block_interval_seconds{block_interval_seconds},
    cross_chain_ratio{cross_chain_ratio},
    version_manager{table_partitions}
{
    LOG(INFO) << fmt::format("OCCCrossChain(num_executors={}, table_partitions={}, evm_type={})", 
                           num_executors, table_partitions, evm_type);
    workload.SetEVMType(evm_type);
}

void OCCCrossChain::Start() {
    LOG(INFO) << "OCC跨链协议启动...";
    stop_flag.store(false);
    
    // 启动区块管理线程
    block_manager = std::thread([this] { BlockControllerTask(); });
    
    // 启动执行器线程
    for (size_t i = 0; i < num_executors; ++i) {
        executors.push_back(std::thread([this] {
            std::make_unique<OCCExecutor>(*this)->Run();
        }));
        PinRoundRobin(executors[i], i);
    }
}

void OCCCrossChain::Stop() {
    LOG(INFO) << "OCC跨链协议停止...";
    stop_flag.store(true);
    
    // 唤醒所有等待的线程
    block_cv.notify_all();
    
    // 等待区块管理线程完成
    if (block_manager.joinable()) {
        block_manager.join();
    }
    
    // 等待所有执行器线程完成
    for (auto& executor : executors) {
        if (executor.joinable()) {
            executor.join();
        }
    }
    executors.clear();
    
    LOG(INFO) << "OCC跨链协议已完全停止";
}

void OCCCrossChain::BlockControllerTask() {
    while (!stop_flag.load()) {
        auto block_start = steady_clock::now();
        auto block_id = current_block_id.load();
        
        LOG(INFO) << fmt::format("开始区块 {} 的执行", block_id);
        
        // 重置区块内交易计数
        txs_in_current_block.store(0);
        block_paused.store(false);
        
        // 等待区块间隔或达到交易数量限制
        while (!stop_flag.load()) {
            if (txs_in_current_block.load() >= txs_per_block) {
                LOG(INFO) << fmt::format("区块 {} 已达到交易数量上限", block_id);
                break;
            }
            
            auto now = steady_clock::now();
            if (duration_cast<seconds>(now - block_start).count() >= block_interval_seconds) {
                LOG(INFO) << fmt::format("区块 {} 已达到时间限制", block_id);
                break;
            }
            
            // 处理已经准备好的跨链交易
            ProcessPendingTransactions();
            
            std::this_thread::sleep_for(milliseconds(10));
        }
        
        if (stop_flag.load()) break;
        
        // 暂停新交易生成
        block_paused.store(true);
        
        // 等待所有活跃交易完成
        {
            std::unique_lock<std::mutex> lock(block_mutex);
            block_cv.wait(lock, [this, block_id] {
                std::lock_guard<std::mutex> tx_lock(transactions_mutex);
                return std::none_of(active_transactions.begin(), active_transactions.end(),
                    [block_id](const auto& tx) { return tx->block_id == block_id; });
            });
        }
        
        // 处理区块完成
        OnBlockComplete(block_id);
        
        // 准备下一个区块
        current_block_id.fetch_add(1);
    }
}

void OCCCrossChain::ProcessPendingTransactions() {
    std::lock_guard<std::mutex> lock(pending_mutex);
    
    // 遍历待处理的跨链交易
    for (auto it = pending_cross_chain_txs.begin(); it != pending_cross_chain_txs.end();) {
        auto tx = *it;
        
        // 如果交易已经准备好
        if (tx->IsReadyForFinalize()) {
            // 将交易移动到活跃交易列表
            {
                std::lock_guard<std::mutex> tx_lock(transactions_mutex);
                active_transactions.push_back(tx);
            }
            
            // 从待处理队列中移除
            it = pending_cross_chain_txs.erase(it);
            
            LOG(INFO) << fmt::format("跨链交易 {} 已准备就绪，移至活跃队列", tx->id);
        } else {
            ++it;
        }
    }
}

void OCCCrossChain::OnBlockComplete(size_t block_id) {
    LOG(INFO) << fmt::format("区块 {} 所有交易执行完成，记录区块信息...", block_id);
    
    std::lock_guard<std::mutex> lock(block_records_mutex);
    std::lock_guard<std::mutex> tx_lock(transactions_mutex);
    
    // 创建区块记录
    BlockInfo record{
        .block_id = block_id,
        .block_start_time = steady_clock::now() - microseconds(static_cast<long long>(block_interval_seconds * 1000000)),
        .block_end_time = steady_clock::now()
    };
    
    // 统计区块内交易信息
    for (const auto& tx : active_transactions) {
        if (tx->block_id != block_id) continue;
        
        record.tx_count++;
        if (tx->type == OCCTxType::CROSS_CHAIN) {
            record.cross_chain_tx_count++;
        } else {
            record.regular_tx_count++;
        }
        
        if (tx->is_aborted) {
            record.aborted_tx_count++;
        }
        
        // 计算交易延迟
        auto latency = duration_cast<microseconds>(
            tx->ready_time.time_since_epoch().count() > 0 ? tx->ready_time - tx->start_time : 
            steady_clock::now() - tx->start_time
        ).count();
        record.total_latency += latency;
    }
    
    // 计算平均延迟
    if (record.tx_count > 0) {
        record.avg_latency = record.total_latency / record.tx_count;
    }
    
    // 添加到区块记录列表
    block_records.push_back(std::move(record));
    
    // 清理已完成的交易
    active_transactions.erase(
        std::remove_if(active_transactions.begin(), active_transactions.end(),
            [block_id](const auto& tx) { return tx->block_id == block_id; }),
        active_transactions.end()
    );
    
    LOG(INFO) << "区块 " << block_id << " 的信息已记录";
}

std::string OCCCrossChain::GetBlockInfo(size_t block_id) {
    std::lock_guard<std::mutex> lock(block_records_mutex);
    
    for (const auto& record : block_records) {
        if (record.block_id == block_id) {
            std::stringstream ss;
            ss << fmt::format("区块 {} 信息:\n", block_id);
            ss << fmt::format("执行时间: {:.2f} 秒\n", 
                           duration_cast<duration<double>>(record.block_end_time - record.block_start_time).count());
            ss << fmt::format("总交易数: {}\n", record.tx_count);
            ss << fmt::format("跨链交易数: {} ({:.1f}%)\n", 
                           record.cross_chain_tx_count,
                           record.tx_count > 0 ? (record.cross_chain_tx_count * 100.0 / record.tx_count) : 0.0);
            ss << fmt::format("普通交易数: {} ({:.1f}%)\n",
                           record.regular_tx_count,
                           record.tx_count > 0 ? (record.regular_tx_count * 100.0 / record.tx_count) : 0.0);
            ss << fmt::format("中止交易数: {} ({:.1f}%)\n",
                           record.aborted_tx_count,
                           record.tx_count > 0 ? (record.aborted_tx_count * 100.0 / record.tx_count) : 0.0);
            ss << fmt::format("平均交易延迟: {:.2f} 微秒\n", record.avg_latency);
            return ss.str();
        }
    }
    return fmt::format("未找到区块 {} 的记录", block_id);
}

std::string OCCCrossChain::GetAllBlocksInfo() {
    std::lock_guard<std::mutex> lock(block_records_mutex);
    
    std::stringstream ss;
    ss << "所有区块信息:\n";
    ss << "========================\n";
    
    for (const auto& record : block_records) {
        ss << GetBlockInfo(record.block_id);
        ss << "------------------------\n";
    }
    
    return ss.str();
}

// OCCExecutor实现
OCCExecutor::OCCExecutor(OCCCrossChain& protocol) :
    protocol(protocol),
    workload(protocol.workload),
    version_manager(protocol.version_manager),
    statistics(protocol.statistics),
    last_executed(protocol.last_executed),
    last_finalized(protocol.last_finalized),
    stop_flag(protocol.stop_flag),
    current_block_id(protocol.current_block_id),
    block_paused(protocol.block_paused),
    block_mutex(protocol.block_mutex),
    block_cv(protocol.block_cv),
    txs_in_current_block(protocol.txs_in_current_block),
    cross_chain_ratio(protocol.cross_chain_ratio)
{
}

void OCCExecutor::Generate() {
    if (tx != nullptr) return;
    
    // 检查是否暂停生成
    if (block_paused.load()) {
        // 等待区块生成恢复
        {
            std::unique_lock<std::mutex> lock(block_mutex);
            block_cv.wait(lock, [this]{ 
                return !block_paused.load() || stop_flag.load(); 
            });
        }
        
        // 如果收到停止信号，直接返回
        if (stop_flag.load()) {
            return;
        }
    }
    
    // 生成新的交易ID和区块ID
    auto tx_id = last_executed.fetch_add(1);
    auto block_id = current_block_id.load();
    
    // 随机决定交易类型
    auto type = (dist(rng) < cross_chain_ratio) ? OCCTxType::CROSS_CHAIN : OCCTxType::REGULAR;
    
    // 创建新的交易
    tx = std::make_unique<OCCTransaction>(workload.Next(), tx_id, block_id, type);
    
    // 增加区块内交易计数
    txs_in_current_block.fetch_add(1);
    
    // 记录新事务
    statistics.JournalTransaction();
    
    // 安装存储处理函数
    tx->InstallSetStorageHandler([this](
        const evmc::address &addr, 
        const evmc::bytes32 &key, 
        const evmc::bytes32 &value
    ) {
        auto _key = std::make_tuple(addr, key);
        tx->write_set.push_back({
            .key = _key,
            .value = value,
            .is_committed = false
        });
        LOG(INFO) << fmt::format("交易 {} 写入键 {}", tx->id, KeyHasher()(_key) % 1000);
        return evmc_storage_status::EVMC_STORAGE_MODIFIED;
    });
    
    tx->InstallGetStorageHandler([this](
        const evmc::address &addr, 
        const evmc::bytes32 &key
    ) {
        auto _key = std::make_tuple(addr, key);
        
        // 先检查本地写集
        for (const auto& write_item : tx->write_set) {
            if (write_item.key == _key) {
                LOG(INFO) << fmt::format("交易 {} 在本地写集中找到键 {}", 
                                     tx->id, KeyHasher()(_key) % 1000);
                return write_item.value;
            }
        }
        
        // 再检查本地读集
        for (const auto& read_item : tx->read_set) {
            if (read_item.key == _key) {
                LOG(INFO) << fmt::format("交易 {} 在本地读集中找到键 {}", 
                                     tx->id, KeyHasher()(_key) % 1000);
                return read_item.value;
            }
        }
        
        // 从版本管理器中读取
        auto version_opt = version_manager.GetVersion(_key);
        evmc::bytes32 value = version_opt ? version_opt->value : evmc::bytes32{0};
        size_t version = version_opt ? version_opt->version : 0;
        
        // 记录到读集
        tx->read_set.push_back({
            .key = _key,
            .value = value,
            .version = version
        });
        
        LOG(INFO) << fmt::format("交易 {} 读取键 {} 版本 {}", 
                              tx->id, KeyHasher()(_key) % 1000, version);
        return value;
    });
    
    // 执行交易
    LOG(INFO) << fmt::format("执行交易 {}", tx->id);
    tx->Execute();
    statistics.JournalExecute();
    statistics.JournalOperations(tx->CountOperations());
}

bool OCCExecutor::Validate() {
    if (!tx) return false;
    
    // 验证读集
    bool is_valid = version_manager.ValidateReadSet(tx.get());
    
    if (is_valid) {
        LOG(INFO) << fmt::format("交易 {} 验证成功", tx->id);
        tx->is_validated = true;
    } else {
        LOG(INFO) << fmt::format("交易 {} 验证失败", tx->id);
        tx->is_aborted = true;
    }
    
    return is_valid;
}

void OCCExecutor::Commit() {
    if (!tx || !tx->is_validated) return;
    
    // 提交写集
    version_manager.CommitWriteSet(tx.get());
    
    // 记录延迟
    auto latency = duration_cast<microseconds>(steady_clock::now() - tx->start_time).count();
    statistics.JournalCommit(latency);
    statistics.JournalMemory(tx->mm_count);
    
    LOG(INFO) << fmt::format("交易 {} 提交完成", tx->id);
    
    // 增加已完成交易计数
    last_finalized.fetch_add(1);
}

void OCCExecutor::Abort() {
    if (!tx) return;
    
    // 记录中止
    statistics.JournalAbort();
    
    LOG(INFO) << fmt::format("交易 {} 中止", tx->id);
    
    // 清空交易
    tx = nullptr;
}

void OCCExecutor::Run() {
    while (!stop_flag.load()) {
        // 生成新交易
        if (!tx) {
            Generate();
            if (!tx) continue;
            
            // 如果是跨链交易，设置延迟时间
            if (tx->type == OCCTxType::CROSS_CHAIN && !tx->is_cross_chain_ready) {
                tx->SetReadyTime();
                tx->is_cross_chain_ready = true;
                continue;  // 先处理其他交易
            }
        }
        
        // 对于跨链交易，检查是否已经过了延迟时间
        if (tx->type == OCCTxType::CROSS_CHAIN && !tx->IsReadyForFinalize()) {
            // 暂时跳过这个交易，继续处理其他交易
            continue;
        }
        
        // 验证和提交逻辑
        bool success = Validate();
        
        if (success) {
            // 提交交易
            Commit();
            LOG(INFO) << fmt::format("交易 {} 成功提交", tx->id);
        } else {
            // 交易中止，记录并继续下一个交易
            tx->is_aborted = true;
            statistics.JournalAbort();  // 记录中止
            LOG(INFO) << fmt::format("交易 {} 中止", tx->id);
        }
        
        // 清理当前交易
        tx.reset();
    }
}

} // namespace spectrum 