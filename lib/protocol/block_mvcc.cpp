#include "evmc/evmc.hpp"
#include <spectrum/protocol/block_mvcc.hpp>
#include <spectrum/common/lock-util.hpp>
#include <spectrum/common/hex.hpp>
#include <spectrum/common/thread-util.hpp>
#include <functional>
#include <thread>
#include <chrono>
#include <glog/logging.h>
#include <ranges>
#include <fmt/core.h>

/*
 * BlockMVCC: A variant of Spectrum with block-based transaction generation
 * Transactions are generated in batches (blocks), with pauses between blocks
 */

namespace spectrum {

using namespace std::chrono;

#define K std::tuple<evmc::address, evmc::bytes32>
#define V BlockMVCCVersionList
#define T BlockMVCCTransaction

/// @brief wrap a base transaction into a block mvcc transaction
/// @param inner the base transaction
/// @param id transaction id
BlockMVCCTransaction::BlockMVCCTransaction(Transaction&& inner, size_t id):
    Transaction{std::move(inner)},
    id{id},
    start_time{std::chrono::steady_clock::now()}
{}

/// @brief determine transaction has to rerun
/// @return if transaction has to rerun
bool BlockMVCCTransaction::HasWAR() {
    auto guard = Guard{rerun_keys_mu};
    return rerun_keys.size() != 0;
}

/// @brief call the transaction to rerun providing the key that caused it
/// @param key the key that caused rerun
void BlockMVCCTransaction::SetWAR(const K& key, size_t cause_id) {
    auto guard = Guard{rerun_keys_mu};
    rerun_keys.push_back(key);
    should_wait = std::max(should_wait, cause_id);
}

/// @brief the multi-version table for block mvcc
/// @param partitions the number of partitions
BlockMVCCTable::BlockMVCCTable(size_t partitions):
    Table<K, V, KeyHasher>{partitions}
{}

/// @brief get a value
/// @param tx the transaction that reads the value
/// @param k the key of the read entry
/// @param v (mutated to be) the value of read entry
/// @param version (mutated to be) the version of read entry
void BlockMVCCTable::Get(T* tx, const K& k, evmc::bytes32& v, size_t& version) {
    Table::Put(k, [&](V& _v) {
        auto rit = _v.entries.rbegin();
        auto end = _v.entries.rend();
        while (rit != end) {
            if (rit->version > tx->id) {
                ++rit; continue;
            }
            v = rit->value;
            version = rit->version;
            rit->readers.insert(tx);
            LOG(INFO) << tx->id << "(" << tx << ")" << " read " << KeyHasher()(k) % 1000 << " version " << rit->version;
            return;
        }
        version = 0;
        LOG(INFO) << tx->id << "(" << tx << ")" << " read " << KeyHasher()(k) % 1000 << " version 0";
        _v.readers_default.insert(tx);
        v = evmc::bytes32{0};
    });
}

/// @brief put a value
/// @param tx the transaction that writes the value
/// @param k the key of the written entry
/// @param v the value to write
void BlockMVCCTable::Put(T* tx, const K& k, const evmc::bytes32& v) {
    CHECK(tx->id > 0) << "we reserve version(0) for default value";
    LOG(INFO) << tx->id << "(" << tx << ")" << " write " << KeyHasher()(k) % 1000;
    Table::Put(k, [&](V& _v) {
        auto rit = _v.entries.rbegin();
        auto end = _v.entries.rend();
        // search from insertion position
        while (rit != end) {
            if (rit->version > tx->id) {
                ++rit; continue;
            }
            // abort transactions that read outdated keys
            for (auto _tx: rit->readers) {
                LOG(INFO) << KeyHasher()(k) % 1000 << " has read dependency " << "(" << _tx << ")";
                if (_tx->id > tx->id) {
                    LOG(INFO) << tx->id << " abort " << _tx->id;
                    _tx->SetWAR(k, tx->id);
                }
            }
            break;
        }
        for (auto _tx: _v.readers_default) {
            LOG(INFO) << KeyHasher()(k) % 1000 << " has read dependency " << "(" << _tx << ")";
            if (_tx->id > tx->id) {
                LOG(INFO) << tx->id << " abort " << _tx->id;
                _tx->SetWAR(k, tx->id);
            }
        }
        // handle duplicated write on the same key
        if (rit != end && rit->version == tx->id) {
            rit->value = v;
            return;
        }
        // insert an entry
        _v.entries.insert(rit.base(), BlockMVCCEntry {
            .value   = v,
            .version = tx->id,
            .readers = std::unordered_set<T*>()
        });
    });
}

/// @brief remove a read dependency from this entry
/// @param tx the transaction that previously read this entry
/// @param k the key of read entry
void BlockMVCCTable::RegretGet(T* tx, const K& k, size_t version) {
    LOG(INFO) << "remove read record " << tx->id << "(" << tx << ")" << " from " << KeyHasher()(k) % 1000;
    Table::Put(k, [&](V& _v) {
        auto vit = _v.entries.begin();
        auto end = _v.entries.end();
        while (vit != end) {
            if (vit->version != version) {
                ++vit; continue;
            }
            vit->readers.erase(tx);
            break;
        }
        if (version == 0) {
            _v.readers_default.erase(tx);
        }
    });
}

/// @brief undo a put operation and abort all dependent transactions
/// @param tx the transaction that previously put into this entry
/// @param k the key of this put entry
void BlockMVCCTable::RegretPut(T* tx, const K& k) {
    LOG(INFO) << "remove write record " << tx->id << "(" << tx << ")" << " from " << KeyHasher()(k) % 1000;
    Table::Put(k, [&](V& _v) {
        auto vit = _v.entries.begin();
        auto end = _v.entries.end();
        while (vit != end) {
            if (vit->version != tx->id) {
                ++vit; continue;
            }
            // abort transactions that read from current transaction
            for (auto _tx: vit->readers) {
                LOG(INFO) << KeyHasher()(k) % 1000 << " has read dependency " << "(" << _tx << ")";
                LOG(INFO) << tx->id << " abort " << _tx->id;
                _tx->SetWAR(k, tx->id);
            }
            break;
        }
        if (vit != end) { _v.entries.erase(vit); }
    });
}

/// @brief remove a read dependency from this entry
/// @param tx the transaction that previously read this entry
/// @param k the key of read entry
/// @param version the version of read entry, which indicates the transaction that writes this value
void BlockMVCCTable::ClearGet(T* tx, const K& k, size_t version) {
    LOG(INFO) << "remove read record " << tx->id << "(" << tx << ")" << " from " << KeyHasher()(k) % 1000;
    Table::Put(k, [&](V& _v) {
        auto vit = _v.entries.begin();
        auto end = _v.entries.end();
        while (vit != end) {
            if (vit->version != version) {
                ++vit; continue;
            }
            LOG(INFO) << "remove " << tx->id << "(" << tx << ")" << " from version " << vit->version; 
            vit->readers.erase(tx);
            break;
        }
        if (version == 0) {
            _v.readers_default.erase(tx);
        }
    });
}

/// @brief remove versions preceeding current transaction
/// @param tx the transaction the previously wrote this entry
/// @param k the key of written entry
void BlockMVCCTable::ClearPut(T* tx, const K& k) {
    LOG(INFO) << "remove write record before " << tx->id << "(" << tx << ")" << " from " << KeyHasher()(k) % 1000;
    Table::Put(k, [&](V& _v) {
        while (_v.entries.size() && _v.entries.front().version < tx->id) {
            _v.entries.pop_front();
        }
    });
}

/// @brief block mvcc initialization parameters
BlockMVCC::BlockMVCC(Workload& workload, Statistics& statistics, 
                   size_t num_executors, size_t table_partitions, 
                   EVMType evm_type,
                   size_t block_size,
                   milliseconds block_interval):
    workload{workload},
    statistics{statistics},
    num_executors{num_executors},
    table{table_partitions},
    block_size{block_size},
    block_interval{block_interval},
    stop_latch{static_cast<ptrdiff_t>(num_executors), []{}}
{
    LOG(INFO) << fmt::format("BlockMVCC(num_executors={}, table_partitions={}, evm_type={}, block_size={}, block_interval={}ms)", 
                           num_executors, table_partitions, evm_type, block_size, block_interval.count());
    workload.SetEVMType(evm_type);
}

/// @brief block controller task that controls transaction generation pacing
void BlockMVCC::BlockControllerTask() {
    LOG(INFO) << "区块控制线程启动，每个区块大小: " << block_size << " 交易，区块间隔: " << block_interval.count() << "毫秒";
    
    while (!stop_flag.load()) {
        // 检查是否需要暂停事务生成
        if (tx_count_in_block.load() >= block_size) {
            LOG(INFO) << "已生成 " << block_size << " 笔交易，暂停生成 " << block_interval.count() << " 毫秒";
            
            // 标记暂停状态
            block_generation_paused.store(true);
            
            // 等待区块间隔时间
            std::this_thread::sleep_for(block_interval);
            
            // 重置区块内交易计数
            tx_count_in_block.store(0);
            
            // 恢复生成
            block_generation_paused.store(false);
            
            // 通知所有等待的执行器
            block_cv.notify_all();
            
            LOG(INFO) << "恢复交易生成";
        }
        
        // 控制线程休眠，降低CPU使用率
        std::this_thread::sleep_for(milliseconds(10));
    }
    
    LOG(INFO) << "区块控制线程停止";
}

/// @brief start block mvcc protocol
void BlockMVCC::Start() {
    LOG(INFO) << "BlockMVCC协议启动...";
    
    // 重置状态
    stop_flag.store(false);
    block_generation_paused.store(false);
    tx_count_in_block.store(0);
    
    // 启动区块控制线程
    block_controller = std::thread(&BlockMVCC::BlockControllerTask, this);
    
    // 启动执行器线程
    for (size_t i = 0; i < num_executors; ++i) {
        executors.push_back(std::thread([this]{
            std::make_unique<BlockMVCCExecutor>(*this)->Run();
        }));
        PinRoundRobin(executors[i], i);
    }
}

/// @brief stop block mvcc protocol
void BlockMVCC::Stop() {
    LOG(INFO) << "BlockMVCC协议停止...";
    
    // 设置停止标志
    stop_flag.store(true);
    
    // 通知所有等待的执行器
    block_cv.notify_all();
    
    // 等待区块控制线程结束
    if (block_controller.joinable()) {
        block_controller.join();
    }
    
    // 等待所有执行器线程结束
    for (auto& x: executors) { 
        x.join(); 
    }
}

/// @brief block mvcc executor
BlockMVCCExecutor::BlockMVCCExecutor(BlockMVCC& block_mvcc):
    workload{block_mvcc.workload},
    table{block_mvcc.table},
    statistics{block_mvcc.statistics},
    last_executed{block_mvcc.last_executed},
    last_finalized{block_mvcc.last_finalized},
    stop_flag{block_mvcc.stop_flag},
    tx_count_in_block{block_mvcc.tx_count_in_block},
    block_generation_paused{block_mvcc.block_generation_paused},
    block_mutex{block_mvcc.block_mutex},
    block_cv{block_mvcc.block_cv},
    stop_latch{block_mvcc.stop_latch}
{}

/// @brief generate a transaction and execute it
void BlockMVCCExecutor::Generate() {
    if (tx != nullptr) return;
    
    // 检查是否暂停生成
    if (block_generation_paused.load()) {
        // 等待区块生成恢复
        {
            std::unique_lock<std::mutex> lock(block_mutex);
            block_cv.wait(lock, [this]{ 
                return !block_generation_paused.load() || stop_flag.load(); 
            });
        }
        
        // 如果收到停止信号，直接返回
        if (stop_flag.load()) {
            return;
        }
    }
    
    // 生成新的交易
    tx = std::make_unique<T>(workload.Next(), last_executed.fetch_add(1));
    tx->start_time = steady_clock::now();
    tx->berun_flag.store(true);
    
    // 增加区块内交易计数
    tx_count_in_block.fetch_add(1);
    
    LOG(INFO) << "生成交易 #" << tx->id << " (区块内第 " << tx_count_in_block.load() << " 笔)";
    
    // 记录新事务
    statistics.JournalTransaction();
    
    tx->InstallSetStorageHandler([this](
        const evmc::address &addr, 
        const evmc::bytes32 &key, 
        const evmc::bytes32 &value
    ) {
        auto _key = std::make_tuple(addr, key);
        tx->tuples_put.push_back({
            .key = _key, 
            .value = value, 
            .is_committed = false
        });
        if (tx->HasWAR()) {
            LOG(INFO) << "block_mvcc tx " << tx->id << " break";
            tx->Break();
        }
        LOG(INFO) << "tx " << tx->id <<
            " tuples put: " << tx->tuples_put.size() <<
            " tuples get: " << tx->tuples_get.size();
        return evmc_storage_status::EVMC_STORAGE_MODIFIED;
    });
    
    tx->InstallGetStorageHandler([this](
        const evmc::address &addr, 
        const evmc::bytes32 &key
    ) {
        auto _key = std::make_tuple(addr, key);
        auto value = evmc::bytes32{0};
        auto version = size_t{0};
        
        // 先检查本地写入缓存
        for (auto& tup: tx->tuples_put | std::views::reverse) {
            if (tup.key != _key) { continue; }
            LOG(INFO) << "block_mvcc tx " << tx->id << " has key " << KeyHasher()(_key) % 1000 << " in tuples_put";
            return tup.value;
        }
        
        // 再检查本地读取缓存
        for (auto& tup: tx->tuples_get) {
            if (tup.key != _key) { continue; }
            LOG(INFO) << "block_mvcc tx " << tx->id << " has key " << KeyHasher()(_key) % 1000 << " in tuples_get";
            return tup.value;
        }
        
        // 从全局存储中获取
        LOG(INFO) << "tx " << tx->id << " " << 
            " read(" << tx->tuples_get.size() << ")" << 
            " key(" << KeyHasher()(_key) % 1000 << ")";
        table.Get(tx.get(), _key, value, version);
        
        tx->tuples_get.push_back({
            .key = _key, 
            .value = value, 
            .version = version,
            .tuples_put_len = tx->tuples_put.size(),
            .checkpoint_id = tx->MakeCheckpoint()
        });
        
        // 检查是否需要中断执行
        if (tx->HasWAR()) {
            LOG(INFO) << "block_mvcc tx " << tx->id << " break";
            tx->Break();
        }
        
        return value;
    });
    
    // 执行交易
    LOG(INFO) << "执行交易 " << tx->id;
    tx->Execute();
    statistics.JournalExecute();
    statistics.JournalOperations(tx->CountOperations());
    
    // 提交所有结果（如果可能且必要）
    for (auto& entry: tx->tuples_put) {
        if (tx->HasWAR()) { break; }
        if (entry.is_committed) { continue; }
        table.Put(tx.get(), entry.key, entry.value);
        entry.is_committed = true;
    }
}

/// @brief rollback transaction with given rollback signal
void BlockMVCCExecutor::ReExecute() {
    LOG(INFO) << "block_mvcc re-execute " << tx->id;
    
    // 获取当前重新执行的键
    std::vector<K> rerun_keys{};
    {
        auto guard = Guard{tx->rerun_keys_mu}; 
        std::swap(tx->rerun_keys, rerun_keys);
    }
    
    // 记录中止
    statistics.JournalAbort();
    
    auto back_to = ~size_t{0};
    
    // 找到需要回滚的检查点
    for (auto& key: rerun_keys) {
        for (size_t i = 0; i < tx->tuples_get.size(); ++i) {
            if (tx->tuples_get[i].key != key) { continue; }
            back_to = std::min(i, back_to); 
            break;
        }
    }
    
    // 如果不需要回滚，则直接恢复执行
    if (back_to == ~size_t{0}) {
        LOG(INFO) << "tx " << tx->id << " 不需要回滚";
        tx->Execute(); 
        return;
    }
    
    // 需要回滚
    auto& tup = tx->tuples_get[back_to];
    tx->ApplyCheckpoint(tup.checkpoint_id);
    
    // 回滚已提交的写入
    for (size_t i = tup.tuples_put_len; i < tx->tuples_put.size(); ++i) {
        if (tx->tuples_put[i].is_committed) {
            table.RegretPut(tx.get(), tx->tuples_put[i].key);
        }
    }
    
    // 移除读取记录
    for (size_t i = back_to; i < tx->tuples_get.size(); ++i) {
        table.RegretGet(tx.get(), tx->tuples_get[i].key, tx->tuples_get[i].version);
    }
    
    // 调整元组大小
    tx->tuples_put.resize(tup.tuples_put_len);
    tx->tuples_get.resize(back_to);
    
    LOG(INFO) << "tx " << tx->id <<
        " tuples put: " << tx->tuples_put.size() <<
        " tuples get: " << tx->tuples_get.size();
    
    // 执行交易
    tx->Execute();
    statistics.JournalExecute();
    statistics.JournalOperations(tx->CountOperations());
    
    // 提交所有结果（如果可能且必要）
    for (auto& entry: tx->tuples_put) {
        if (tx->HasWAR()) { break; }
        if (entry.is_committed) { continue; }
        table.Put(tx.get(), entry.key, entry.value);
        entry.is_committed = true;
    }
}

/// @brief finalize a block mvcc transaction
void BlockMVCCExecutor::Finalize() {
    LOG(INFO) << "完成交易 " << tx->id;
    
    // 增加已完成交易计数
    last_finalized.fetch_add(1, std::memory_order_seq_cst);
    
    // 清理交易对应的读写记录
    for (auto& entry: tx->tuples_get) {
        table.ClearGet(tx.get(), entry.key, entry.version);
    }
    
    for (auto& entry: tx->tuples_put) {
        table.ClearPut(tx.get(), entry.key);
    }
    
    // 记录交易延迟和内存使用
    auto latency = duration_cast<microseconds>(steady_clock::now() - tx->start_time).count();
    
    // 根据是否有WAR冲突决定记录提交还是中止
    if (!tx->HasWAR()) {
        statistics.JournalCommit(latency);
        statistics.JournalMemory(tx->mm_count);
    } else {
        statistics.JournalAbort();
    }
    
    // 清空交易指针，准备处理下一个交易
    tx = nullptr;
}

/// @brief start an executor
void BlockMVCCExecutor::Run() {
    while (!stop_flag.load()) {
        // 首先生成一个交易（如果当前块没有暂停生成）
        if (tx == nullptr) {
            Generate();
            
            // 检查是否收到停止信号
            if (stop_flag.load()) {
                break;
            }
        }
        
        if (tx->HasWAR()) {
            // 如果有需要重新执行的键，重新执行以获取正确的结果
            ReExecute();
        }
        else if (last_finalized.load() + 1 == tx->id && !tx->HasWAR()) {
            // 如果上一个交易已经完成，且当前交易不需要重新执行，则可以最终提交
            Finalize();
        }
    }
    
    // 通知所有执行器已停止
    stop_latch.arrive_and_wait();
}

#undef T
#undef V
#undef K

} // namespace spectrum 