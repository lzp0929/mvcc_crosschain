#include "evmc/evmc.hpp"
#include <spectrum/protocol/crosschain_mvcc.hpp>
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

// 在这里添加与头文件相同的宏定义
#define K std::tuple<evmc::address, evmc::bytes32>
#define V CrosschainMVCCVersionList
#define T CrosschainMVCCTransaction

using namespace std::chrono;

// CrosschainMVCCTransaction实现
CrosschainMVCCTransaction::CrosschainMVCCTransaction(Transaction&& inner, size_t id) :
    Transaction{std::move(inner)},
    id{id},
    start_time{std::chrono::steady_clock::now()}
{
    LOG(INFO) << fmt::format("CrosschainMVCC: 创建交易 ID={}", id);
}

bool CrosschainMVCCTransaction::HasWAR() {
    auto guard = Guard{rerun_keys_mu};
    return rerun_keys.size() != 0;
}

void CrosschainMVCCTransaction::SetWAR(const K& key, size_t cause_id) {
    auto guard = Guard{rerun_keys_mu};
    rerun_keys.push_back(key);
    should_wait = std::max(should_wait, cause_id);
}

void CrosschainMVCCTransaction::RecordReadSet() {
    // 收集所有读操作到最终读集中
    read_set.clear();
    for (const auto& tup : tuples_get) {
        read_set.push_back({
            .key = tup.key,
            .value = tup.value,
            .writer_tx_id = tup.writer_tx_id
        });
    }
    
    // 打印读集信息
    if (!read_set.empty()) {
        LOG(INFO) << fmt::format("交易 ID={} 最终读集记录:", id);
        for (const auto& entry : read_set) {
            LOG(INFO) << "  " << entry.toString();
        }
    } else {
        LOG(INFO) << fmt::format("交易 ID={} 没有读集记录", id);
    }
}

// CrosschainMVCCTable实现
CrosschainMVCCTable::CrosschainMVCCTable(size_t partitions):
    Table<K, V, KeyHasher>{partitions}
{}

void CrosschainMVCCTable::Get(T* tx, const K& k, evmc::bytes32& v, size_t& version, size_t& writer_tx_id) {
    Table::Put(k, [&](V& _v) {
        auto rit = _v.entries.rbegin();
        auto end = _v.entries.rend();
        while (rit != end) {
            if (rit->version > tx->id) {
                ++rit; continue;
            }
            v = rit->value;
            version = rit->version;
            writer_tx_id = rit->writer_tx_id; // 记录写入者ID
            rit->readers.insert(tx);
            LOG(INFO) << tx->id << "(" << tx << ")" << " read " << KeyHasher()(k) % 1000 << " version " << rit->version << " written by " << writer_tx_id;
            return;
        }
        version = 0;
        writer_tx_id = 0; // 默认值的写入者为0
        LOG(INFO) << tx->id << "(" << tx << ")" << " read " << KeyHasher()(k) % 1000 << " version 0 (default)";
        _v.readers_default.insert(tx);
        v = evmc::bytes32{0};
    });
}

void CrosschainMVCCTable::Put(T* tx, const K& k, const evmc::bytes32& v) {
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
        _v.entries.insert(rit.base(), CrosschainMVCCEntry {
            .value = v,
            .version = tx->id,
            .writer_tx_id = tx->id,  // 记录写入者ID
            .readers = std::unordered_set<T*>()
        });
    });
}

void CrosschainMVCCTable::RegretGet(T* tx, const K& k, size_t version) {
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

void CrosschainMVCCTable::RegretPut(T* tx, const K& k) {
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

void CrosschainMVCCTable::ClearGet(T* tx, const K& k, size_t version) {
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

void CrosschainMVCCTable::ClearPut(T* tx, const K& k) {
    LOG(INFO) << "remove write record before " << tx->id << "(" << tx << ")" << " from " << KeyHasher()(k) % 1000;
    Table::Put(k, [&](V& _v) {
        while (_v.entries.size() && _v.entries.front().version < tx->id) {
            _v.entries.pop_front();
        }
    });
}

// CrosschainMVCC实现
CrosschainMVCC::CrosschainMVCC(Workload& workload, Statistics& statistics, 
                           size_t num_executors, size_t table_partitions, 
                           EVMType evm_type) :
    workload{workload},
    statistics{statistics},
    num_executors{num_executors},
    table{table_partitions},
    stop_latch{static_cast<ptrdiff_t>(num_executors), []{}}
{
    LOG(INFO) << fmt::format("CrosschainMVCC(num_executors={}, table_partitions={}, evm_type={})", 
                           num_executors, table_partitions, evm_type);
    workload.SetEVMType(evm_type);
}

void CrosschainMVCC::Start() {
    LOG(INFO) << "CrosschainMVCC协议启动...";
    stop_flag.store(false);
    for (size_t i = 0; i < num_executors; ++i) {
        executors.push_back(std::thread([this]{
            std::make_unique<CrosschainMVCCExecutor>(*this)->Run();
        }));
        PinRoundRobin(executors[i], i);
    }
}

void CrosschainMVCC::Stop() {
    LOG(INFO) << "CrosschainMVCC协议停止...";
    stop_flag.store(true);
    for (auto& x: executors) { x.join(); }
}

// CrosschainMVCCExecutor实现
CrosschainMVCCExecutor::CrosschainMVCCExecutor(CrosschainMVCC& ccmvcc) :
    workload{ccmvcc.workload},
    table{ccmvcc.table},
    statistics{ccmvcc.statistics},
    last_executed{ccmvcc.last_executed},
    last_finalized{ccmvcc.last_finalized},
    stop_flag{ccmvcc.stop_flag},
    stop_latch{ccmvcc.stop_latch}
{
}

void CrosschainMVCCExecutor::Generate() {
    if (tx != nullptr) return;
    
    // 生成新的交易ID
    auto id = last_executed.fetch_add(1);
    
    // 创建新的交易
    tx = std::make_unique<CrosschainMVCCTransaction>(workload.Next(), id);
    tx->start_time = steady_clock::now();
    tx->berun_flag.store(true);
    
    // 记录事务开始
    statistics.JournalTransaction();
    
    // 安装存储处理函数
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
            LOG(INFO) << "crosschain_mvcc tx " << tx->id << " break";
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
        auto writer_tx_id = size_t{0};
        
        // 先检查本地写入缓存
        for (auto& tup: tx->tuples_put | std::views::reverse) {
            if (tup.key != _key) { continue; }
            LOG(INFO) << "crosschain_mvcc tx " << tx->id << " has key " << KeyHasher()(_key) % 1000 << " in tuples_put";
            return tup.value;
        }
        
        // 再检查本地读取缓存
        for (auto& tup: tx->tuples_get) {
            if (tup.key != _key) { continue; }
            LOG(INFO) << "crosschain_mvcc tx " << tx->id << " has key " << KeyHasher()(_key) % 1000 << " in tuples_get";
            return tup.value;
        }
        
        // 从全局存储中获取
        LOG(INFO) << "tx " << tx->id << " " << 
            " read(" << tx->tuples_get.size() << ")" << 
            " key(" << KeyHasher()(_key) % 1000 << ")";
        table.Get(tx.get(), _key, value, version, writer_tx_id);
        tx->tuples_get.push_back({
            .key = _key, 
            .value = value, 
            .version = version,
            .tuples_put_len = tx->tuples_put.size(),
            .checkpoint_id = tx->MakeCheckpoint(),
            .writer_tx_id = writer_tx_id
        });
        
        // 检查是否需要中断执行
        if (tx->HasWAR()) {
            LOG(INFO) << "crosschain_mvcc tx " << tx->id << " break";
            tx->Break();
        }
        return value;
    });
    
    LOG(INFO) << "执行交易 " << tx->id;
    
    // 执行交易
    tx->Execute();
    statistics.JournalExecute();
    statistics.JournalOperations(tx->CountOperations());
    
    // 提交所有结果（如果可能且必要）
    for (auto entry: tx->tuples_put) {
        if (tx->HasWAR()) { break; }
        if (entry.is_committed) { continue; }
        table.Put(tx.get(), entry.key, entry.value);
        entry.is_committed = true;
    }
}

void CrosschainMVCCExecutor::ReExecute() {
    LOG(INFO) << "重新执行交易 " << tx->id;
    
    // 获取当前重新执行的键
    std::vector<K> rerun_keys{};
    {
        auto guard = Guard{tx->rerun_keys_mu}; 
        std::swap(tx->rerun_keys, rerun_keys);
    }
    
    // 记录中止
    statistics.JournalAbort();
    
    auto back_to = ~size_t{0};
    
    // 找到最早的检查点
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
    for (auto entry: tx->tuples_put) {
        if (tx->HasWAR()) { break; }
        if (entry.is_committed) { continue; }
        table.Put(tx.get(), entry.key, entry.value);
        entry.is_committed = true;
    }
}

void CrosschainMVCCExecutor::Finalize() {
    LOG(INFO) << "完成交易 " << tx->id;
    
    // 记录最终读集
    tx->RecordReadSet();
    
    // 增加已完成交易计数
    last_finalized.fetch_add(1, std::memory_order_seq_cst);
    
    // 清理交易对应的读写记录
    for (auto entry: tx->tuples_get) {
        table.ClearGet(tx.get(), entry.key, entry.version);
    }
    
    for (auto entry: tx->tuples_put) {
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
    
    // 交易已完成，清空交易指针
    tx = nullptr;
}

void CrosschainMVCCExecutor::Run() {
    while (!stop_flag.load()) {
        // 首先生成一个交易
        if (tx == nullptr) {
            Generate();
        }
        
        if (tx->HasWAR()) {
            // 如果有需要重新执行的键，重新执行以获取正确的结果
            ReExecute();
        }
        else if (last_finalized.load() + 1 == tx->id && !tx->HasWAR()) {
            // 如果上一个交易已经完成，且当前交易不需要重新执行，可以最终提交
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