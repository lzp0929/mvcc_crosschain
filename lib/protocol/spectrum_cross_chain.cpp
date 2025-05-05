#include "evmc/evmc.hpp"
#include <spectrum/protocol/spectrum_cross_chain.hpp>
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
#define V CrossChainVersionList
#define T CrossChainTransaction

using namespace std::chrono;

// CrossChainTransaction实现
CrossChainTransaction::CrossChainTransaction(Transaction&& inner, size_t id, TransactionType type) :
    Transaction{std::move(inner)},
    id{id},
    type{type},
    start_time{std::chrono::steady_clock::now()},
    delay_completed{false}
{
    LOG(INFO) << fmt::format("CrossChainTransaction: 创建交易 ID={} 类型={}", 
                          id, type == TransactionType::REGULAR ? "普通" : "跨链");
}

void CrossChainTransaction::SetReadyTime() {
    // 设置跨链交易完成时间为当前时间+2秒
    ready_time = steady_clock::now() + seconds(2);
    LOG(INFO) << fmt::format("CrossChainTransaction: 跨链交易 ID={} 设置延迟完成时间，将在2秒后完成", id);
}

bool CrossChainTransaction::IsReadyForFinalize() const {
    // 如果不是跨链交易，或者已经完成延迟等待，则直接返回true
    if (type != TransactionType::CROSS_CHAIN || delay_completed) {
        return true;
    }
    
    // 检查是否已经到达完成时间
    bool ready = steady_clock::now() >= ready_time;
    if (ready) {
        LOG(INFO) << fmt::format("CrossChainTransaction: 跨链交易 ID={} 已到达延迟完成时间，可以finalize", id);
    }
    return ready;
}

bool CrossChainTransaction::HasWAR() {
    auto guard = Guard{rerun_keys_mu};
    return rerun_keys.size() != 0;
}

void CrossChainTransaction::SetWAR(const K& key, size_t cause_id) {
    auto guard = Guard{rerun_keys_mu};
    rerun_keys.push_back(key);
    should_wait = std::max(should_wait, cause_id);
}

// CrossChainTable实现
CrossChainTable::CrossChainTable(size_t partitions):
    Table<K, V, KeyHasher>{partitions}
{}

void CrossChainTable::Get(T* tx, const K& k, evmc::bytes32& v, size_t& version) {
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

void CrossChainTable::Put(T* tx, const K& k, const evmc::bytes32& v) {
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
        _v.entries.insert(rit.base(), CrossChainEntry {
            .value   = v,
            .version = tx->id,
            .readers = std::unordered_set<T*>()
        });
    });
}

void CrossChainTable::RegretGet(T* tx, const K& k, size_t version) {
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

void CrossChainTable::RegretPut(T* tx, const K& k) {
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

void CrossChainTable::ClearGet(T* tx, const K& k, size_t version) {
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

void CrossChainTable::ClearPut(T* tx, const K& k) {
    LOG(INFO) << "remove write record before " << tx->id << "(" << tx << ")" << " from " << KeyHasher()(k) % 1000;
    Table::Put(k, [&](V& _v) {
        while (_v.entries.size() && _v.entries.front().version < tx->id) {
            _v.entries.pop_front();
        }
    });
}

// SpectrumCrossChain实现
SpectrumCrossChain::SpectrumCrossChain(Workload& workload, Statistics& statistics, 
                                     size_t num_executors, size_t table_partitions, 
                                     EVMType evm_type) :
    workload{workload},
    statistics{statistics},
    num_executors{num_executors},
    table{table_partitions},
    stop_latch{static_cast<ptrdiff_t>(num_executors), []{}}
{
    LOG(INFO) << fmt::format("SpectrumCrossChain(num_executors={}, table_partitions={}, evm_type={})", 
                           num_executors, table_partitions, evm_type);
    workload.SetEVMType(evm_type);
}

void SpectrumCrossChain::Start() {
    LOG(INFO) << "跨链Spectrum协议启动...";
    stop_flag.store(false);
    for (size_t i = 0; i < num_executors; ++i) {
        executors.push_back(std::thread([this]{
            std::make_unique<CrossChainExecutor>(*this)->Run();
        }));
        PinRoundRobin(executors[i], i);
    }
}

void SpectrumCrossChain::Stop() {
    LOG(INFO) << "跨链Spectrum协议停止...";
    stop_flag.store(true);
    for (auto& x: executors) { x.join(); }
}

// CrossChainExecutor实现
CrossChainExecutor::CrossChainExecutor(SpectrumCrossChain& spectrum) :
    workload{spectrum.workload},
    table{spectrum.table},
    statistics{spectrum.statistics},
    last_executed{spectrum.last_executed},
    last_finalized{spectrum.last_finalized},
    stop_flag{spectrum.stop_flag},
    stop_latch{spectrum.stop_latch}
{
}

void CrossChainExecutor::Generate() {
    if (tx != nullptr) return;
    
    // 生成新的交易ID
    auto id = last_executed.fetch_add(1);
    
    // 随机决定交易类型，10%概率是跨链交易
    auto type = (std::rand() % 10 == 0) ? TransactionType::CROSS_CHAIN : TransactionType::REGULAR;
    
    // 创建新的交易
    tx = std::make_unique<CrossChainTransaction>(workload.Next(), id, type);
    tx->start_time = steady_clock::now();
    tx->berun_flag.store(true);
    
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
            LOG(INFO) << "spectrum tx " << tx->id << " break";
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
        for (auto& tup: tx->tuples_put) {
            if (tup.key != _key) { continue; }
            LOG(INFO) << "spectrum tx " << tx->id << " has key " << KeyHasher()(_key) % 1000 << " in tuples_put";
            return tup.value;
        }
        
        // 再检查本地读取缓存
        for (auto& tup: tx->tuples_get) {
            if (tup.key != _key) { continue; }
            LOG(INFO) << "spectrum tx " << tx->id << " has key " << KeyHasher()(_key) % 1000 << " in tuples_get";
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
            LOG(INFO) << "spectrum tx " << tx->id << " break";
            tx->Break();
        }
        return value;
    });
    
    LOG(INFO) << "执行交易 " << tx->id 
              << (tx->GetType() == TransactionType::CROSS_CHAIN ? " (跨链)" : " (普通)");
    
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

void CrossChainExecutor::ReExecute() {
    LOG(INFO) << "重新执行交易 " << tx->id;
    
    // 获取当前重新执行的键
    std::vector<K> rerun_keys{};
    {
        auto guard = Guard{tx->rerun_keys_mu}; 
        std::swap(tx->rerun_keys, rerun_keys);
    }
    
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
    for (auto& entry: tx->tuples_put) {
        if (tx->HasWAR()) { break; }
        if (entry.is_committed) { continue; }
        table.Put(tx.get(), entry.key, entry.value);
        entry.is_committed = true;
    }
}

void CrossChainExecutor::Finalize() {
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
    statistics.JournalCommit(latency);
    statistics.JournalMemory(tx->mm_count);
    
    // 交易已完成，清空交易指针
    tx = nullptr;
}

void CrossChainExecutor::Run() {
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
            // 如果上一个交易已经完成，且当前交易不需要重新执行，
            // 对于跨链交易，检查是否已经过了延迟时间
            if (tx->GetType() == TransactionType::CROSS_CHAIN) {
                // 如果还没有设置过完成时间，则设置完成时间
                if (tx->ready_time == steady_clock::time_point{}) {
                    tx->SetReadyTime();
                }
                
                // 检查是否已经可以完成
                if (!tx->IsReadyForFinalize()) {
                    // LOG(INFO) << fmt::format("跨链交易 ID={} 尚未达到延迟完成时间，暂不finalize", tx->id);
                    continue; // 还不能完成，稍后再尝试
                }
                
                // 已经到达延迟完成时间，标记为已完成
                tx->SetDelayCompleted();
                LOG(INFO) << fmt::format("跨链交易 ID={} 已完成2秒延迟，可以finalize", tx->id);
            }
            
            // 可以最终提交并准备处理下一个交易
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