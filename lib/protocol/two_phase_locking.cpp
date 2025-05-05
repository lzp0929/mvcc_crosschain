#include <spectrum/protocol/two_phase_locking.hpp>
#include <spectrum/common/lock-util.hpp>
#include <spectrum/common/statistics.hpp>
#include <spectrum/common/glog-prefix.hpp>
#include <fmt/core.h>
#include <glog/logging.h>
#include <optional>
#include <mutex>

namespace spectrum {

using namespace std::chrono;
using Statistics = spectrum::Statistics;
using SpinLock = spectrum::SpinLock;


// 在函数定义之前定义K类型
#define K std::tuple<evmc::address, evmc::bytes32>

TwoPhaseTransaction::TwoPhaseTransaction(Transaction&& inner, size_t id)
    : Transaction(std::move(inner))
    , id(id)
    , start_time(steady_clock::now()) {}

bool TPLTable::Get(const K& key, evmc::bytes32& value, TwoPhaseTransaction& tx) {
    auto guard = Guard{table_lock};
    auto it = table.find(key);
    if (it == table.end()) {
        DLOG(INFO) << "tx " << tx.id << " Get failed: key not found";
        return false;
    }
    
    // 尝试获取共享锁
    if (!it->second.mutex->try_lock_shared()) {
        DLOG(INFO) << "tx " << tx.id << " Get failed: cannot acquire shared lock";
        return false;
    }
    std::shared_lock<std::shared_mutex> lock(*it->second.mutex, std::adopt_lock);
    value = it->second.value;
    tx.read_set.push_back({key, value});
    DLOG(INFO) << "tx " << tx.id << " Get succeeded";
    return true;
}

bool TPLTable::Put(const K& key, const evmc::bytes32& value, TwoPhaseTransaction& tx) {
    auto guard = Guard{table_lock};
    auto it = table.find(key);
    if (it == table.end()) {
        auto [new_it, inserted] = table.try_emplace(key, TPLEntry{});
        if (!inserted) {
            return false;
        }
        it = new_it;
    }
    
    // 尝试获取独占锁
    if (!it->second.mutex->try_lock()) {
        DLOG(INFO) << "tx " << tx.id << " Put failed: cannot acquire exclusive lock";
        return false;
    }
    std::unique_lock<std::shared_mutex> lock(*it->second.mutex, std::adopt_lock);
    it->second.value = value;
    tx.write_set.push_back({key, value});
    DLOG(INFO) << "tx " << tx.id << " Put succeeded";
    return true;
}

evmc::bytes32 TPLTable::ReadForSet(const K& key, TwoPhaseTransaction& tx) {
    auto guard = Guard{table_lock};
    
    // 检查写集
    for (const auto& item : tx.write_set) {
        if (item.key == key) {
            DLOG(INFO) << "tx" << tx.id << " 从写集中读取 key=" << KeyHasher()(key) % 1000;
            return item.value;
        }
    }
    
    // 检查读集
    for (const auto& item : tx.read_set) {
        if (item.key == key) {
            DLOG(INFO) << "tx" << tx.id << " 从读集中读取 key=" << KeyHasher()(key) % 1000;
            return item.value;
        }
    }
    
    // 从存储中读取
    auto it = table.find(key);
    if (it == table.end()) {
        TPLEntry entry;
        entry.value = evmc::bytes32{0};
        table.try_emplace(key, std::move(entry));
        
        DLOG(INFO) << "tx" << tx.id << " 将key=" << KeyHasher()(key) % 1000 << " 加入读集（key不存在）";
        tx.read_set.push_back({key, evmc::bytes32{0}});
        return evmc::bytes32{0};
    }
    
    auto value = it->second.value;
    tx.read_set.push_back({key, value});
    DLOG(INFO) << "tx" << tx.id << " 将key=" << KeyHasher()(key) % 1000 << " 加入读集";
    
    return value;
}

void TPLTable::WriteForSet(const K& key, const evmc::bytes32& value, TwoPhaseTransaction& tx) {
    auto guard = Guard{table_lock};
    
    // 检查写集
    for (auto& item : tx.write_set) {
        if (item.key == key) {
            item.value = value;
            DLOG(INFO) << "tx" << tx.id << " 更新写集中的 key=" << KeyHasher()(key) % 1000;
            return;
        }
    }
    
    tx.write_set.push_back({key, value});
    DLOG(INFO) << "tx" << tx.id << " 将key=" << KeyHasher()(key) % 1000 << " 加入写集";
}

bool TPLTable::AcquireLocks(TwoPhaseTransaction& tx) {
    auto guard = Guard{table_lock};
    
    DLOG(INFO) << "tx" << tx.id << " 开始获取锁, 读集大小: " << tx.read_set.size() 
              << ", 写集大小: " << tx.write_set.size();
    
    // 对键进行排序，防止死锁
    std::vector<K> write_keys;
    for (const auto& item : tx.write_set) {
        write_keys.push_back(item.key);
    }
    std::sort(write_keys.begin(), write_keys.end());
    
    // 获取写锁
    for (const auto& write_key : write_keys) {
        auto it = table.find(write_key);
        if (it == table.end()) {
            TPLEntry entry;
            entry.value = evmc::bytes32{0};
            table.try_emplace(write_key, std::move(entry));
            it = table.find(write_key);
        }
        
        if (!it->second.mutex->try_lock()) {
            ReleaseLocks(tx);
            return false;
        }
    }
    
    // 获取读锁
    for (const auto& item : tx.read_set) {
        bool in_write_set = false;
        for (const auto& write_key : write_keys) {
            if (item.key == write_key) {
                in_write_set = true;
                break;
            }
        }
        if (in_write_set) continue;
        
        auto it = table.find(item.key);
        if (it == table.end()) {
            TPLEntry entry;
            entry.value = evmc::bytes32{0};
            table.try_emplace(item.key, std::move(entry));
            it = table.find(item.key);
        }
        
        if (!it->second.mutex->try_lock_shared()) {
            ReleaseLocks(tx);
            return false;
        }
    }
    
    DLOG(INFO) << "tx" << tx.id << " 成功获取所有锁";
    return true;
}

bool TPLTable::ValidateReadSet(TwoPhaseTransaction& tx) {
    auto guard = Guard{table_lock};
    
    for (const auto& item : tx.read_set) {
        auto it = table.find(item.key);
        if (it == table.end()) {
            if (item.value != evmc::bytes32{0}) {
                return false;
            }
            continue;
        }
        
        if (it->second.value != item.value) {
            return false;
        }
    }
    
    DLOG(INFO) << "tx" << tx.id << " 读集验证成功";
    return true;
}

void TPLTable::CommitAndRelease(TwoPhaseTransaction& tx) {
    auto guard = Guard{table_lock};
    
    // 提交写集
    for (const auto& item : tx.write_set) {
        auto it = table.find(item.key);
        if (it == table.end()) {
            TPLEntry entry;
            entry.value = item.value;
            table.try_emplace(item.key, std::move(entry));
        } else {
            it->second.value = item.value;
        }
    }
    
    // 释放所有锁
    ReleaseLocks(tx);
    
    tx.committed = true;
    tx.growing_phase = false;
    
    DLOG(INFO) << "tx" << tx.id << " 成功提交事务";
}

void TPLTable::AbortAndRelease(TwoPhaseTransaction& tx) {
    auto guard = Guard{table_lock};
    
    ReleaseLocks(tx);
    
    tx.committed = false;
    tx.growing_phase = false;
    
    DLOG(INFO) << "tx" << tx.id << " 事务已中止";
}

void TPLTable::ReleaseLocks(TwoPhaseTransaction& tx) {
    // 释放写锁
    for (const auto& item : tx.write_set) {
        auto it = table.find(item.key);
        if (it != table.end()) {
            it->second.mutex->unlock();
            DLOG(INFO) << "tx" << tx.id << " 释放写锁：key=" << KeyHasher()(item.key) % 1000;
        }
    }
    
    // 释放读锁
    for (const auto& item : tx.read_set) {
        // 跳过写集中的键
        bool in_write_set = false;
        for (const auto& write_item : tx.write_set) {
            if (item.key == write_item.key) {
                in_write_set = true;
                break;
            }
        }
        if (in_write_set) continue;
        
        auto it = table.find(item.key);
        if (it != table.end()) {
            it->second.mutex->unlock_shared();
            DLOG(INFO) << "tx" << tx.id << " 释放读锁：key=" << KeyHasher()(item.key) % 1000;
        }
    }
}

// TwoPhaseLocking实现
TwoPhaseLocking::TwoPhaseLocking(Workload& workload, Statistics& statistics, 
                                size_t num_threads, EVMType evm_type)
    : workload(workload)
    , statistics(statistics)
    , num_threads(num_threads) {
    workload.SetEVMType(evm_type);
}

void TwoPhaseLocking::Start() {
    stop_flag = false;
    
    // 启动工作线程
    for (size_t i = 0; i < num_threads; ++i) {
        std::thread worker([this]() {
            while (!stop_flag) {
                // 获取下一个事务
                Transaction tx_opt = workload.Next();
                
                // 创建两阶段锁事务
                auto tx = TwoPhaseTransaction(std::move(tx_opt), last_executed++);
                
                // 记录新事务
                statistics.JournalTransaction();
                
                // 安装存储处理函数
                TwoPhaseTransaction* tx_ptr = &tx;
                tx.InstallGetStorageHandler([this, tx_ptr](
                    const evmc::address &addr, 
                    const evmc::bytes32 &key
                ) {
                    auto _key = std::make_tuple(addr, key);
                    return this->table.ReadForSet(_key, *tx_ptr);
                });
                
                tx.InstallSetStorageHandler([this, tx_ptr](
                    const evmc::address &addr, 
                    const evmc::bytes32 &key, 
                    const evmc::bytes32 &value
                ) {
                    auto _key = std::make_tuple(addr, key);
                    this->table.WriteForSet(_key, value, *tx_ptr);
                    return evmc_storage_status::EVMC_STORAGE_MODIFIED;
                });
                
                // 执行事务
                bool success = false;
                auto start = steady_clock::now();
                bool has_aborted = false;
                size_t retry_count = 0;
                const size_t MAX_RETRIES = 5;  // 最大重试次数
                const microseconds WAIT_TIME(100);  // 固定等待时间：100微秒
                
                do {
                    // 执行事务操作
                    tx.Execute();
                    
                    // 获取所有锁
                    if (!table.AcquireLocks(tx)) {
                        table.AbortAndRelease(tx);
                        has_aborted = true;
                        
                        if (++retry_count >= MAX_RETRIES) {
                            DLOG(INFO) << "事务 " << tx.id << " 达到最大重试次数，放弃执行";
                            break;
                        }
                        
                        // 固定等待时间
                        std::this_thread::sleep_for(WAIT_TIME);
                        continue;
                    }
                    
                    // 验证读集
                    if (!table.ValidateReadSet(tx)) {
                        table.AbortAndRelease(tx);
                        has_aborted = true;
                        
                        if (++retry_count >= MAX_RETRIES) {
                            DLOG(INFO) << "事务 " << tx.id << " 达到最大重试次数，放弃执行";
                            break;
                        }
                        
                        // 固定等待时间
                        std::this_thread::sleep_for(WAIT_TIME);
                        continue;
                    }
                    
                    // 提交事务
                    table.CommitAndRelease(tx);
                    success = true;
                    
                } while (!success && !stop_flag);
                
                // 记录统计信息
                if (has_aborted && !success) {
                    statistics.JournalAbort();
                }
                
                auto end = steady_clock::now();
                auto duration = duration_cast<microseconds>(end - start);
                if (success) {
                    statistics.JournalCommit(duration.count());
                }
            }
        });
        executors.push_back(std::move(worker));
    }
}

void TwoPhaseLocking::Stop() {
    stop_flag = true;
    
    // 等待所有工作线程结束
    for (auto& executor : executors) {
        if (executor.joinable()) {
            executor.join();
        }
    }
    executors.clear();
}

#undef K

} // namespace spectrum 