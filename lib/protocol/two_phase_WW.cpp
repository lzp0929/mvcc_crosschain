#include "two_phase_WW.hpp"
#include <spectrum/common/thread-util.hpp>
#include <spectrum/common/hex.hpp>
#include <spectrum/common/lock-util.hpp>
#include <glog/logging.h>
#include <chrono>
#include <fmt/format.h>

namespace spectrum {

using namespace std::chrono;

// LockEntry实现
bool LockEntry::canAcquire(size_t tx_id, LockMode mode) {
    if (holders.empty()) return true;
    
    if (mode == LockMode::SHARED) {
        return holders.begin()->second != LockMode::EXCLUSIVE;
    }
    
    return holders.size() == 1 && holders.begin()->first == tx_id;
}

bool LockEntry::addHolder(size_t tx_id, LockMode mode) {
    if (!canAcquire(tx_id, mode)) return false;
    holders[tx_id] = mode;
    return true;
}

void LockEntry::removeHolder(size_t tx_id) {
    holders.erase(tx_id);
}

std::vector<size_t> LockEntry::getConflictingTxs(size_t tx_id) {
    std::vector<size_t> conflicts;
    for (const auto& [holder_id, _] : holders) {
        if (holder_id != tx_id) {
            conflicts.push_back(holder_id);
        }
    }
    return conflicts;
}

// WWLockManager实现
bool WWLockManager::acquireLock(size_t tx_id, const K& key, LockMode mode) {
    size_t segment = getSegment(key);
    auto guard = Guard{segment_locks[segment]};
    
    auto& lock_table = lock_tables[segment];
    auto& entry = lock_table[key];
    
    if (entry.canAcquire(tx_id, mode)) {
        entry.addHolder(tx_id, mode);
        DLOG(INFO) << fmt::format("tx {} acquired {} lock on key {}", 
            tx_id, 
            (mode == LockMode::SHARED ? "shared" : "exclusive"),
            KeyHasher()(key) % 1000);
        return true;
    }
    
    // Wound-Wait策略
    auto conflicts = entry.getConflictingTxs(tx_id);
    for (size_t conflict_id : conflicts) {
        if (conflict_id > tx_id) {  // 当前事务优先级更高
            DLOG(INFO) << fmt::format("tx {} wounds tx {}", tx_id, conflict_id);
            entry.removeHolder(conflict_id);
            return true;
        }
    }
    
    // 当前事务优先级较低，需要等待
    DLOG(INFO) << fmt::format("tx {} waits for lock", tx_id);
    entry.wait_queue.push({tx_id, mode, std::chrono::steady_clock::now()});
    return false;
}

void WWLockManager::releaseLocks(size_t tx_id) {
    for (size_t i = 0; i < NUM_SEGMENTS; ++i) {
        auto guard = Guard{segment_locks[i]};
        auto& lock_table = lock_tables[i];
        
        for (auto it = lock_table.begin(); it != lock_table.end();) {
            auto& entry = it->second;
            entry.removeHolder(tx_id);
            
            // 处理等待队列
            while (!entry.wait_queue.empty()) {
                auto request = entry.wait_queue.top();
                if (entry.canAcquire(request.tx_id, request.mode)) {
                    entry.addHolder(request.tx_id, request.mode);
                    entry.wait_queue.pop();
                    DLOG(INFO) << fmt::format("tx {} acquired lock after wait", request.tx_id);
                } else {
                    break;
                }
            }
            
            if (entry.holders.empty() && entry.wait_queue.empty()) {
                it = lock_table.erase(it);
            } else {
                ++it;
            }
        }
    }
    DLOG(INFO) << fmt::format("tx {} released all locks", tx_id);
}

// WWTransaction实现
WWTransaction::WWTransaction(std::unique_ptr<Transaction>&& inner, size_t id)
    : Transaction(std::move(*inner))
    , id(id)
    , start_time(std::chrono::steady_clock::now()) {}

bool WWTransaction::Read(const K& key) {
    if (is_aborted) return false;
    
    // 如果已经在写集中，直接返回写集中的值
    auto write_it = write_set.find(key);
    if (write_it != write_set.end()) {
        read_set[key] = write_it->second;
        return true;
    }
    
    // 否则添加到读集
    read_set[key] = evmc::bytes32{0};
    return true;
}

bool WWTransaction::Write(const K& key, const evmc::bytes32& value) {
    if (is_aborted) return false;
    write_set[key] = value;
    return true;
}

bool WWTransaction::Commit() {
    if (is_aborted) return false;
    is_committed = true;
    return true;
}

void WWTransaction::Abort() {
    is_aborted = true;
}

// TwoPhaseLockingWW实现
TwoPhaseLockingWW::TwoPhaseLockingWW(
    Workload& workload,
    Statistics& statistics,
    size_t num_threads,
    EVMType evm_type
)
    : workload(workload)
    , statistics(statistics)
    , num_threads(num_threads) {
    workload.SetEVMType(evm_type);
}

void TwoPhaseLockingWW::Start() {
    DLOG(INFO) << fmt::format("Starting WW-2PL protocol with {} threads", num_threads);
    stop_flag = false;
    for (size_t i = 0; i < num_threads; ++i) {
        executors.emplace_back(&TwoPhaseLockingWW::ExecutorThread, this, i);
    }
}

void TwoPhaseLockingWW::Stop() {
    DLOG(INFO) << "Stopping WW-2PL protocol";
    stop_flag = true;
    for (auto& thread : executors) {
        thread.join();
    }
    executors.clear();
}

void TwoPhaseLockingWW::ExecutorThread(size_t thread_id) {
    DLOG(INFO) << fmt::format("Thread {} started", thread_id);
    
    while (!stop_flag) {
        size_t tx_id = ++last_executed;
        std::optional<Transaction> tx_opt = workload.Next();
        if (!tx_opt.has_value()) break;
        
        auto tx = std::make_unique<Transaction>(std::move(tx_opt.value()));
        auto ww_tx = std::make_unique<WWTransaction>(std::move(tx), tx_id);
        
        // 记录事务开始
        statistics.JournalTransaction();
        
        if (ExecuteTransaction(ww_tx.get())) {
            // 记录事务提交
            auto latency = duration_cast<microseconds>(
                steady_clock::now() - ww_tx->start_time).count();
            statistics.JournalCommit(latency);
            DLOG(INFO) << fmt::format("tx {} committed", tx_id);
        } else {
            // 记录事务中止
            statistics.JournalAbort();
            DLOG(INFO) << fmt::format("tx {} aborted", tx_id);
        }
    }
    
    DLOG(INFO) << fmt::format("Thread {} stopped", thread_id);
}

bool TwoPhaseLockingWW::ExecuteTransaction(WWTransaction* tx) {
    for (const auto& op : tx->operations) {
        auto mode = op.type == Operation::Type::GET 
                   ? LockMode::SHARED 
                   : LockMode::EXCLUSIVE;
                   
        if (!lock_manager.acquireLock(tx->id, op.key, mode)) {
            if (!HandleRetry(tx->id)) {
                AbortTransaction(tx);
                return false;
            }
            continue;
        }
        
        if (op.type == Operation::Type::GET) {
            if (!tx->Read(op.key)) {
                AbortTransaction(tx);
                return false;
            }
        } else {
            if (!tx->Write(op.key, op.value)) {
                AbortTransaction(tx);
                return false;
            }
        }
    }
    
    if (!tx->Commit()) {
        AbortTransaction(tx);
        return false;
    }
    
    lock_manager.releaseLocks(tx->id);
    return true;
}

void TwoPhaseLockingWW::AbortTransaction(WWTransaction* tx) {
    tx->Abort();
    lock_manager.releaseLocks(tx->id);
}

bool TwoPhaseLockingWW::HandleRetry(size_t tx_id) {
    std::lock_guard<std::mutex> lock(retry_mutex);
    
    size_t& retry_count = retry_map[tx_id];
    retry_count++;
    
    // 如果重试次数超过最大值，返回失败
    if (retry_count >= 3) {
        DLOG(INFO) << fmt::format("tx {} exceeds max retry count", tx_id);
        return false;
    }
    
    // 等待一段时间后重试
    std::this_thread::sleep_for(milliseconds(10 * (1 << retry_count)));
    DLOG(INFO) << fmt::format("tx {} retrying, attempt {}", tx_id, retry_count + 1);
    return true;
}

} // namespace spectrum 
 