#include "two_phase_lock_block.hpp"
#include <glog/logging.h>
#include <fmt/format.h>

namespace spectrum {

// TPLBlockTransaction实现
TPLBlockTransaction::TPLBlockTransaction(Transaction&& tx, size_t id, size_t block_id, TxType type)
    : Transaction{std::move(tx)}, id{id}, block_id{block_id}, type{type} {
    start_time = std::chrono::steady_clock::now();
    LOG(INFO) << fmt::format("创建交易 ID={} 区块={} 类型={}", 
                          id, block_id, type == TxType::REGULAR ? "普通" : "跨链");
}

void TPLBlockTransaction::SetReadyTime() {
    ready_time = steady_clock::now() + seconds(2);
    LOG(INFO) << fmt::format("跨链交易 ID={} 设置延迟完成时间，将在2秒后完成", id);
}

bool TPLBlockTransaction::IsReadyForFinalize() const {
    if (type != TxType::CROSS_CHAIN || delay_completed) {
        return true;
    }
    bool ready = steady_clock::now() >= ready_time;
    if (ready) {
        LOG(INFO) << fmt::format("跨链交易 ID={} 已到达延迟完成时间，可以finalize", id);
    }
    return ready;
}

// TPLBlockTable实现
bool TPLBlockTable::Get(const K& key, evmc::bytes32& value, TPLBlockTransaction& tx) {
    if (!tx.growing_phase) {
        LOG(INFO) << "tx" << tx.id << " 已进入收缩阶段，无法执行Get操作";
        return false;
    }
    
    auto segment = GetSegment(key);
    auto& segment_lock = segment_locks[segment];
    auto& segment_table = segments[segment];
    
    {
        auto guard = Guard{segment_lock};
        auto it = segment_table.find(key);
        if (it == segment_table.end()) {
            LOG(INFO) << "tx" << tx.id << " Get失败：key=" << KeyHasher()(key) % 1000;
            return false;
        }
        
        if (!it->second.mutex->try_lock_shared()) {
            LOG(INFO) << "tx" << tx.id << " Get失败：无法获取读锁 key=" << KeyHasher()(key) % 1000;
            return false;
        }
        
        value = it->second.value;
        tx.read_set_.emplace(key, value);
        LOG(INFO) << "tx" << tx.id << " Get成功：key=" << KeyHasher()(key) % 1000;
    }
    
    return true;
}

bool TPLBlockTable::Put(const K& key, const evmc::bytes32& value, TPLBlockTransaction& tx) {
    if (!tx.growing_phase) {
        LOG(INFO) << "tx" << tx.id << " 已进入收缩阶段，无法执行Put操作";
        return false;
    }
    
    auto segment = GetSegment(key);
    auto& segment_lock = segment_locks[segment];
    auto& segment_table = segments[segment];
    
    {
        auto guard = Guard{segment_lock};
        auto it = segment_table.find(key);
        if (it == segment_table.end()) {
            TPLBlockEntry entry;
            auto [new_it, success] = segment_table.try_emplace(key, std::move(entry));
            it = new_it;
        }
        
        if (!it->second.mutex->try_lock()) {
            LOG(INFO) << "tx" << tx.id << " Put失败：无法获取写锁 key=" << KeyHasher()(key) % 1000;
            return false;
        }
        
        tx.write_set_.emplace(key, value);
        LOG(INFO) << "tx" << tx.id << " Put成功：key=" << KeyHasher()(key) % 1000;
    }
    
    return true;
}

evmc::bytes32 TPLBlockTable::ReadForSet(const K& key, TPLBlockTransaction& tx) {
    auto segment = GetSegment(key);
    auto& segment_lock = segment_locks[segment];
    auto& segment_table = segments[segment];
    
    auto guard = Guard{segment_lock};
    auto it = segment_table.find(key);
    if (it == segment_table.end()) {
        return evmc::bytes32{0};
    }
    return it->second.value;
}

void TPLBlockTable::WriteForSet(const K& key, const evmc::bytes32& value, TPLBlockTransaction& tx) {
    tx.write_set_.emplace(key, value);
}

bool TPLBlockTable::AcquireLocks(TPLBlockTransaction& tx) {
    std::vector<K> all_keys;
    
    for (const auto& [key, value] : tx.write_set_) {
        all_keys.push_back(key);
    }
    
    for (const auto& [key, value] : tx.read_set_) {
        if (tx.write_set_.find(key) == tx.write_set_.end()) {
            all_keys.push_back(key);
        }
    }
    
    std::sort(all_keys.begin(), all_keys.end());
    
    auto timeout = std::chrono::milliseconds(100);
    auto start_time = std::chrono::steady_clock::now();
    
    for (const auto& key : all_keys) {
        auto segment = GetSegment(key);
        auto& segment_lock = segment_locks[segment];
        auto& segment_table = segments[segment];
        
        if (std::chrono::steady_clock::now() - start_time > timeout) {
            LOG(INFO) << "tx" << tx.id << (tx.type == TxType::CROSS_CHAIN ? "(跨链)" : "") 
                      << " 获取锁超时，中止事务";
            AbortAndRelease(tx);
            return false;
        }
        
        {
            auto guard = Guard{segment_lock};
            auto it = segment_table.find(key);
            if (it == segment_table.end()) {
                TPLBlockEntry entry;
                auto [new_it, success] = segment_table.try_emplace(key, std::move(entry));
                it = new_it;
            }
            
            bool is_write = tx.write_set_.find(key) != tx.write_set_.end();
            bool lock_success = is_write ? 
                it->second.mutex->try_lock() : 
                it->second.mutex->try_lock_shared();
            
            if (!lock_success) {
                LOG(INFO) << "tx" << tx.id << (tx.type == TxType::CROSS_CHAIN ? "(跨链)" : "") 
                          << " 无法获取" << (is_write ? "写" : "读") << "锁：key=" 
                          << KeyHasher()(key) % 1000;
                AbortAndRelease(tx);
                return false;
            }
        }
    }
    
    LOG(INFO) << "tx" << tx.id << (tx.type == TxType::CROSS_CHAIN ? "(跨链)" : "") 
              << " 成功获取所有锁";
    return true;
}

bool TPLBlockTable::ValidateReadSet(TPLBlockTransaction& tx) {
    for (const auto& [key, value] : tx.read_set_) {
        auto segment = GetSegment(key);
        auto& segment_lock = segment_locks[segment];
        auto& segment_table = segments[segment];
        
        auto guard = Guard{segment_lock};
        auto it = segment_table.find(key);
        if (it == segment_table.end() || it->second.value != value) {
            return false;
        }
    }
    return true;
}

void TPLBlockTable::CommitAndRelease(TPLBlockTransaction& tx) {
    for (const auto& [key, value] : tx.write_set_) {
        auto segment = GetSegment(key);
        auto& segment_lock = segment_locks[segment];
        auto& segment_table = segments[segment];
        
        auto guard = Guard{segment_lock};
        auto it = segment_table.find(key);
        if (it != segment_table.end()) {
            it->second.value = value;
        }
    }
    
    tx.committed = true;
    tx.growing_phase = false;
    
    LOG(INFO) << "tx" << tx.id << " 成功提交事务";
}

void TPLBlockTable::CommitAndDelayRelease(TPLBlockTransaction& tx) {
    for (const auto& [key, value] : tx.write_set_) {
        auto segment = GetSegment(key);
        auto& segment_lock = segment_locks[segment];
        auto& segment_table = segments[segment];
        
        auto guard = Guard{segment_lock};
        auto it = segment_table.find(key);
        if (it != segment_table.end()) {
            it->second.value = value;
        }
    }
    
    tx.committed = true;
    tx.growing_phase = false;
    
    LOG(INFO) << "tx" << tx.id << "(跨链) 成功提交事务，相关键的锁将延迟1秒释放";
    std::this_thread::sleep_for(std::chrono::seconds(1));
    LOG(INFO) << "tx" << tx.id << "(跨链) 释放所有锁";
}

void TPLBlockTable::AbortAndRelease(TPLBlockTransaction& tx) {
    tx.committed = false;
    tx.growing_phase = false;
    LOG(INFO) << "tx" << tx.id << " 事务中止，释放所有锁";
}

// TwoPhaseLockBlock实现
TwoPhaseLockBlock::TwoPhaseLockBlock(
    Workload& workload, 
    Statistics& statistics, 
    size_t num_threads,
    EVMType evm_type,
    size_t txs_per_block,
    double block_interval_seconds,
    double cross_chain_ratio
) : workload(workload),
    statistics(statistics),
    num_threads(num_threads),
    txs_per_block(txs_per_block),
    block_interval_seconds(block_interval_seconds),
    cross_chain_ratio(cross_chain_ratio),
    stop_barrier(num_threads)
{
    workload.SetEVMType(evm_type);
    LOG(INFO) << fmt::format("TwoPhaseLockBlock(num_threads={}, txs_per_block={}, block_interval={}s, cross_chain_ratio={})", 
                           num_threads, txs_per_block, block_interval_seconds, cross_chain_ratio);
}

void TwoPhaseLockBlock::Start() {
    stop_flag = false;
    
    // 启动区块管理线程
    block_manager = std::thread([this] { BlockControllerTask(); });
    
    // 启动工作线程
    for (size_t i = 0; i < num_threads; ++i) {
        executors.emplace_back([this, i]() {
            // 线程局部统计
            size_t local_executions = 0;
            size_t local_commits = 0;
            size_t local_aborts = 0;
            
            LOG(INFO) << "线程 " << i << " 启动，运行在CPU " << sched_getcpu();
            
            while (!stop_flag) {
                // 检查是否需要暂停当前区块
                {
                    std::unique_lock<std::mutex> lock(block_mutex);
                    block_cv.wait(lock, [this] { return !block_paused || stop_flag; });
                    if (stop_flag) break;
                }
                
                // 获取下一个事务
                auto tx_id = last_executed.fetch_add(1);
                auto current_block = current_block_id.load();
                
                // 检查当前区块是否已满
                if (txs_in_current_block.fetch_add(1) >= txs_per_block) {
                    continue;
                }
                
                // 随机决定是否为跨链交易
                auto type = (dist(rng) < cross_chain_ratio) ? TxType::CROSS_CHAIN : TxType::REGULAR;
                
                // 记录事务总数
                statistics.JournalTransaction();
                
                // 重试计数器
                int retry_count = 0;
                const int MAX_RETRIES = 5;
                bool success = false;
                
                while (retry_count < MAX_RETRIES && !stop_flag) {
                    // 创建事务对象
                    auto tx_ptr = std::make_unique<TPLBlockTransaction>(workload.Next(), tx_id, current_block, type);
                    auto& tx = *tx_ptr;
                    
                    // 设置执行超时时间
                    auto exec_timeout = std::chrono::milliseconds(500);
                    auto exec_start = std::chrono::steady_clock::now();
                    
                    try {
                        // 启动执行
                        tx.Execute();
                        statistics.JournalExecute();
                        local_executions++;
                        
                        LOG(INFO) << "线程 " << i << " 执行事务 " << tx_id 
                                  << " (重试次数=" << retry_count 
                                  << ", 总执行次数=" << local_executions << ")";
                        
                        // 检查是否超时
                        if (std::chrono::steady_clock::now() - exec_start > exec_timeout) {
                            LOG(INFO) << "线程 " << i << " tx" << tx_id 
                                      << " 执行超时，准备重试 (" << retry_count + 1 << "/" << MAX_RETRIES << ")";
                            retry_count++;
                            continue;
                        }
                        
                        // 获取所有锁
                        if (table.AcquireLocks(tx)) {
                            // 验证读集
                            if (table.ValidateReadSet(tx)) {
                                // 根据交易类型选择提交方式
                                if (tx.type == TxType::CROSS_CHAIN) {
                                    // 设置跨链交易完成时间
                                    tx.SetReadyTime();
                                    // 等待延迟完成
                                    while (!tx.IsReadyForFinalize() && !stop_flag) {
                                        std::this_thread::sleep_for(std::chrono::milliseconds(100));
                                    }
                                    if (!stop_flag) {
                                        table.CommitAndDelayRelease(tx);
                                    }
                                } else {
                                    table.CommitAndRelease(tx);
                                }
                                
                                // 更新统计信息
                                statistics.JournalCommit(
                                    duration_cast<microseconds>(
                                        steady_clock::now() - tx.start_time
                                    ).count()
                                );
                                statistics.JournalOperations(tx.CountOperations());
                                local_commits++;
                                
                                // 将交易添加到活跃交易列表
                                {
                                    std::lock_guard<std::mutex> lock(transactions_mutex);
                                    active_transactions.push_back(std::move(tx_ptr));
                                }
                                
                                success = true;
                                break;
                            } else {
                                table.AbortAndRelease(tx);
                                LOG(INFO) << "线程 " << i << " tx" << tx_id 
                                          << " 读集验证失败，准备重试 (" << retry_count + 1 << "/" << MAX_RETRIES << ")";
                            }
                        } else {
                            LOG(INFO) << "线程 " << i << " tx" << tx_id 
                                      << " 获取锁失败，准备重试 (" << retry_count + 1 << "/" << MAX_RETRIES << ")";
                        }
                    } catch (const std::exception& e) {
                        LOG(ERROR) << "线程 " << i << " tx" << tx_id 
                                   << " 执行异常: " << e.what() << "，准备重试 (" << retry_count + 1 << "/" << MAX_RETRIES << ")";
                    } catch (...) {
                        LOG(ERROR) << "线程 " << i << " tx" << tx_id 
                                   << " 执行未知异常，准备重试 (" << retry_count + 1 << "/" << MAX_RETRIES << ")";
                    }
                    
                    retry_count++;
                    std::this_thread::sleep_for(std::chrono::milliseconds(10 * retry_count));
                }
                
                if (!success) {
                    LOG(INFO) << "线程 " << i << " tx" << tx_id 
                              << " 重试" << MAX_RETRIES << "次后仍然失败，记录中止";
                    statistics.JournalAbort();
                    local_aborts++;
                }
                
                if (local_executions % 1000 == 0) {
                    LOG(INFO) << "线程 " << i << " 统计："
                             << " 执行=" << local_executions
                             << " 提交=" << local_commits
                             << " 中止=" << local_aborts;
                }
                
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            }
            
            LOG(INFO) << "线程 " << i << " 结束，最终统计："
                     << " 执行=" << local_executions
                     << " 提交=" << local_commits
                     << " 中止=" << local_aborts;
                     
            stop_barrier.arrive_and_wait();
        });
        
        // 设置线程亲和性
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(i % std::thread::hardware_concurrency(), &cpuset);
        int rc = pthread_setaffinity_np(executors.back().native_handle(), sizeof(cpu_set_t), &cpuset);
        if (rc != 0) {
            LOG(WARNING) << "无法设置线程 " << i << " 的CPU亲和性";
        }
    }
}

void TwoPhaseLockBlock::Stop() {
    stop_flag = true;
    block_cv.notify_all();
    
    if (block_manager.joinable()) {
        block_manager.join();
    }
    
    for (auto& executor : executors) {
        if (executor.joinable()) {
            executor.join();
        }
    }
    executors.clear();
}

void TwoPhaseLockBlock::BlockControllerTask() {
    LOG(INFO) << "区块控制任务启动";
    
    while (!stop_flag) {
        // 开始新的区块
        auto block_id = current_block_id.load();
        auto block_start = steady_clock::now();
        
        LOG(INFO) << fmt::format("开始区块 {}", block_id);
        
        // 重置区块内交易计数
        txs_in_current_block.store(0);
        
        // 解除区块暂停
        block_paused.store(false);
        block_cv.notify_all();
        
        // 等待区块间隔
        std::this_thread::sleep_for(std::chrono::duration<double>(block_interval_seconds));
        
        // 暂停区块执行
        block_paused.store(true);
        
        // 处理区块完成
        OnBlockComplete(block_id);
        
        // 增加区块ID
        current_block_id.fetch_add(1);
    }
    
    LOG(INFO) << "区块控制任务结束";
}

void TwoPhaseLockBlock::OnBlockComplete(size_t block_id) {
    LOG(INFO) << fmt::format("完成区块 {}", block_id);
    
    // 更新区块记录
    {
        std::lock_guard<std::mutex> lock(block_records_mutex);
        std::lock_guard<std::mutex> tx_lock(transactions_mutex);
        
        BlockInfo info;
        info.block_id = block_id;
        
        // 修复时间点计算
        auto now = steady_clock::now();
        auto interval = duration_cast<steady_clock::duration>(
            std::chrono::duration<double>(block_interval_seconds)
        );
        info.block_start_time = now - interval;
        info.block_end_time = now;
        
        // 统计区块内交易
        for (const auto& tx : active_transactions) {
            if (tx->block_id == block_id) {
                info.tx_count++;
                if (tx->type == TxType::CROSS_CHAIN) {
                    info.cross_chain_tx_count++;
                } else {
                    info.regular_tx_count++;
                }
                if (!tx->committed) {
                    info.aborted_tx_count++;
                }
                
                // 计算延迟
                auto latency = duration_cast<microseconds>(
                    tx->committed ? steady_clock::now() - tx->start_time : microseconds(0)
                ).count();
                info.total_latency += latency;
            }
        }
        
        // 计算平均延迟
        if (info.tx_count > 0) {
            info.avg_latency = info.total_latency / info.tx_count;
        }
        
        block_records.push_back(info);
        
        // 清理已完成的交易
        active_transactions.erase(
            std::remove_if(
                active_transactions.begin(),
                active_transactions.end(),
                [block_id](const auto& tx) { return tx->block_id <= block_id; }
            ),
            active_transactions.end()
        );
    }
}

std::string TwoPhaseLockBlock::GetBlockInfo(size_t block_id) {
    std::lock_guard<std::mutex> lock(block_records_mutex);
    
    auto it = std::find_if(
        block_records.begin(),
        block_records.end(),
        [block_id](const auto& info) { return info.block_id == block_id; }
    );
    
    if (it == block_records.end()) {
        return fmt::format("区块 {} 不存在", block_id);
    }
    
    return fmt::format(
        "区块 {} 统计:\n"
        "  总交易数: {}\n"
        "  普通交易: {}\n"
        "  跨链交易: {}\n"
        "  中止交易: {}\n"
        "  平均延迟: {:.2f}us\n"
        "  区块时长: {:.2f}s",
        it->block_id,
        it->tx_count,
        it->regular_tx_count,
        it->cross_chain_tx_count,
        it->aborted_tx_count,
        it->avg_latency,
        duration_cast<duration<double>>(it->block_end_time - it->block_start_time).count()
    );
}

std::string TwoPhaseLockBlock::GetAllBlocksInfo() {
    std::lock_guard<std::mutex> lock(block_records_mutex);
    
    if (block_records.empty()) {
        return "没有区块记录";
    }
    
    std::stringstream ss;
    ss << "所有区块统计:\n";
    
    size_t total_tx_count = 0;
    size_t total_regular_tx_count = 0;
    size_t total_cross_chain_tx_count = 0;
    size_t total_aborted_tx_count = 0;
    double total_latency = 0;
    
    for (const auto& info : block_records) {
        total_tx_count += info.tx_count;
        total_regular_tx_count += info.regular_tx_count;
        total_cross_chain_tx_count += info.cross_chain_tx_count;
        total_aborted_tx_count += info.aborted_tx_count;
        total_latency += info.total_latency;
        
        ss << fmt::format(
            "区块 {}:\n"
            "  总交易数: {}\n"
            "  普通交易: {}\n"
            "  跨链交易: {}\n"
            "  中止交易: {}\n"
            "  平均延迟: {:.2f}us\n"
            "  区块时长: {:.2f}s\n",
            info.block_id,
            info.tx_count,
            info.regular_tx_count,
            info.cross_chain_tx_count,
            info.aborted_tx_count,
            info.avg_latency,
            duration_cast<duration<double>>(info.block_end_time - info.block_start_time).count()
        );
    }
    
    ss << fmt::format(
        "\n总体统计:\n"
        "  总区块数: {}\n"
        "  总交易数: {}\n"
        "  普通交易: {}\n"
        "  跨链交易: {}\n"
        "  中止交易: {}\n"
        "  平均延迟: {:.2f}us\n",
        block_records.size(),
        total_tx_count,
        total_regular_tx_count,
        total_cross_chain_tx_count,
        total_aborted_tx_count,
        total_tx_count > 0 ? total_latency / total_tx_count : 0.0
    );
    
    return ss.str();
}

} // namespace spectrum 