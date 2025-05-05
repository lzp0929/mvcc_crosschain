#include "two_phase_locking_crosschain_pro.hpp"
#include <glog/logging.h>
#include <filesystem>
#include <spectrum/common/lock-util.hpp>
#include <spectrum/common/thread-util.hpp>
#include <fmt/core.h>
#include <functional>
#include <thread>
#include <chrono>
#include <ranges>
#include <algorithm>
#include <sstream>
#include <fstream>

namespace spectrum {

using namespace std::chrono;

TPLCCProTransaction::TPLCCProTransaction(Transaction&& tx, size_t id, TxType type)
    : Transaction{std::move(tx)}, id{id}, type{type} {
    
    start_time = std::chrono::steady_clock::now();
    
    // 设置存储处理函数
    auto set_storage = [this, id = this->id](
        const evmc::address &addr, 
        const evmc::bytes32 &key, 
        const evmc::bytes32 &value
    ) {
        auto _key = std::make_tuple(addr, key);
        write_set_.emplace(_key, value);
        DLOG(INFO) << "tx " << id << " write key=" << KeyHasher()(_key) % 1000;
        return evmc_storage_status::EVMC_STORAGE_MODIFIED;
    };
    
    auto get_storage = [this, id = this->id](
        const evmc::address &addr, 
        const evmc::bytes32 &key
    ) {
        auto _key = std::make_tuple(addr, key);
        
        // 先检查写集
        auto write_it = write_set_.find(_key);
        if (write_it != write_set_.end()) {
            DLOG(INFO) << "tx " << id << " read from write_set key=" << KeyHasher()(_key) % 1000;
            return write_it->second;
        }
        
        // 再检查读集
        auto read_it = read_set_.find(_key);
        if (read_it != read_set_.end()) {
            DLOG(INFO) << "tx " << id << " read from read_set key=" << KeyHasher()(_key) % 1000;
            return read_it->second;
        }
        
        // 从存储中读取
        if (!read_set_.contains(_key)) {
            read_set_.emplace(_key, evmc::bytes32{0});
            DLOG(INFO) << "tx " << id << " read new key=" << KeyHasher()(_key) % 1000;
        }
        return read_set_[_key];
    };
    
    // 安装处理函数
    InstallSetStorageHandler(std::move(set_storage));
    InstallGetStorageHandler(std::move(get_storage));
}

bool TPLCCProTable::Get(const K& key, evmc::bytes32& value, TPLCCProTransaction& tx) {
    if (!tx.growing_phase) {
        DLOG(INFO) << "tx" << tx.id << " 已进入收缩阶段，无法执行Get操作";
        return false;
    }
    
    auto segment = GetSegment(key);
    auto& segment_lock = segment_locks[segment];
    auto& segment_table = segments[segment];
    
    {
        auto guard = spectrum::Guard<spectrum::SpinLock>{segment_lock};
        auto it = segment_table.find(key);
        if (it == segment_table.end()) {
            DLOG(INFO) << "tx" << tx.id << " Get失败：key=" << KeyHasher()(key) % 1000;
            return false;
        }
        
        // 尝试获取共享锁
        if (!it->second.mutex->try_lock_shared()) {
            DLOG(INFO) << "tx" << tx.id << " Get失败：无法获取读锁 key=" << KeyHasher()(key) % 1000;
            return false;
        }
        
        value = it->second.value;
        tx.read_set_.emplace(key, value);
        DLOG(INFO) << "tx" << tx.id << " Get成功：key=" << KeyHasher()(key) % 1000;
    }
    
    return true;
}

bool TPLCCProTable::Put(const K& key, const evmc::bytes32& value, TPLCCProTransaction& tx) {
    if (!tx.growing_phase) {
        DLOG(INFO) << "tx" << tx.id << " 已进入收缩阶段，无法执行Put操作";
        return false;
    }
    
    auto segment = GetSegment(key);
    auto& segment_lock = segment_locks[segment];
    auto& segment_table = segments[segment];
    
    {
        auto guard = spectrum::Guard<spectrum::SpinLock>{segment_lock};
        auto it = segment_table.find(key);
        if (it == segment_table.end()) {
            TPLCCProEntry entry;
            auto [new_it, success] = segment_table.try_emplace(key, std::move(entry));
            it = new_it;
        }
        
        // 尝试获取排他锁
        if (!it->second.mutex->try_lock()) {
            DLOG(INFO) << "tx" << tx.id << " Put失败：无法获取写锁 key=" << KeyHasher()(key) % 1000;
            return false;
        }
        
        tx.write_set_.emplace(key, value);
        DLOG(INFO) << "tx" << tx.id << " Put成功：key=" << KeyHasher()(key) % 1000;
    }
    
    return true;
}

evmc::bytes32 TPLCCProTable::ReadForSet(const K& key, TPLCCProTransaction& tx) {
    auto segment = GetSegment(key);
    auto& segment_lock = segment_locks[segment];
    auto& segment_table = segments[segment];
    
    auto guard = spectrum::Guard<spectrum::SpinLock>{segment_lock};
    auto it = segment_table.find(key);
    if (it == segment_table.end()) {
        return evmc::bytes32{0};
    }
    return it->second.value;
}

void TPLCCProTable::WriteForSet(const K& key, const evmc::bytes32& value, TPLCCProTransaction& tx) {
    tx.write_set_.emplace(key, value);
}

bool TPLCCProTable::AcquireLocks(TPLCCProTransaction& tx) {
    // 收集所有需要的键
    std::vector<K> all_keys;
    
    // 添加写集的键
    for (const auto& [key, value] : tx.write_set_) {
        all_keys.push_back(key);
    }
    
    // 添加读集的键（排除写集中已有的键）
    for (const auto& [key, value] : tx.read_set_) {
        if (tx.write_set_.find(key) == tx.write_set_.end()) {
            all_keys.push_back(key);
        }
    }
    
    // 对所有键进行排序以防止死锁
    std::sort(all_keys.begin(), all_keys.end());
    
    // 设置获取锁的超时时间
    auto timeout = std::chrono::milliseconds(100);
    auto start_time = std::chrono::steady_clock::now();
    
    // 获取所有锁
    for (const auto& key : all_keys) {
        auto segment = GetSegment(key);
        auto& segment_lock = segment_locks[segment];
        auto& segment_table = segments[segment];
        
        // 检查是否超时
        if (std::chrono::steady_clock::now() - start_time > timeout) {
            DLOG(INFO) << "tx" << tx.id << (tx.type == TxType::CROSS_CHAIN ? "(跨链)" : "") 
                      << " 获取锁超时，中止事务";
            AbortAndRelease(tx);
            return false;
        }
        
        {
            auto guard = spectrum::Guard<spectrum::SpinLock>{segment_lock};
            auto it = segment_table.find(key);
            if (it == segment_table.end()) {
                TPLCCProEntry entry;
                auto [new_it, success] = segment_table.try_emplace(key, std::move(entry));
                it = new_it;
            }
            
            // 根据操作类型获取对应的锁
            bool is_write = tx.write_set_.find(key) != tx.write_set_.end();
            bool lock_success = is_write ? 
                it->second.mutex->try_lock() : 
                it->second.mutex->try_lock_shared();
            
            if (!lock_success) {
                DLOG(INFO) << "tx" << tx.id << (tx.type == TxType::CROSS_CHAIN ? "(跨链)" : "") 
                          << " 无法获取" << (is_write ? "写" : "读") << "锁：key=" 
                          << KeyHasher()(key) % 1000;
                AbortAndRelease(tx);
                return false;
            }
        }
    }
    
    DLOG(INFO) << "tx" << tx.id << (tx.type == TxType::CROSS_CHAIN ? "(跨链)" : "") 
              << " 成功获取所有锁";
    return true;
}

bool TPLCCProTable::ValidateReadSet(TPLCCProTransaction& tx) {
    for (const auto& [key, value] : tx.read_set_) {
        auto segment = GetSegment(key);
        auto& segment_lock = segment_locks[segment];
        auto& segment_table = segments[segment];
        
        auto guard = spectrum::Guard<spectrum::SpinLock>{segment_lock};
        auto it = segment_table.find(key);
        if (it == segment_table.end() || it->second.value != value) {
            return false;
        }
    }
    return true;
}

void TPLCCProTable::CommitAndRelease(TPLCCProTransaction& tx) {
    // 提交写集
    for (const auto& [key, value] : tx.write_set_) {
        auto segment = GetSegment(key);
        auto& segment_lock = segment_locks[segment];
        auto& segment_table = segments[segment];
        
        auto guard = spectrum::Guard<spectrum::SpinLock>{segment_lock};
        auto it = segment_table.find(key);
        if (it != segment_table.end()) {
            it->second.value = value;
        }
    }
    
    tx.committed = true;
    tx.growing_phase = false;
    
    DLOG(INFO) << "tx" << tx.id << " 成功提交事务";
}

void TPLCCProTable::CommitAndDelayRelease(TPLCCProTransaction& tx) {
    // 提交写集
    for (const auto& [key, value] : tx.write_set_) {
        auto segment = GetSegment(key);
        auto& segment_lock = segment_locks[segment];
        auto& segment_table = segments[segment];
        
        auto guard = spectrum::Guard<spectrum::SpinLock>{segment_lock};
        auto it = segment_table.find(key);
        if (it != segment_table.end()) {
            it->second.value = value;
        }
    }
    
    tx.committed = true;
    tx.growing_phase = false;
    
    DLOG(INFO) << "tx" << tx.id << "(跨链) 成功提交事务，相关键的锁将延迟2秒释放";
    
    // 等待2秒后释放锁
    std::this_thread::sleep_for(std::chrono::seconds(2));
    
    // 设置实际完成时间（在锁释放之前）
    tx.actual_finish_time = std::chrono::steady_clock::now();
    
    DLOG(INFO) << "tx" << tx.id << "(跨链) 释放所有锁";
    // 锁会通过RAII自动释放
}

void TPLCCProTable::AbortAndRelease(TPLCCProTransaction& tx) {
    tx.committed = false;
    tx.growing_phase = false;
    
    // 锁会通过RAII自动释放
    DLOG(INFO) << "tx" << tx.id << " 事务中止，释放所有锁";
}

TwoPhaseLockingCrossChainPro::TwoPhaseLockingCrossChainPro(
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
    cross_chain_ratio(cross_chain_ratio) {
    workload.SetEVMType(evm_type);
}

void TwoPhaseLockingCrossChainPro::Start() {
    stop_flag = false;
    
    // 创建日志目录
    std::filesystem::path log_path = "./logs";
    if (!std::filesystem::exists(log_path)) {
        std::filesystem::create_directory(log_path);
    }
    
    // 预留存储100笔交易记录的空间
    {
        std::lock_guard<std::mutex> lock(tx_records_mutex);
        tx_records.reserve(100);
    }
    
    // 初始化时间点
    first_tx_time = steady_clock::now();
    latest_finish_time = first_tx_time;
    latest_cross_chain_finish_time = first_tx_time;
    
    // 启动区块管理线程
    block_manager = std::thread([this]{ BlockControllerTask(); });
    
    // 启动工作线程
    for (size_t i = 0; i < num_threads; ++i) {
        executors.emplace_back([this, i]() {
            // 线程局部统计
            size_t local_executions = 0;
            size_t local_commits = 0;
            size_t local_aborts = 0;
            
            LOG(INFO) << "线程 " << i << " 启动，运行在CPU " << sched_getcpu();
            
            while (!stop_flag) {
                // 获取下一个事务
                auto tx_id = last_executed.fetch_add(1);
                
                // 随机决定是否为跨链交易
                auto type = (dist(rng) < cross_chain_ratio) ? TxType::CROSS_CHAIN : TxType::REGULAR;
                
                // 记录事务总数
                statistics.JournalTransaction();
                
                // 重试计数器
                int retry_count = 0;
                const int MAX_RETRIES = 10;
                bool success = false;
                
                while (retry_count < MAX_RETRIES && !stop_flag && !success) {
                    // 创建事务对象
                    TPLCCProTransaction tx(workload.Next(), tx_id, type);
                    
                    // 设置执行超时时间
                    auto exec_timeout = std::chrono::milliseconds(500);
                    auto exec_start = std::chrono::steady_clock::now();
                    
                    try {
                        // 启动执行
                        tx.Execute();
                        statistics.JournalExecute();
                        local_executions++;
                        
                        // 检查是否超时
                        if (std::chrono::steady_clock::now() - exec_start > exec_timeout) {
                            DLOG(INFO) << "线程 " << i << " tx" << tx_id 
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
                                    table.CommitAndDelayRelease(tx);
                                } else {
                                    table.CommitAndRelease(tx);
                                }
                                
                                // 更新统计信息
                                auto finish_time = steady_clock::now();
                                statistics.JournalCommit(
                                    duration_cast<microseconds>(
                                        finish_time - tx.start_time
                                    ).count()
                                );
                                statistics.JournalOperations(tx.CountOperations());
                                local_commits++;
                                success = true;  // 设置成功标志
                                
                                // 增加区块内交易计数
                                txs_in_current_block.fetch_add(1);
                                
                                // 在前100笔交易完成时记录日志
                                if (completed_tx_count.load() < 100) {
                                    auto now = tx.type == TxType::CROSS_CHAIN ? tx.actual_finish_time : steady_clock::now();
                                    auto latency = duration_cast<microseconds>(now - tx.start_time).count();
                                    
                                    if (!first_tx_started.exchange(true)) {
                                        first_tx_time = tx.start_time;
                                    }
                                    
                                    latest_finish_time = now;
                                    
                                    // 将交易记录添加到数组中，而不是直接写入文件
                                    {
                                        std::lock_guard<std::mutex> lock(tx_records_mutex);
                                        
                                        // 只有当记录数量小于100时才添加
                                        if (tx_records.size() < 100) {
                                            tx_records.push_back({
                                                tx.id,
                                                tx.start_time,
                                                now,
                                                static_cast<size_t>(latency),
                                                tx.type,
                                                current_block_id.load(),
                                                false // 非中止交易
                                            });
                                        }
                                    }
                                    
                                    if (completed_tx_count.fetch_add(1) + 1 >= 100) {
                                        auto total_time = duration_cast<microseconds>(latest_finish_time - first_tx_time).count();
                                        LOG(INFO) << fmt::format("完成前100笔交易总耗时: {} 微秒 ({:.2f}秒)",
                                            total_time, total_time / 1000000.0);
                                    }
                                }
                            } else {
                                table.AbortAndRelease(tx);
                                DLOG(INFO) << "线程 " << i << " tx" << tx_id 
                                          << " 读集验证失败，准备重试 (" << retry_count + 1 << "/" << MAX_RETRIES << ")";
                                retry_count++;
                            }
                        } else {
                            DLOG(INFO) << "线程 " << i << " tx" << tx_id 
                                      << " 获取锁失败，准备重试 (" << retry_count + 1 << "/" << MAX_RETRIES << ")";
                            retry_count++;
                        }
                    } catch (const std::exception& e) {
                        LOG(ERROR) << "线程 " << i << " tx" << tx_id 
                                   << " 执行异常: " << e.what() << "，准备重试 (" << retry_count + 1 << "/" << MAX_RETRIES << ")";
                    } catch (...) {
                        retry_count++;
                    }
                }
                
                // 如果所有重试都失败，记录中止
                if (!success) {
                    statistics.JournalAbort();
                    local_aborts++;
                    
                    // 记录中止交易信息
                    {
                        std::lock_guard<std::mutex> lock(tx_records_mutex);
                        tx_records.push_back({
                            tx_id,
                            steady_clock::now(),
                            steady_clock::now(),
                            0,
                            type,
                            current_block_id.load(),
                            true // 中止交易
                        });
                    }
                    
                    // 增加区块中止交易计数
                    aborted_txs_in_current_block.fetch_add(1);
                    
                    DLOG(INFO) << "线程 " << i << " tx" << tx_id 
                              << " 重试" << MAX_RETRIES << "次后仍然失败，记录中止";
                }
                
                // 每1000次执行输出一次统计信息
                if (local_executions % 1000 == 0) {
                    LOG(INFO) << "线程 " << i << " 统计："
                             << " 执行=" << local_executions
                             << " 提交=" << local_commits
                             << " 中止=" << local_aborts;
                }
                
                // 执行完成后短暂休眠，避免过度竞争
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            }
            
            // 线程结束时输出最终统计
            LOG(INFO) << "线程 " << i << " 结束，最终统计："
                     << " 执行=" << local_executions
                     << " 提交=" << local_commits
                     << " 中止=" << local_aborts;
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

void TwoPhaseLockingCrossChainPro::Stop() {
    stop_flag = true;
    
    // 等待区块管理线程结束
    if (block_manager.joinable()) {
        block_manager.join();
    }
    
    // 等待所有工作线程结束
    for (auto& executor : executors) {
        if (executor.joinable()) {
            executor.join();
        }
    }
    executors.clear();
    
    // 输出最终的区块统计信息
    LOG(INFO) << "最终区块统计信息:\n" << GetAllBlocksInfo();
    
    // 如果已经完成了100笔交易，写入日志文件
    if (completed_tx_count.load() >= 100) {
        // 打开日志文件
        std::ofstream latency_log("./logs/latency_block_two_lock.log", std::ios::out | std::ios::app);
        if (!latency_log.is_open()) {
            LOG(ERROR) << "无法创建日志文件 ./logs/latency_block_two_lock.log";
            return;
        }
        
        // 写入表头
        latency_log << "交易ID,开始时间,完成时间,延迟(微秒),类型,区块ID,总延迟(微秒),是否中止\n";
        
        // 获取交易记录并写入文件
        {
            std::lock_guard<std::mutex> lock(tx_records_mutex);
            for (const auto& record : tx_records) {
                // 确保我们只写入前100笔交易
                if (latency_log.is_open()) {
                    latency_log << record.tx_id << ","
                               << duration_cast<microseconds>(record.start_time.time_since_epoch()).count() << ","
                               << duration_cast<microseconds>(record.finish_time.time_since_epoch()).count() << ","
                               << record.latency << ","
                               << (record.type == TxType::CROSS_CHAIN ? "跨链" : "普通") << ","
                               << record.block_id << ","
                               << duration_cast<microseconds>(record.finish_time - first_tx_time).count() << ","
                               << (record.is_aborted ? "1" : "0") << "\n";
                }
            }
        }
        
        // 写入执行统计信息
        latency_log << "\n=== 执行统计 ===\n";
        
        // 第一笔交易开始时间
        auto first_tx_start_us = duration_cast<microseconds>(
            first_tx_time.time_since_epoch()).count();
        latency_log << "第一笔交易开始时间: " << first_tx_start_us << " 微秒\n";
        
        // 最后一笔交易完成时间（包含跨链延迟）
        auto last_tx_time = std::max(latest_finish_time, latest_cross_chain_finish_time);
        auto last_tx_finish_us = duration_cast<microseconds>(
            last_tx_time.time_since_epoch()).count();
        latency_log << "最后一笔交易完成时间: " << last_tx_finish_us << " 微秒\n";
        
        // 计算总执行时间（包含跨链延迟）
        auto total_time_with_delay = duration_cast<microseconds>(
            last_tx_time - first_tx_time).count();
        latency_log << "前100笔交易总执行时间(包含跨链延迟): " 
                   << total_time_with_delay << " 微秒\n";
        
        // 计算总执行时间（不含跨链延迟）
        auto total_time = duration_cast<microseconds>(
            latest_finish_time - first_tx_time).count();
        latency_log << "前100笔交易总执行时间(不含跨链延迟): " 
                   << total_time << " 微秒\n";
        
        // 确保写入到文件
        latency_log.flush();
        latency_log.close();
        
        LOG(INFO) << "执行统计信息已写入日志文件";
    }
}

void TwoPhaseLockingCrossChainPro::BlockControllerTask() {
    LOG(INFO) << "区块控制任务启动";
    
    while (!stop_flag) {
        auto block_id = current_block_id.load();
        auto block_start_time = steady_clock::now();
        
        // 重置区块内交易计数
        txs_in_current_block.store(0);
        
        // 等待区块内交易完成或达到时间限制
        auto block_deadline = block_start_time + microseconds(static_cast<long long>(block_interval_seconds * 1000000));
        
        bool should_complete_block = false;
        while (!should_complete_block && !stop_flag) {
            // 检查是否已经收集到足够的交易
            auto current_txs = txs_in_current_block.load();
            LOG(INFO) << fmt::format("区块 {} 当前交易数: {}/{}", 
                block_id, current_txs, txs_per_block);
                
            if (current_txs >= txs_per_block && current_txs > 0) {
                LOG(INFO) << fmt::format("区块 {} 已收集到足够的交易 ({}/{})", 
                    block_id, current_txs, txs_per_block);
                should_complete_block = true;
                break;
            }
            
            // 检查是否达到时间限制
            if (steady_clock::now() >= block_deadline) {
                auto current_txs = txs_in_current_block.load();
                if (current_txs > 0) {
                    LOG(INFO) << fmt::format("区块 {} 达到时间限制，当前交易数: {}", block_id, current_txs);
                    should_complete_block = true;
                    break;
                } else {
                    LOG(INFO) << fmt::format("区块 {} 达到时间限制，但没有交易，继续等待", block_id);
                    block_deadline = steady_clock::now() + microseconds(static_cast<long long>(block_interval_seconds * 1000000));
                }
            }
            
            // 短暂休眠避免忙等
            std::this_thread::sleep_for(milliseconds(10));
        }
        
        if (stop_flag) break;
        
        // 如果没有交易，继续等待
        if (txs_in_current_block.load() == 0) {
            LOG(INFO) << fmt::format("区块 {} 没有交易，继续等待", block_id);
            continue;
        }
        
        // 处理区块完成
        OnBlockComplete(block_id);
        
        // 增加区块ID
        current_block_id.fetch_add(1);
        
        LOG(INFO) << fmt::format("区块 {} 已完成，耗时: {:.2f}秒", 
            block_id,
            duration_cast<duration<double>>(steady_clock::now() - block_start_time).count());
    }
    
    LOG(INFO) << "区块控制任务停止";
}

void TwoPhaseLockingCrossChainPro::OnBlockComplete(size_t block_id) {
    LOG(INFO) << fmt::format("区块 {} 所有交易执行完成，记录区块信息...", block_id);
    
    // 创建新的区块记录
    BlockInfo record;
    record.block_id = block_id;
    record.block_start_time = steady_clock::now() - microseconds(
        static_cast<long long>(block_interval_seconds * 1000000));
    record.block_end_time = steady_clock::now();
    
    // 获取区块内的交易总数和中止交易数
    record.tx_count = txs_in_current_block.load();
    record.aborted_tx_count = aborted_txs_in_current_block.load();
    
    // 将记录添加到列表中
    {
        std::lock_guard<std::mutex> lock(block_records_mutex);
        block_records.push_back(record);
    }
    
    LOG(INFO) << fmt::format("区块 {} 信息已记录：\n - 总交易数: {}\n - 中止交易数: {}\n - 执行时间: {:.2f}秒",
        block_id, 
        record.tx_count,
        record.aborted_tx_count,
        duration_cast<duration<double>>(record.block_end_time - record.block_start_time).count());
    
    // 重置区块中止交易计数
    aborted_txs_in_current_block.store(0);
    // 重置区块内交易计数
    txs_in_current_block.store(0);
}

std::string TwoPhaseLockingCrossChainPro::GetBlockInfo(size_t block_id) {
    std::lock_guard<std::mutex> lock(block_records_mutex);
    
    for (const auto& record : block_records) {
        if (record.block_id == block_id) {
            std::stringstream ss;
            ss << fmt::format("区块 {} 统计信息:\n", block_id);
            ss << fmt::format("总交易数: {}\n", record.tx_count);
            ss << fmt::format("中止交易数: {}\n", record.aborted_tx_count);
            ss << fmt::format("区块执行时间: {:.2f} 秒\n", 
                duration_cast<duration<double>>(record.block_end_time - record.block_start_time).count());
            return ss.str();
        }
    }
    return fmt::format("未找到区块 {} 的记录", block_id);
}

std::string TwoPhaseLockingCrossChainPro::GetAllBlocksInfo() {
    std::lock_guard<std::mutex> lock(block_records_mutex);
    
    std::stringstream ss;
    ss << "所有区块统计信息:\n";
    ss << "================\n";
    
    for (const auto& record : block_records) {
        ss << fmt::format("区块 {} 统计信息:\n", record.block_id);
        ss << fmt::format("总交易数: {}\n", record.tx_count);
        ss << fmt::format("中止交易数: {}\n", record.aborted_tx_count);
        ss << fmt::format("区块执行时间: {:.2f} 秒\n", 
            duration_cast<duration<double>>(record.block_end_time - record.block_start_time).count());
        ss << "----------------\n";
    }
    
    return ss.str();
}

std::string TwoPhaseLockingCrossChainPro::GetExecutionStatistics() {
    std::lock_guard<std::mutex> lock(tx_records_mutex);
    
    std::stringstream ss;
    ss << "执行统计信息:\n";
    ss << "================\n";
    
    // 获取完成的交易数量
    size_t completed = 0;
    size_t aborted = 0;
    
    for (const auto& record : tx_records) {
        if (record.is_aborted) {
            aborted++;
        } else {
            completed++;
        }
    }
    
    ss << fmt::format("总交易记录数: {}\n", tx_records.size());
    ss << fmt::format("成功完成数: {}\n", completed);
    ss << fmt::format("中止数: {}\n", aborted);
    
    if (!tx_records.empty()) {
        // 计算平均延迟
        size_t total_latency = 0;
        for (const auto& record : tx_records) {
            if (!record.is_aborted) { // 只计算非中止交易
                total_latency += record.latency;
            }
        }
        
        double avg_latency = completed > 0 ? static_cast<double>(total_latency) / completed : 0;
        ss << fmt::format("平均延迟: {:.2f} 微秒\n", avg_latency);
        
        // 计算总执行时间
        auto first = std::min_element(tx_records.begin(), tx_records.end(),
            [](const TransactionRecord& a, const TransactionRecord& b) {
                return a.start_time < b.start_time;
            });
        
        auto last = std::max_element(tx_records.begin(), tx_records.end(),
            [](const TransactionRecord& a, const TransactionRecord& b) {
                return a.finish_time < b.finish_time;
            });
        
        if (first != tx_records.end() && last != tx_records.end()) {
            auto total_time = duration_cast<microseconds>(
                (*last).finish_time - (*first).start_time).count();
            ss << fmt::format("总执行时间: {} 微秒 ({:.2f} 秒)\n",
                             total_time, total_time / 1000000.0);
        }
    }
    
    return ss.str();
}
}