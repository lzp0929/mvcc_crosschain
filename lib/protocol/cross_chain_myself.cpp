#include "cross_chain_myself.hpp"
#include <glog/logging.h>
#include <fmt/core.h>
#include <pthread.h>
#include <fstream>
#include <filesystem>

namespace spectrum {

// 添加文件输出流
static std::ofstream tx_log_file;

// 在使用K、V、T之前先定义它们
using K = std::tuple<evmc::address, evmc::bytes32>;
using V = MySelfVersionList;
using T = MySelfTransaction;

// 实现线程绑定函数
void PinRoundRobin(std::thread& thread, size_t index) {
#ifdef __linux__
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(index % std::thread::hardware_concurrency(), &cpuset);
    pthread_setaffinity_np(thread.native_handle(), sizeof(cpu_set_t), &cpuset);
#endif
}

// MySelfTransaction实现
MySelfTransaction::MySelfTransaction(Transaction&& inner, size_t id, size_t block_id, MySelfTxType type)
    : Transaction{std::move(inner)}
    , id{id}
    , block_id{block_id}
    , type{type}
    , start_time{std::chrono::steady_clock::now()}
{
    LOG(INFO) << fmt::format("MySelfTransaction: 创建交易 ID={} 区块={} 类型={}", 
                          id, block_id,
                          type == MySelfTxType::REGULAR ? "普通" : "跨链");
}

bool MySelfTransaction::HasWAR() {
    // 如果事务已完成，直接返回false
    if (completed) {
        return false;
    }
    
    auto guard = Guard{rerun_keys_mu};
    return rerun_keys.size() != 0;
}

void MySelfTransaction::SetWAR(const K& key, size_t cause_id) {
    auto guard = Guard{rerun_keys_mu};
    rerun_keys.push_back(key);
    should_wait = std::max(should_wait, cause_id);
}

// MySelfTable实现
MySelfTable::MySelfTable(size_t partitions)
    : Table<K, V, KeyHasher>{partitions} {}

void MySelfTable::Get(T* tx, const K& k, evmc::bytes32& v, size_t& version, size_t& writer_tx_id) {
    Table::Put(k, [&](V& _v) {
        auto rit = _v.entries.rbegin();
        auto end = _v.entries.rend();
        while (rit != end) {
            if (rit->version > tx->id) {
                ++rit; continue;
            }
            v = rit->value;
            version = rit->version;
            writer_tx_id = rit->writer_tx_id;
            rit->readers.insert(tx);
            LOG(INFO) << fmt::format("交易 {} 读取键 {} 版本 {} (写入者:{})", 
                                   tx->id, 
                                   KeyHasher()(k) % 1000,
                                   rit->version,
                                   writer_tx_id);
            return;
        }
        version = 0;
        writer_tx_id = 0;
        LOG(INFO) << fmt::format("交易 {} 读取键 {} 默认版本", 
                               tx->id, 
                               KeyHasher()(k) % 1000);
        _v.readers_default.insert(tx);
        v = evmc::bytes32{0};
    });
}

void MySelfTable::Put(T* tx, const K& k, const evmc::bytes32& v) {
    CHECK(tx->id > 0) << "保留版本0作为默认值";
    LOG(INFO) << fmt::format("交易 {} 写入键 {}", 
                           tx->id, 
                           KeyHasher()(k) % 1000);
    
    Table::Put(k, [&](V& _v) {
        auto rit = _v.entries.rbegin();
        auto end = _v.entries.rend();
        
        while (rit != end) {
            if (rit->version > tx->id) {
                ++rit; continue;
            }
            
            // 中止读取过期数据的事务
            for (auto _tx: rit->readers) {
                LOG(INFO) << fmt::format("键 {} 存在读依赖 交易{}", 
                                       KeyHasher()(k) % 1000,
                                       _tx->id);
                if (_tx->id > tx->id) {
                    LOG(INFO) << fmt::format("交易 {} 中止 交易 {}", 
                                           tx->id,
                                           _tx->id);
                    _tx->SetWAR(k, tx->id);
                }
            }
            break;

        }
        
        for (auto _tx: _v.readers_default) {
            LOG(INFO) << fmt::format("键 {} 存在默认值读依赖 交易{}", 
                                   KeyHasher()(k) % 1000,
                                   _tx->id);
            if (_tx->id > tx->id) {
                LOG(INFO) << fmt::format("交易 {} 中止 交易 {}", 
                                       tx->id,
                                       _tx->id);
                _tx->SetWAR(k, tx->id);
            }
        }
        
        // 处理同一键的重复写入
        if (rit != end && rit->version == tx->id) {
            rit->value = v;
            return;
        }
        
        // 插入新条目
        _v.entries.insert(rit.base(), MySelfEntry{
            .value = v,
            .version = tx->id,
            .writer_tx_id = tx->id,
            .readers = std::unordered_set<T*>()
        });
    });
}

void MySelfTable::RegretGet(T* tx, const K& k, size_t version) {
    LOG(INFO) << fmt::format("移除交易 {} 对键 {} 的读记录", 
                           tx->id,
                           KeyHasher()(k) % 1000);
    
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

void MySelfTable::RegretPut(T* tx, const K& k) {
    LOG(INFO) << fmt::format("移除交易 {} 对键 {} 的写记录", 
                           tx->id,
                           KeyHasher()(k) % 1000);
    
    Table::Put(k, [&](V& _v) {
        auto vit = _v.entries.begin();
        auto end = _v.entries.end();
        while (vit != end) {
            if (vit->version != tx->id) {
                ++vit; continue;
            }
            
            // 中止读取当前事务数据的事务
            for (auto _tx: vit->readers) {
                LOG(INFO) << fmt::format("键 {} 存在读依赖 交易{}", 
                                       KeyHasher()(k) % 1000,
                                       _tx->id);
                LOG(INFO) << fmt::format("交易 {} 中止 交易 {}", 
                                       tx->id,
                                       _tx->id);
                _tx->SetWAR(k, tx->id);
            }
            break;
        }
        if (vit != end) { _v.entries.erase(vit); }
    });
}

void MySelfTable::ClearGet(T* tx, const K& k, size_t version) {
    LOG(INFO) << fmt::format("移除交易 {} 对键 {} 的读记录", 
                           tx->id,
                           KeyHasher()(k) % 1000);
    
    Table::Put(k, [&](V& _v) {
        auto vit = _v.entries.begin();
        auto end = _v.entries.end();
        while (vit != end) {
            if (vit->version != version) {
                ++vit; continue;
            }
            LOG(INFO) << fmt::format("从版本 {} 移除交易 {}", 
                                   vit->version,
                                   tx->id);
            vit->readers.erase(tx);
            break;
        }
        if (version == 0) {
            _v.readers_default.erase(tx);
        }
    });
}

void MySelfTable::ClearPut(T* tx, const K& k) {
    LOG(INFO) << fmt::format("移除交易 {} 之前对键 {} 的写记录", 
                           tx->id,
                           KeyHasher()(k) % 1000);
    
    Table::Put(k, [&](V& _v) {
        while (_v.entries.size() && _v.entries.front().version < tx->id) {
            _v.entries.pop_front();
        }
    });
}

// CrossChainMyself实现
CrossChainMyself::CrossChainMyself(
    Workload& workload, Statistics& statistics,
    size_t num_executors, size_t table_partitions,
    EVMType evm_type, size_t txs_per_block,
    double block_interval_seconds, double cross_chain_ratio
) :
    workload{workload},
    statistics{statistics},
    num_executors{num_executors},
    table{table_partitions},
    txs_per_block{txs_per_block},
    block_interval_seconds{block_interval_seconds},
    cross_chain_ratio{cross_chain_ratio},
    stop_barrier{num_executors}
{
    // 创建日志目录
    std::filesystem::path log_path = "/root/spectrum";
    if (!std::filesystem::exists(log_path)) {
        std::filesystem::create_directory(log_path);
    }
    
    // 初始化交易记录容器，预留空间用于存储100笔交易的数据
    tx_records.reserve(100);
    
    LOG(INFO) << "将记录前100笔交易数据，并在测试结束后写入 /root/spectrum/latency_block.log";
    
    workload.SetEVMType(evm_type);
}

void CrossChainMyself::Start() {
    // 防止重复启动
    if (is_running.exchange(true)) {
        LOG(WARNING) << "CrossChainMyself协议已在运行中";
        return;
    }

    LOG(INFO) << "CrossChainMyself协议启动...";
    
    try {
        // 重置所有状态
        stop_flag.store(false);
        current_block_id.store(1);
        last_executed.store(1);
        last_finalized.store(0);
        txs_in_current_block.store(0);
        block_paused.store(false);
        
        // 确保线程容器为空
        executors.clear();
        
        // 预留线程容器空间
        executors.reserve(num_executors);
        
        // 先启动区块管理线程
        try {
            block_manager = std::thread([this] { 
                BlockControllerTask(); 
            });
            LOG(INFO) << "区块管理线程启动成功";
        } catch (const std::system_error& e) {
            LOG(ERROR) << "创建区块管理线程失败: " << e.what();
            is_running.store(false);
            throw;
        }
        
        // 逐个启动执行器线程
        for (size_t i = 0; i < num_executors; ++i) {
            try {
                executors.emplace_back([this] {
                    auto executor = std::make_unique<MySelfExecutor>(*this);
                    executor->Run();
                });
                PinRoundRobin(executors.back(), i);
                LOG(INFO) << "执行器线程 " << i << " 启动成功";
            } catch (const std::system_error& e) {
                LOG(ERROR) << "创建执行器线程 " << i << " 失败: " << e.what();
                // 停止已创建的线程
                Stop();
                throw;
            }
        }
        
        LOG(INFO) << "所有线程启动完成";
        
    } catch (const std::exception& e) {
        LOG(ERROR) << "启动过程中发生错误: " << e.what();
        Stop();
        throw;
    }
}

void CrossChainMyself::Stop() {
    if (!is_running.exchange(false)) {
        LOG(WARNING) << "CrossChainMyself协议已经停止";
        return;
    }
    
    LOG(INFO) << "CrossChainMyself协议停止...";
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
    
    // 在测试结束时写入前100笔交易的日志数据
    {
        std::lock_guard<std::mutex> lock(latency_log_mutex);
        if (!tx_records.empty()) {
            // 打开日志文件
            latency_log.open("/root/spectrum/latency_block.log", std::ios::out | std::ios::app);
            if (!latency_log.is_open()) {
                LOG(ERROR) << "无法打开日志文件 /root/spectrum/latency_block.log";
            } else {
                LOG(INFO) << "写入交易日志数据到 /root/spectrum/latency_block.log";
                
                // 写入表头
                latency_log << "交易ID,开始时间,完成时间,延迟(微秒),类型" << std::endl;
                
                // 写入所有交易记录
                for (const auto& record : tx_records) {
                    auto latency = duration_cast<microseconds>(record.finish_time - record.start_time).count();
                    latency_log << record.tx_id << ","
                              << duration_cast<microseconds>(record.start_time.time_since_epoch()).count() << ","
                              << duration_cast<microseconds>(record.finish_time.time_since_epoch()).count() << ","
                              << latency << ","
                              << (record.type == MySelfTxType::CROSS_CHAIN ? "跨链" : "普通")
                              << std::endl;
                }
                
                // 如果有至少100笔交易，计算并写入汇总统计信息
                if (tx_records.size() >= 100) {
                    // 确保时间点有效
                    if (latest_finish_time >= first_tx_time) {
                        // 找出第100笔交易的完成时间
                        auto hundredth_tx_time = tx_records[99].finish_time;
                        
                        // 查找离第100笔交易最近的跨链交易完成时间
                        // 只考虑前100笔交易中的跨链交易
                        std::optional<time_point<steady_clock>> latest_cc_before_100;
                        size_t cross_chain_count = 0;
                        
                        for (size_t i = 0; i < 100; i++) {
                            if (tx_records[i].type == MySelfTxType::CROSS_CHAIN) {
                                cross_chain_count++;
                                if (!latest_cc_before_100 || tx_records[i].finish_time > latest_cc_before_100.value()) {
                                    latest_cc_before_100 = tx_records[i].finish_time;
                                }
                            }
                        }
                        
                        // 计算总执行时间
                        auto total_time = duration_cast<microseconds>(
                            hundredth_tx_time - first_tx_time).count();
                        
                        // 如果有跨链交易并且其最晚完成时间比第100笔交易的完成时间晚
                        auto total_time_with_delay = total_time;
                        if (cross_chain_count > 0 && latest_cc_before_100.has_value() && 
                            latest_cc_before_100.value() > hundredth_tx_time) {
                            total_time_with_delay = duration_cast<microseconds>(
                                latest_cc_before_100.value() - first_tx_time).count();
                        }
                        
                        latency_log << "\n=== 执行统计 ===" << std::endl;
                        latency_log << "第一笔交易开始时间: " 
                            << duration_cast<microseconds>(first_tx_time.time_since_epoch()).count() 
                            << " 微秒" << std::endl;
                        latency_log << "第100笔交易完成时间: " 
                            << duration_cast<microseconds>(hundredth_tx_time.time_since_epoch()).count() 
                            << " 微秒" << std::endl;
                        
                        if (cross_chain_count > 0 && latest_cc_before_100.has_value() && 
                            latest_cc_before_100.value() > hundredth_tx_time) {
                            latency_log << "离第100笔交易最近的跨链交易完成时间: " 
                                << duration_cast<microseconds>(latest_cc_before_100.value().time_since_epoch()).count() 
                                << " 微秒" << std::endl;
                        }
                        
                        latency_log << "前100笔交易总执行时间(包含跨链延迟): " << total_time_with_delay << " 微秒" << std::endl;
                        latency_log << "前100笔交易总执行时间(不含跨链延迟): " << total_time << " 微秒" << std::endl;
                        
                        if (cross_chain_count > 0) {
                            latency_log << "跨链交易数量: " << cross_chain_count << std::endl;
                        }
                        
                        LOG(INFO) << fmt::format("前100笔交易总执行时间(包含跨链延迟): {} 微秒", total_time_with_delay);
                        LOG(INFO) << fmt::format("前100笔交易总执行时间(不含跨链延迟): {} 微秒", total_time);
                    } else {
                        LOG(ERROR) << "时间计算错误: 最后完成时间早于第一笔交易开始时间";
                    }
                }
                
                // 关闭日志文件
                latency_log.close();
                LOG(INFO) << "日志数据写入完成";
            }
        } else {
            LOG(WARNING) << "没有交易记录，不写入日志文件";
        }
    }
    
    LOG(INFO) << "CrossChainMyself协议已完全停止";
}

std::shared_ptr<CompositeTransaction> CrossChainMyself::CreateCompositeTransaction(bool is_cross_chain) {
    auto guard = Guard{composite_lock};
    auto composite_id = next_composite_id.fetch_add(1);
    auto composite = std::make_shared<CompositeTransaction>(composite_id, is_cross_chain);
    composite_txs[composite_id] = composite;
    LOG(INFO) << fmt::format("创建复合事务体 ID={} 类型={}", 
                           composite_id,
                           is_cross_chain ? "跨链" : "普通");
    return composite;
}

void CrossChainMyself::AddTransactionToComposite(MySelfTransaction* tx) {
    // 如果是跨链交易，创建新的复合事务体
    if (tx->type == MySelfTxType::CROSS_CHAIN) {
        auto composite = CreateCompositeTransaction(true);
        tx->composite_tx = composite;
        composite->tx_list.push_back(tx->id);
        
        // 添加写集
        for (const auto& put_tuple : tx->tuples_put) {
            composite->write_set.push_back(WriteSetEntry{
                .key = put_tuple.key,
                .value = put_tuple.value,
                .writer_tx_id = tx->id
            });
        }
        
        LOG(INFO) << fmt::format("跨链交易 {} 创建新的复合事务体 {}", 
                               tx->id,
                               composite->composite_id);
        return;
    }
    
    // 对于普通交易，获取依赖的复合事务体
    auto dependent_composites = GetDependentCompositeTxs(tx);
    
    if (dependent_composites.empty()) {
        // 如果没有依赖任何复合事务体，直接完成交易
        LOG(INFO) << fmt::format("普通交易 {} 没有依赖任何复合事务体，直接完成", tx->id);
        return;
    }
    
    // 创建新的复合事务体
    auto composite = CreateCompositeTransaction(false);
    tx->composite_tx = composite;
    composite->tx_list.push_back(tx->id);
    
    // 添加写集
    for (const auto& put_tuple : tx->tuples_put) {
        composite->write_set.push_back(WriteSetEntry{
            .key = put_tuple.key,
            .value = put_tuple.value,
            .writer_tx_id = tx->id
        });
    }
    
    // 添加所有依赖关系
    for (const auto& dep : dependent_composites) {
        composite->dependent_composites.insert(dep->composite_id);
    }
    
    LOG(INFO) << fmt::format("普通交易 {} 创建新的复合事务体 {} 并依赖 {} 个其他复合事务体", 
                           tx->id,
                           composite->composite_id,
                           dependent_composites.size());
}

std::vector<std::shared_ptr<CompositeTransaction>> CrossChainMyself::GetDependentCompositeTxs(MySelfTransaction* tx) {
    std::unordered_set<size_t> composite_ids;
    std::vector<std::shared_ptr<CompositeTransaction>> result;
    
    // 遍历读集，查找写入者所属的复合事务体
    for (const auto& get_tuple : tx->tuples_get) {
        if (get_tuple.writer_tx_id == 0) continue;  // 跳过默认值
        
        auto guard = Guard{composite_lock};
        // 遍历所有复合事务体，查找包含写入者的事务体
        for (const auto& [id, composite] : composite_txs) {
            for (size_t tx_id : composite->tx_list) {
                if (tx_id == get_tuple.writer_tx_id) {
                    composite_ids.insert(id);
                    break;
                }
            }
        }
    }
    
    // 将找到的复合事务体添加到结果中
    for (size_t id : composite_ids) {
        result.push_back(composite_txs[id]);
    }
    
    return result;
}

// MySelfExecutor实现
MySelfExecutor::MySelfExecutor(CrossChainMyself& protocol)
    : protocol(protocol)
    , workload(protocol.workload)
    , table(protocol.table)
    , statistics(protocol.statistics)
    , last_executed(protocol.last_executed)
    , last_finalized(protocol.last_finalized)
    , stop_flag(protocol.stop_flag)
    , current_block_id(protocol.current_block_id)
    , block_paused(protocol.block_paused)
    , block_mutex(protocol.block_mutex)
    , block_cv(protocol.block_cv)
    , txs_in_current_block(protocol.txs_in_current_block)
    , cross_chain_ratio(protocol.cross_chain_ratio)
    , stop_barrier(protocol.stop_barrier)
{
}

void MySelfExecutor::Generate() {
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
    
    // 增加区块内交易计数，并检查是否超过限制
    size_t block_tx_count = txs_in_current_block.fetch_add(1, std::memory_order_acq_rel);
    
    // 如果已经达到区块交易上限，则减回去，不生成新交易
    if (block_tx_count >= protocol.txs_per_block) {
        txs_in_current_block.fetch_sub(1, std::memory_order_acq_rel);
        return;
    }
    
    // 生成新的交易ID和区块ID
    auto id = last_executed.fetch_add(1);
    auto block_id = current_block_id.load();
    
    // 随机决定交易类型，按照指定比例生成跨链交易
    auto type = (dist(rng) < cross_chain_ratio) ? 
               MySelfTxType::CROSS_CHAIN : MySelfTxType::REGULAR;
    
    // 在锁保护下更新区块统计信息
    {
        std::lock_guard<std::mutex> lock(protocol.block_records_mutex);
        // 查找或创建当前区块的记录
        auto block_record_it = std::find_if(protocol.block_records.begin(), protocol.block_records.end(),
            [block_id](const auto& record) { return record.block_id == block_id; });
            
        if (block_record_it == protocol.block_records.end()) {
            // 为当前区块创建新记录
            protocol.block_records.push_back(BlockCompositeTxs{
                .block_id = block_id,
                .composites = {},
                .dependency_graph = "",
                .block_start_time = steady_clock::now(),
                .block_end_time = steady_clock::now(),
                .total_memory_usage = 0,
                .tx_count = 1,  // 初始化为1，因为这是第一笔交易
                .cross_chain_tx_count = static_cast<size_t>(type == MySelfTxType::CROSS_CHAIN ? 1 : 0),
                .regular_tx_count = static_cast<size_t>(type == MySelfTxType::REGULAR ? 1 : 0)
            });
        } else {
            // 更新现有记录
            block_record_it->tx_count++;
            if (type == MySelfTxType::CROSS_CHAIN) {
                block_record_it->cross_chain_tx_count++;
            } else {
                block_record_it->regular_tx_count++;
            }
            
            // 打印当前区块统计信息
            LOG(INFO) << fmt::format("区块 {} 当前统计:\n - 总交易数: {}\n - 跨链交易数: {}\n - 普通交易数: {}\n - 区块内交易计数: {}", 
                block_id,
                block_record_it->tx_count,
                block_record_it->cross_chain_tx_count,
                block_record_it->regular_tx_count,
                block_tx_count + 1);
        }
    }
    
    // 创建新的交易
    tx = std::make_unique<MySelfTransaction>(workload.Next(), id, block_id, type);
    tx->start_time = steady_clock::now();  // 记录开始时间
    tx->berun_flag.store(true);
    
    // 记录新事务
    statistics.JournalTransaction();
    
    LOG(INFO) << fmt::format("生成交易 [区块{}] ID={} 类型={} (区块内第 {} 笔)", 
                          block_id, id, 
                          type == MySelfTxType::REGULAR ? "普通" : "跨链",
                          block_tx_count + 1);
    
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
            LOG(INFO) << "交易 " << tx->id << " break";
            tx->Break();
        }
        LOG(INFO) << "交易 " << tx->id <<
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
        for (auto it = tx->tuples_put.rbegin(); it != tx->tuples_put.rend(); ++it) {
            if (it->key != _key) { continue; }
            LOG(INFO) << "交易 " << tx->id << " 在本地写入缓存中找到键 " << KeyHasher()(_key) % 1000;
            return it->value;
        }
        
        // 再检查本地读取缓存
        for (auto& tup: tx->tuples_get) {
            if (tup.key != _key) { continue; }
            LOG(INFO) << "交易 " << tx->id << " 在本地读取缓存中找到键 " << KeyHasher()(_key) % 1000;
            return tup.value;
        }
        
        // 从全局存储中获取
        LOG(INFO) << "交易 " << tx->id << " " << 
            " read(" << tx->tuples_get.size() << ")" << 
            " key(" << KeyHasher()(_key) % 1000 << ")";
        table.Get(tx.get(), _key, value, version, writer_tx_id);
        tx->tuples_get.push_back({
            .key = _key,
            .value = value,
            .version = version,
            .writer_tx_id = writer_tx_id,
            .tuples_put_len = tx->tuples_put.size(),
            .checkpoint_id = tx->MakeCheckpoint()
        });
        
        // 检查是否需要中断执行
        if (tx->HasWAR()) {
            LOG(INFO) << "交易 " << tx->id << " break";
            tx->Break();
        }
        return value;
    });
    
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
    
    // 将交易添加到复合事务体中
    protocol.AddTransactionToComposite(tx.get());
}

void MySelfExecutor::ReExecute() {
    LOG(INFO) << fmt::format("重新执行交易 {}", tx->id);
    
    std::vector<K> rerun_keys{};
    {
        auto guard = Guard{tx->rerun_keys_mu};
        std::swap(tx->rerun_keys, rerun_keys);
    }
    
    statistics.JournalAbort();
    
    // 增加中止交易计数
    {
        std::lock_guard<std::mutex> lock(protocol.block_records_mutex);
        auto block_record_it = std::find_if(protocol.block_records.begin(), protocol.block_records.end(),
            [this](const auto& record) { return record.block_id == tx->block_id; });
            
        if (block_record_it != protocol.block_records.end()) {
            block_record_it->aborted_tx_count++;
            LOG(INFO) << fmt::format("区块 {} 中止交易数增加到 {}", 
                                   tx->block_id, 
                                   block_record_it->aborted_tx_count);
        }
    }
    
    auto back_to = ~size_t{0};
    
    // 找到最早的检查点
    for (auto& key : rerun_keys) {
        for (size_t i = 0; i < tx->tuples_get.size(); ++i) {
            if (tx->tuples_get[i].key != key) continue;
            back_to = std::min(i, back_to);
            break;
        }
    }
    
    if (back_to == ~size_t{0}) {
        LOG(INFO) << fmt::format("交易 {} 不需要回滚", tx->id);
        tx->Execute();
        return;
    }
    
    // 需要回滚
    auto& tup = tx->tuples_get[back_to];
    tx->ApplyCheckpoint(tup.checkpoint_id);
    
    // 回滚写入
    for (size_t i = tup.tuples_put_len; i < tx->tuples_put.size(); ++i) {
        if (tx->tuples_put[i].is_committed) {
            table.RegretPut(tx.get(), tx->tuples_put[i].key);
        }
    }
    
    // 移除读记录
    for (size_t i = back_to; i < tx->tuples_get.size(); ++i) {
        table.RegretGet(tx.get(), tx->tuples_get[i].key, tx->tuples_get[i].version);
    }
    
    tx->tuples_put.resize(tup.tuples_put_len);
    tx->tuples_get.resize(back_to);
    
    tx->Execute();
    statistics.JournalExecute();
    statistics.JournalOperations(tx->CountOperations());
    
    // 提交写集
    for (auto& entry : tx->tuples_put) {
        if (tx->HasWAR()) break;
        if (entry.is_committed) continue;
        table.Put(tx.get(), entry.key, entry.value);
        entry.is_committed = true;
    }
}

void MySelfExecutor::Finalize() {
    if (!tx) return;
    
    // 记录完成时间
    auto finish_time = steady_clock::now();
    
    // 如果是跨链交易，完成时间需要加2秒
    if (tx->GetType() == MySelfTxType::CROSS_CHAIN) {
        finish_time += milliseconds(2200);
    }
    
    // 更新最新完成时间
    {
        std::lock_guard<std::mutex> lock(protocol.latency_log_mutex);
        if (finish_time > protocol.latest_finish_time) {
            protocol.latest_finish_time = finish_time;
        }
        // 如果是跨链交易，更新最后一笔跨链交易的完成时间
        if (tx->GetType() == MySelfTxType::CROSS_CHAIN && finish_time > protocol.latest_cross_chain_finish_time) {
            protocol.latest_cross_chain_finish_time = finish_time;
        }
    }
    
    // 如果是第一笔交易，记录开始时间
    if (!protocol.first_tx_started.exchange(true)) {
        protocol.first_tx_time = tx->start_time;
        LOG(INFO) << fmt::format("第一笔交易开始时间: {} 微秒", 
            duration_cast<microseconds>(tx->start_time.time_since_epoch()).count());
    }
    
    // 增加完成交易计数
    size_t completed = protocol.completed_tx_count.fetch_add(1) + 1;
    
    // 只记录前100笔交易数据到数组中
    if (completed <= 100) {
        std::lock_guard<std::mutex> lock(protocol.latency_log_mutex);
        // 将交易记录添加到数组
        protocol.tx_records.push_back(TransactionRecord{
            .tx_id = tx->id,
            .start_time = tx->start_time,
            .finish_time = finish_time,
            .type = tx->GetType()
        });
        
        if (completed == 100) {
            LOG(INFO) << "已记录100笔交易数据，将在测试结束后写入日志文件";
        }
    }
    
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
    auto latency = duration_cast<microseconds>(finish_time - tx->start_time).count();
    statistics.JournalCommit(latency);
    statistics.JournalMemory(tx->mm_count);
    
    // 交易已完成，清空交易指针
    tx = nullptr;
}

void MySelfExecutor::Run() {
    while (!stop_flag.load()) {
        // 检查区块暂停状态
        {
            std::unique_lock<std::mutex> lock(block_mutex);
            while (block_paused.load() && !stop_flag.load()) {
                // 如果当前有正在处理的事务，先完成处理
                if (tx != nullptr) {
                    if (tx->HasWAR()) {
                        ReExecute();
                    } else if (last_finalized.load() + 1 == tx->id) {
                        Finalize();
                    }
                }
                block_cv.wait(lock);
            }
            if (stop_flag.load()) break;
        }
        
        if (tx == nullptr) {
            Generate();
        }
        
        if (tx != nullptr) {  // 确保 tx 不为空再访问
            if (tx->HasWAR()) {
                ReExecute();
            }
            else if (last_finalized.load() + 1 == tx->id && !tx->HasWAR()) {
                Finalize();
            }
        }
    }
    
    // 确保最后一个事务被正确处理
    if (tx != nullptr) {
        if (tx->HasWAR()) {
            ReExecute();
        } else if (last_finalized.load() + 1 == tx->id) {
            Finalize();
        }
    }
    
    // 使用 SimpleBarrier 等待所有线程
    stop_barrier.arrive_and_wait();
}

void CrossChainMyself::BlockControllerTask() {
    LOG(INFO) << "区块管理线程启动，每个区块大小: " << txs_per_block << " 交易，区块间隔: " 
              << block_interval_seconds << "秒";
    
    // 创建第一个区块的记录
    {
        auto guard = Guard{composite_lock};
        block_records.push_back(BlockCompositeTxs{
            .block_id = current_block_id.load(),
            .composites = {},
            .dependency_graph = "",
            .block_start_time = steady_clock::now(),
            .block_end_time = steady_clock::now(),
            .total_memory_usage = 0,
            .tx_count = 0,
            .cross_chain_tx_count = 0,
            .regular_tx_count = 0
        });
    }
    
    while (!stop_flag.load()) {
        size_t current_tx_count = txs_in_current_block.load();
        
        // 检查是否需要暂停事务生成
        if (current_tx_count >= txs_per_block) {
            LOG(INFO) << fmt::format("区块 {} 已达到 {} 笔交易(当前: {})，准备切换新区块", 
                current_block_id.load(), txs_per_block, current_tx_count);
            
            // 标记暂停状态
            block_paused.store(true);
            
            // 等待区块间隔时间
            std::this_thread::sleep_for(duration<double>(block_interval_seconds));
            
            // 处理区块完成事件
            OnBlockComplete(current_block_id.load());
            
            // 递增区块ID
            size_t new_block_id = current_block_id.fetch_add(1) + 1;
            
            // 创建新区块的记录
            {
                auto guard = Guard{composite_lock};
                block_records.push_back(BlockCompositeTxs{
                    .block_id = new_block_id,
                    .composites = {},
                    .dependency_graph = "",
                    .block_start_time = steady_clock::now(),
                    .block_end_time = steady_clock::now(),
                    .total_memory_usage = 0,
                    .tx_count = 0,
                    .cross_chain_tx_count = 0,
                    .regular_tx_count = 0
                });
            }
            
            // 重置区块内交易计数
            txs_in_current_block.store(0, std::memory_order_release);
            
            // 恢复生成
            block_paused.store(false);
            
            // 通知所有等待的执行器
            block_cv.notify_all();
            
            LOG(INFO) << fmt::format("开始生成区块 {} 的交易，当前交易计数: {}", 
                new_block_id, txs_in_current_block.load());
        }
        
        // 控制线程休眠，降低CPU使用率
        std::this_thread::sleep_for(milliseconds(10));
    }
    
    LOG(INFO) << "区块管理线程停止";
}

void CrossChainMyself::OnBlockComplete(size_t block_id) {
    LOG(INFO) << fmt::format("区块 {} 所有交易执行完成，记录复合事务体信息...", block_id);
    
    auto guard = Guard{composite_lock};
    
    // 查找现有记录
    auto block_record_it = std::find_if(block_records.begin(), block_records.end(),
        [block_id](const auto& record) { return record.block_id == block_id; });
        
    if (block_record_it == block_records.end()) {
        LOG(ERROR) << fmt::format("区块 {} 记录未找到", block_id);
        return;
    }
    
    // 更新区块结束时间
    block_record_it->block_end_time = steady_clock::now();
    auto duration = duration_cast<milliseconds>(block_record_it->block_end_time - block_record_it->block_start_time);
    LOG(INFO) << fmt::format("区块 {} 执行耗时: {} 毫秒", block_id, duration.count());
    
    size_t total_txs = 0;
    size_t cross_chain_composites = 0;
    
    // 收集该区块相关的复合事务体
    for (const auto& [id, composite] : composite_txs) {
        bool has_tx_in_block = false;
        for (size_t tx_id : composite->tx_list) {
            // 检查交易ID是否属于当前区块
            // 假设每个区块的交易ID是连续的
            size_t tx_block_id = (tx_id - 1) / txs_per_block + 1;
            if (tx_block_id == block_id) {
                has_tx_in_block = true;
                total_txs += composite->tx_list.size();
                if (composite->is_cross_chain) {
                    cross_chain_composites++;
                }
                break;
            }
        }
        
        if (has_tx_in_block) {
            block_record_it->composites.push_back(composite);
            // 累加内存使用量
            // 计算复合事务体的基本大小
            size_t composite_size = sizeof(CompositeTransaction);
            // 添加交易列表的内存
            composite_size += composite->tx_list.size() * sizeof(size_t);
            // 添加写集的内存
            composite_size += composite->write_set.size() * sizeof(WriteSetEntry);
            // 添加依赖集的内存
            composite_size += composite->dependent_composites.size() * sizeof(size_t);
            
            block_record_it->total_memory_usage += composite_size;
            
            LOG(INFO) << fmt::format("复合事务体 {} 添加到区块 {}: {} 个交易, {} 个写入, {} 个依赖, 内存占用 {:.2f}KB",
                composite->composite_id,
                block_id,
                composite->tx_list.size(),
                composite->write_set.size(),
                composite->dependent_composites.size(),
                composite_size / 1024.0);
        }
    }
    
    // 生成依赖图
    std::stringstream graph;
    graph << "区块 " << block_id << " 的复合事务体依赖关系:\n";
    for (const auto& composite : block_record_it->composites) {
        graph << fmt::format("复合事务体 {} ({}): [", 
                           composite->composite_id,
                           composite->is_cross_chain ? "跨链" : "普通");
        
        // 列出包含的交易
        for (size_t i = 0; i < composite->tx_list.size(); ++i) {
            if (i > 0) graph << ", ";
            graph << composite->tx_list[i];
        }
        graph << "] -> {";
        
        // 列出依赖的复合事务体
        size_t i = 0;
        for (size_t dep_id : composite->dependent_composites) {
            if (i++ > 0) graph << ", ";
            graph << dep_id;
        }
        graph << "}\n";
    }
    block_record_it->dependency_graph = graph.str();
    
    LOG(INFO) << fmt::format("区块 {} 统计信息:\n - 复合事务体总数: {}\n - 跨链复合事务体数: {}\n - 总交易数: {}\n - 中止交易数: {}\n - 总内存占用: {:.2f}KB\n - 依赖图:\n{}", 
        block_id, 
        block_record_it->composites.size(),
        cross_chain_composites,
        total_txs,
        block_record_it->aborted_tx_count,
        block_record_it->total_memory_usage / 1024.0,
        block_record_it->dependency_graph);
}

std::string CrossChainMyself::GetBlockCompositeTxsInfo(size_t block_id) const {
    for (const auto& record : block_records) {
        if (record.block_id == block_id) {
            std::stringstream ss;
            ss << fmt::format("区块 {} 信息:\n", block_id);
            ss << fmt::format("执行时间: {:.2f} 秒\n", 
                           duration_cast<duration<double>>(record.block_end_time - record.block_start_time).count());
            ss << fmt::format("交易数量: {}\n", record.tx_count);
            ss << fmt::format("跨链交易数量: {}\n", record.cross_chain_tx_count);
            ss << fmt::format("普通交易数量: {}\n", record.regular_tx_count);
            ss << fmt::format("中止交易数量: {}\n", record.aborted_tx_count);
            ss << fmt::format("复合事务体数量: {}\n", record.composites.size());
            ss << fmt::format("复合事务体总内存占用: {:.2f} KB\n", record.total_memory_usage / 1024.0);
            ss << record.dependency_graph;
            return ss.str();
        }
    }
    return fmt::format("未找到区块 {} 的记录", block_id);
}

std::string CrossChainMyself::GetAllBlocksCompositeTxsInfo() const {
    std::stringstream ss;
    ss << "所有区块的复合事务体信息:\n";
    ss << "========================\n";
    
    for (const auto& record : block_records) {
        ss << GetBlockCompositeTxsInfo(record.block_id);
        ss << "------------------------\n";
    }
    
    return ss.str();
}

} // namespace spectrum 
