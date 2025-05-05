#include <spectrum/protocol/block_mvcc_crosschain.hpp>
#include <spectrum/common/lock-util.hpp>
#include <spectrum/common/thread-util.hpp>
#include <fmt/core.h>
#include <functional>
#include <thread>
#include <chrono>
#include <glog/logging.h>
#include <ranges>
#include <algorithm>
#include <sstream>
#include <fstream>
#include <filesystem>

namespace spectrum {

using namespace std::chrono;
// 使用完整名称而不是别名
// using Guard = spectrum::Guard; // 这行代码有问题

// 添加文件输出流
static std::ofstream tx_log_file;

// ======================= BlockMVCCTransaction 实现 =======================

BlockMVCCTransaction::BlockMVCCTransaction(Transaction&& inner, size_t id, size_t block_id, BlockTxType type)
    : Transaction(std::move(inner))
    , id(id)
    , block_id(block_id)
    , type(type)
    , start_time(steady_clock::now())
{
    LOG(INFO) << fmt::format("创建交易 [区块{}] ID={} 类型={}", 
                          block_id, id, type == BlockTxType::REGULAR ? "普通" : "跨链");
}

bool BlockMVCCTransaction::HasWAR() {
    auto guard = spectrum::Guard{rerun_keys_mu};
    return rerun_keys.size() != 0;
}

void BlockMVCCTransaction::SetWAR(const K& key, size_t cause_id) {
    auto guard = spectrum::Guard{rerun_keys_mu};
    rerun_keys.push_back(key);
    should_wait = std::max(should_wait, cause_id);
}

void BlockMVCCTransaction::SetReadyTime() {
    // 设置跨链交易完成时间为当前时间+0.5秒
    ready_time = steady_clock::now() + milliseconds(1150);
    LOG(INFO) << fmt::format("跨链交易 [区块{}] ID={} 设置延迟完成时间，将在0.5秒后完成", 
                          block_id, id);
}

bool BlockMVCCTransaction::IsReadyForFinalize() const {
    // 如果不是跨链交易，或者已经完成延迟等待，则直接返回true
    if (type != BlockTxType::CROSS_CHAIN || delay_completed) {
        return true;
    }
    
    // 检查是否已经到达完成时间
    bool ready = steady_clock::now() >= ready_time;
    if (ready) {
        LOG(INFO) << fmt::format("跨链交易 [区块{}] ID={} 已到达延迟完成时间，可以finalize", 
                              block_id, id);
    }
    return ready;
}

// ======================= BlockMVCCTable 实现 =======================

BlockMVCCTable::BlockMVCCTable(size_t partitions)
    : table(partitions)
{
}

void BlockMVCCTable::Get(BlockMVCCTransaction* tx, const K& k, evmc::bytes32& v, size_t& version) {
    table.Put(k, [&](BlockMVCCVersionList& _v) {
        auto rit = _v.entries.rbegin();
        auto end = _v.entries.rend();
        while (rit != end) {
            if (rit->version > tx->id) {
                ++rit; continue;
            }
            v = rit->value;
            version = rit->version;
            rit->readers.insert(tx);
            LOG(INFO) << tx->id << "(" << tx << ")" << " read " << spectrum::KeyHasher()(k) % 1000 << " version " << rit->version;
            return;
        }
        version = 0;
        LOG(INFO) << tx->id << "(" << tx << ")" << " read " << spectrum::KeyHasher()(k) % 1000 << " version 0";
        _v.readers_default.insert(tx);
        v = evmc::bytes32{0};
    });
}

void BlockMVCCTable::Put(BlockMVCCTransaction* tx, const K& k, const evmc::bytes32& v) {
    CHECK(tx->id > 0) << "we reserve version(0) for default value";
    LOG(INFO) << tx->id << "(" << tx << ")" << " write " << spectrum::KeyHasher()(k) % 1000;
    table.Put(k, [&](BlockMVCCVersionList& _v) {
        auto rit = _v.entries.rbegin();
        auto end = _v.entries.rend();
        // search from insertion position
        while (rit != end) {
            if (rit->version > tx->id) {
                ++rit; continue;
            }
            // abort transactions that read outdated keys
            for (auto _tx: rit->readers) {
                LOG(INFO) << spectrum::KeyHasher()(k) % 1000 << " has read dependency " << "(" << _tx << ")";
                if (_tx->id > tx->id) {
                    LOG(INFO) << tx->id << " abort " << _tx->id;
                    _tx->SetWAR(k, tx->id);
                    // 这里不能直接访问block_records，因为BlockMVCCTable不持有BlockMVCCCrossChain的引用
                    // 我们需要在交易对象中标记它已被中止
                    _tx->is_aborted = true;
                }
            }
            break;
        }
        for (auto _tx: _v.readers_default) {
            LOG(INFO) << spectrum::KeyHasher()(k) % 1000 << " has read dependency " << "(" << _tx << ")";
            if (_tx->id > tx->id) {
                LOG(INFO) << tx->id << " abort " << _tx->id;
                _tx->SetWAR(k, tx->id);
                _tx->is_aborted = true;
            }
        }
        // handle duplicated write on the same key
        if (rit != end && rit->version == tx->id) {
            rit->value = v;
            return;
        }
        // insert an entry
        _v.entries.insert(rit.base(), BlockMVCCEntry{
            .value = v,
            .version = tx->id,
            .readers = std::unordered_set<BlockMVCCTransaction*>()
        });
    });
}

void BlockMVCCTable::RegretGet(BlockMVCCTransaction* tx, const K& k, size_t version) {
    LOG(INFO) << "remove read record " << tx->id << "(" << tx << ")" << " from " << spectrum::KeyHasher()(k) % 1000;
    table.Put(k, [&](BlockMVCCVersionList& _v) {
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

void BlockMVCCTable::RegretPut(BlockMVCCTransaction* tx, const K& k) {
    LOG(INFO) << "remove write record " << tx->id << "(" << tx << ")" << " from " << spectrum::KeyHasher()(k) % 1000;
    table.Put(k, [&](BlockMVCCVersionList& _v) {
        auto vit = _v.entries.begin();
        auto end = _v.entries.end();
        while (vit != end) {
            if (vit->version != tx->id) {
                ++vit; continue;
            }
            // abort transactions that read from current transaction
            for (auto _tx: vit->readers) {
                LOG(INFO) << spectrum::KeyHasher()(k) % 1000 << " has read dependency " << "(" << _tx << ")";
                LOG(INFO) << tx->id << " abort " << _tx->id;
                _tx->SetWAR(k, tx->id);
            }
            break;
        }
        if (vit != end) { _v.entries.erase(vit); }
    });
}

void BlockMVCCTable::ClearGet(BlockMVCCTransaction* tx, const K& k, size_t version) {
    LOG(INFO) << "remove read record " << tx->id << "(" << tx << ")" << " from " << spectrum::KeyHasher()(k) % 1000;
    table.Put(k, [&](BlockMVCCVersionList& _v) {
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

void BlockMVCCTable::ClearPut(BlockMVCCTransaction* tx, const K& k) {
    LOG(INFO) << "remove write record before " << tx->id << "(" << tx << ")" << " from " << spectrum::KeyHasher()(k) % 1000;
    table.Put(k, [&](BlockMVCCVersionList& _v) {
        while (_v.entries.size() && _v.entries.front().version < tx->id) {
            _v.entries.pop_front();
        }
    });
}

// ======================= BlockMVCCCrossChain 实现 =======================

BlockMVCCCrossChain::BlockMVCCCrossChain(
    Workload& workload, 
    Statistics& statistics, 
    size_t num_executors,
    size_t table_partitions,
    EVMType evm_type,
    size_t txs_per_block,
    double block_interval_seconds,
    double cross_chain_ratio)
    : workload(workload)
    , statistics(statistics)
    , num_executors(num_executors)
    , txs_per_block(txs_per_block)
    , block_interval_seconds(block_interval_seconds)
    , cross_chain_ratio(cross_chain_ratio)
    , table(table_partitions)
    , stop_barrier(num_executors)
{
    LOG(INFO) << fmt::format("区块MVCC跨链协议(执行器数量={}, 表分区={}, EVM类型={}, 每区块交易数={}, 区块间隔={}秒, 跨链比例={})",
                          num_executors, table_partitions, 
                          static_cast<int>(evm_type), txs_per_block, 
                          block_interval_seconds, cross_chain_ratio);
                          
    workload.SetEVMType(evm_type);
}

// 区块控制器任务函数
void BlockMVCCCrossChain::BlockControllerTask() {
    LOG(INFO) << "区块控制任务启动...";
    
    while (!stop_flag.load()) {
        auto block_id = current_block_id.load();
        auto block_start_time = steady_clock::now();
        
        LOG(INFO) << fmt::format("开始生成区块 {} (当前交易数: {})", block_id, txs_in_current_block.load());
        
        // 重置区块内交易计数
        txs_in_current_block.store(0);
        
        // 恢复交易生成
        block_paused.store(false);
        block_cv.notify_all();
        
        // 等待区块间隔时间
        std::this_thread::sleep_for(microseconds(static_cast<long long>(block_interval_seconds * 1000000)));
        
        // 暂停新交易生成
        block_paused.store(true);
        
        // 等待当前区块内所有交易完成
        LOG(INFO) << fmt::format("等待区块 {} 内所有交易完成...", block_id);
        
        size_t wait_count = 0;
        while (true) {
            size_t completed_count = 0;
            {
                std::lock_guard<std::mutex> lock(transactions_mutex);
                for (const auto& tx : active_transactions) {
                    if (tx->block_id == block_id) {
                        completed_count++;
                    }
                }
            }
            
            if (completed_count == 0) {
                break;
            }
            
            if (++wait_count % 100 == 0) {  // 每100次循环输出一次日志
                LOG(INFO) << fmt::format("区块 {} 还有 {} 笔交易未完成...", block_id, completed_count);
            }
            
            std::this_thread::sleep_for(milliseconds(10));
            
            // 如果收到停止信号，提前退出
            if (stop_flag.load()) {
                LOG(INFO) << "收到停止信号，区块控制任务退出";
                return;
            }
        }
        
        LOG(INFO) << fmt::format("区块 {} 所有交易已完成，开始处理区块完成事件", block_id);
        
        // 处理区块完成
        OnBlockComplete(block_id);
        
        // 增加区块ID
        current_block_id.fetch_add(1);
        
        LOG(INFO) << fmt::format("区块 {} 已完成，耗时: {:.2f}秒", 
                              block_id,
                              duration_cast<duration<double>>(steady_clock::now() - block_start_time).count());
    }
    
    LOG(INFO) << "区块控制任务正常停止";
}

void BlockMVCCCrossChain::OnBlockComplete(size_t block_id) {
    LOG(INFO) << fmt::format("区块 {} 所有交易执行完成，开始记录区块信息...", block_id);
    
    // 保留写入时的锁保护
    std::lock_guard<std::mutex> lock(block_records_mutex);
    
    // 查找当前区块的记录
    auto block_record_it = std::find_if(block_records.begin(), block_records.end(),
        [block_id](const auto& record) { return record.block_id == block_id; });
        
    if (block_record_it == block_records.end()) {
        // 如果没有记录，创建一个空记录
        LOG(INFO) << fmt::format("区块 {} 没有交易记录", block_id);
        return;
    }
    
    LOG(INFO) << fmt::format("区块 {} 统计完成: 总交易数={}, 跨链交易数={}, 普通交易数={}", 
                          block_id, block_record_it->tx_count, block_record_it->cross_chain_tx_count, 
                          block_record_it->regular_tx_count);
}

std::string BlockMVCCCrossChain::GetBlockInfo(size_t block_id) const {
    // 因为只在测试完成后调用，所以不需要加锁
    for (const auto& record : block_records) {
        if (record.block_id == block_id) {
            std::stringstream ss;
            ss << fmt::format("区块 {} 信息:\n", block_id);
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
            return ss.str();
        }
    }
    
    return fmt::format("未找到区块 {} 的记录\n", block_id);
}

std::string BlockMVCCCrossChain::GetAllBlocksInfo() const {
    // 因为只在测试完成后调用，所以不需要加锁
    std::stringstream ss;
    ss << "所有区块信息:\n";
    ss << "========================\n";
    
    if (block_records.empty()) {
        ss << "没有区块记录\n";
        return ss.str();
    }
    
    for (const auto& record : block_records) {
        ss << GetBlockInfo(record.block_id);
        ss << "------------------------\n";
    }
    
    return ss.str();
}

void BlockMVCCCrossChain::Start() {
    LOG(INFO) << "区块MVCC跨链协议启动...";
    
    // 创建日志目录
    std::filesystem::path log_path = "./logs";
    if (!std::filesystem::exists(log_path)) {
        std::filesystem::create_directory(log_path);
    }
    
    // 打开日志文件
    latency_log.open("./logs/latency_block_spe.log", std::ios::out | std::ios::app);
    if (!latency_log.is_open()) {
        LOG(ERROR) << "无法创建日志文件 ./logs/latency_block_spe.log";
    } else {
        LOG(INFO) << "成功创建日志文件 ./logs/latency_block_spe.log";
        // 写入表头
        latency_log << "交易ID,开始时间,完成时间,延迟(微秒),类型\n";
    }
    
    // 初始化时间点
    first_tx_time = steady_clock::now();
    latest_finish_time = first_tx_time;
    latest_cross_chain_finish_time = first_tx_time;
    
    stop_flag.store(false);
    for (size_t i = 0; i < num_executors; ++i) {
        executors.push_back(std::thread([this]{
            std::make_unique<BlockMVCCExecutor>(*this)->Run();
        }));
        // 使用正确的函数签名调用 PinRoundRobin
        PinRoundRobin(executors[i], static_cast<unsigned>(i));
    }
    
    // 启动区块管理线程
    block_manager = std::thread([this]{ BlockControllerTask(); });
}

void BlockMVCCCrossChain::Stop() {
    LOG(INFO) << "区块MVCC跨链协议停止...";
    stop_flag.store(true);
    
    // 等待所有线程结束
    for (auto& x: executors) { x.join(); }
    if (block_manager.joinable()) {
        block_manager.join();
    }
    
    // 添加内存屏障,确保所有写入都已完成
    std::atomic_thread_fence(std::memory_order_seq_cst);
    
    // 后续的统计信息输出和日志记录不需要加锁
    // 如果已经完成了100笔交易，输出统计信息
    if (completed_tx_count.load() >= 100) {
        latency_log << "\n=== 执行统计 ===\n";
        latency_log << "第一笔交易开始时间: " 
                   << duration_cast<microseconds>(first_tx_time.time_since_epoch()).count() 
                   << " 微秒\n";
        
        auto last_tx_time = std::max(latest_finish_time, latest_cross_chain_finish_time);
        latency_log << "最后一笔交易完成时间: " 
                   << duration_cast<microseconds>(last_tx_time.time_since_epoch()).count() 
                   << " 微秒\n";
        
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
    }
    
    // 关闭日志文件
    if (latency_log.is_open()) {
        latency_log.close();
    }
}

// ======================= BlockMVCCExecutor 实现 =======================

BlockMVCCExecutor::BlockMVCCExecutor(BlockMVCCCrossChain& protocol)
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

void BlockMVCCExecutor::Generate() {
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
    
    // 增加区块内交易计数
    size_t block_tx_count = txs_in_current_block.fetch_add(1);
    
    // 如果已经达到区块交易上限，则减回去，不生成新交易
    if (block_tx_count >= protocol.txs_per_block) {
        txs_in_current_block.fetch_sub(1);
        return;
    }
    
    // 生成新的交易ID和区块ID
    auto id = last_executed.fetch_add(1);
    auto block_id = current_block_id.load();
    
    // 随机决定交易类型，按照指定比例生成跨链交易
    auto type = (dist(rng) < cross_chain_ratio) ? 
               BlockTxType::CROSS_CHAIN : BlockTxType::REGULAR;
    
    // 创建新的交易
    tx = std::make_unique<BlockMVCCTransaction>(workload.Next(), id, block_id, type);
    tx->start_time = steady_clock::now();
    tx->berun_flag.store(true);
    tx->is_aborted.store(false);  // 重置中止标记
    
    // 记录新事务
    statistics.JournalTransaction();
    
    // 在锁保护下更新区块统计信息
    {
        std::lock_guard<std::mutex> lock(protocol.block_records_mutex);
        // 查找或创建当前区块的记录
        auto block_record_it = std::find_if(protocol.block_records.begin(), protocol.block_records.end(),
            [block_id](const auto& record) { return record.block_id == block_id; });
            
        if (block_record_it == protocol.block_records.end()) {
            // 为当前区块创建新记录
            protocol.block_records.push_back(BlockMVCCCrossChain::BlockInfo{.block_id = block_id});
            block_record_it = protocol.block_records.end() - 1;
        }
        
        // 更新统计信息
        block_record_it->tx_count++;
        if (type == BlockTxType::CROSS_CHAIN) {
            block_record_it->cross_chain_tx_count++;
        } else {
            block_record_it->regular_tx_count++;
        }
    }
    
    LOG(INFO) << fmt::format("生成交易 [区块{}] ID={} 类型={} (区块内第 {} 笔)", 
                          block_id, id, 
                          type == BlockTxType::REGULAR ? "普通" : "跨链",
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
        
        // 先检查本地写入缓存
        for (auto& tup: tx->tuples_put) {
            if (tup.key != _key) { continue; }
            LOG(INFO) << "交易 " << tx->id << " 在写缓存中找到键 " << spectrum::KeyHasher()(_key) % 1000;
            return tup.value;
        }
        
        // 再检查本地读取缓存
        for (auto& tup: tx->tuples_get) {
            if (tup.key != _key) { continue; }
            LOG(INFO) << "交易 " << tx->id << " 在读缓存中找到键 " << spectrum::KeyHasher()(_key) % 1000;
            return tup.value;
        }
        
        // 从全局存储中获取
        LOG(INFO) << "交易 " << tx->id << " " << 
            " read(" << tx->tuples_get.size() << ")" << 
            " key(" << spectrum::KeyHasher()(_key) % 1000 << ")";
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
}

void BlockMVCCExecutor::ReExecute() {
    LOG(INFO) << fmt::format("重新执行交易 [区块{}] ID={}", tx->block_id, tx->id);
    
    // 获取当前重新执行的键
    std::vector<K> rerun_keys{};
    {
        auto guard = spectrum::Guard{tx->rerun_keys_mu}; 
        std::swap(tx->rerun_keys, rerun_keys);
    }
    
    // 更新区块统计信息 - 记录中止交易
    {
        std::lock_guard<std::mutex> lock(protocol.block_records_mutex);
        auto block_record_it = std::find_if(protocol.block_records.begin(), protocol.block_records.end(),
            [this](const auto& record) { return record.block_id == tx->block_id; });
            
        if (block_record_it != protocol.block_records.end() && !rerun_keys.empty()) {
            block_record_it->aborted_tx_count++;
            LOG(INFO) << fmt::format("交易 [区块{}] ID={} 发生WAR冲突，计入中止统计", tx->block_id, tx->id);
        }
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
        LOG(INFO) << "交易 " << tx->id << " 不需要回滚";
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
    
    LOG(INFO) << "交易 " << tx->id <<
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

void BlockMVCCExecutor::Finalize() {
    if (!tx) return;
    
    LOG(INFO) << fmt::format("完成交易 [区块{}] ID={}", tx->block_id, tx->id);
    
    // 记录完成时间
    auto finish_time = steady_clock::now();
    
    // 更新区块统计信息
    {
        std::lock_guard<std::mutex> lock(protocol.block_records_mutex);
        auto block_record_it = std::find_if(protocol.block_records.begin(), protocol.block_records.end(),
            [this](const auto& record) { return record.block_id == tx->block_id; });
            
        if (block_record_it != protocol.block_records.end()) {
            // 如果交易有WAR冲突，则计入中止交易
            if (tx->HasWAR()) {
                block_record_it->aborted_tx_count++;
                LOG(INFO) << fmt::format("交易 [区块{}] ID={} 因WAR冲突被中止", tx->block_id, tx->id);
            }
        }
    }
    
    // 更新统计信息
    if (tx->id <= 100) {
        std::lock_guard<std::mutex> lock(protocol.latency_log_mutex);
        
        // 记录第一笔交易的开始时间
        if (!protocol.first_tx_started.exchange(true)) {
            protocol.first_tx_time = tx->start_time;
        }
        
        // 更新最新完成时间
        if (tx->type == BlockTxType::CROSS_CHAIN) {
            auto actual_finish = finish_time + milliseconds(1150);  // 加上0.5秒跨链延迟
            protocol.latest_cross_chain_finish_time = std::max(
                protocol.latest_cross_chain_finish_time, actual_finish);
            protocol.latency_log << tx->id << ","
                               << duration_cast<microseconds>(tx->start_time.time_since_epoch()).count() << ","
                               << duration_cast<microseconds>(actual_finish.time_since_epoch()).count() << ","
                               << duration_cast<microseconds>(actual_finish - tx->start_time).count()
                               << ",跨链\n";
        } else {
            protocol.latest_finish_time = std::max(
                protocol.latest_finish_time, finish_time);
            protocol.latency_log << tx->id << ","
                               << duration_cast<microseconds>(tx->start_time.time_since_epoch()).count() << ","
                               << duration_cast<microseconds>(finish_time.time_since_epoch()).count() << ","
                               << duration_cast<microseconds>(finish_time - tx->start_time).count()
                               << ",普通\n";
        }
        
        protocol.completed_tx_count.fetch_add(1);
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
    
    // 计算延迟时间
    auto latency = duration_cast<microseconds>(finish_time - tx->start_time).count();
    
    // 记录统计信息
    statistics.JournalCommit(latency);
    statistics.JournalMemory(tx->mm_count);
    
    // 将交易完成信息写入日志文件
    if (tx_log_file.is_open()) {
        tx_log_file << fmt::format("交易 {} {} 完成: 开始时间={}, 完成时间={}, 延迟={}微秒\n",
            tx->id,
            tx->type == BlockTxType::CROSS_CHAIN ? "(跨链)" : "(普通)",
            std::chrono::duration_cast<std::chrono::milliseconds>(
                tx->start_time.time_since_epoch()).count(),
            std::chrono::duration_cast<std::chrono::milliseconds>(
                finish_time.time_since_epoch()).count(),
            latency
        );
        tx_log_file.flush();
    }
    
    // 交易已完成，清空交易指针
    tx = nullptr;
}

void BlockMVCCExecutor::Run() {
    while (!stop_flag.load()) {
        // 首先生成一个交易
        if (tx == nullptr) {
            Generate();
            
            // 如果没有生成交易（可能是因为当前区块已满或已停止）
            // 则休眠一小段时间后继续尝试
            if (tx == nullptr) {
                std::this_thread::sleep_for(milliseconds(10));
                continue;
            }
        }
        
        if (tx->HasWAR()) {
            // 如果有需要重新执行的键，重新执行以获取正确的结果
            ReExecute();
        }
        else if (last_finalized.load() + 1 == tx->id && !tx->HasWAR()) {
            // 如果上一个交易已经完成，且当前交易不需要重新执行，
            // 对于跨链交易，检查是否已经过了延迟时间
            if (tx->GetType() == BlockTxType::CROSS_CHAIN) {
                // 如果还没有设置过完成时间，则设置完成时间
                if (tx->ready_time == steady_clock::time_point{}) {
                    tx->SetReadyTime();
                }
                
                // 检查是否已经可以完成
                if (!tx->IsReadyForFinalize()) {
                    // 还不能完成，稍后再尝试
                    std::this_thread::sleep_for(milliseconds(10));
                    LOG(INFO) << fmt::format("跨链交易 ID={} 尚未达到延迟完成时间，暂不finalize", tx->id);
                    continue;
                }
                
                // 已经到达延迟完成时间，标记为已完成
                tx->SetDelayCompleted();
                LOG(INFO) << fmt::format("跨链交易 [区块{}] ID={} 已完成0.5秒延迟，可以finalize", 
                                      tx->block_id, tx->id);
            }
            
            // 可以最终提交并准备处理下一个交易
            Finalize();
        } else {
            // 当前交易不能完成，等待其他条件满足
            std::this_thread::sleep_for(milliseconds(1));
        }
    }
    
    // 通知所有执行器已停止
    stop_barrier.arrive_and_wait();
}

} // namespace spectrum
