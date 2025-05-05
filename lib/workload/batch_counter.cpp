#include "batch_counter.hpp"
#include "evmc/evmc.hpp"
#include "spectrum/transaction/evm-hash.hpp"
#include <spectrum/common/hex.hpp>
#include <fmt/core.h>
#include <glog/logging.h>
#include <random>
#include <ctime>

namespace spectrum {

const static char* CODE = 
    #include "../../contracts/simple_counter.bin"
;

BatchCounter::BatchCounter(size_t num_txs, size_t cross_chain_percent): 
    evm_type{EVMType::STRAWMAN},
    num_transactions{num_txs},
    current_count{0},
    cross_chain_ratio{cross_chain_percent}
{
    LOG(INFO) << fmt::format("BatchCounter(num_txs={}, cross_chain_percent={})", num_txs, cross_chain_percent);
    this->code = spectrum::from_hex(std::string{CODE}).value();
    
    // 创建x变量的存储键
    // 在Solidity中，公共状态变量的存储位置是keccak256(abi.encode(0))，其中0是变量的位置
    // 我们用一个简单表示，实际应用需要根据您的合约计算真实的键
    key = evmc::bytes32{};
    key.bytes[0] = 0; // 简化表示，这不是准确的Solidity存储布局
    
    // 初始化随机数生成器
    std::srand(static_cast<unsigned int>(std::time(nullptr)));
}

void BatchCounter::SetEVMType(EVMType ty) { 
    this->evm_type = ty; 
}

Transaction BatchCounter::Next() {
    // 检查是否已经生成足够数量的交易
    if (current_count >= num_transactions) {
        LOG(WARNING) << "已达到指定的交易数量上限，重置计数器";
        // 重置计数器，从头开始
        current_count = 0;
    }
    
    // 增加当前计数
    current_count++;
    
    // 判断是否应该是跨链交易
    bool is_cross_chain = ShouldBeCrossChain();
    
    LOG(INFO) << fmt::format("BatchCounter::Next() - 生成第 {}/{} 个交易 ({})", 
                           current_count, 
                           num_transactions, 
                           is_cross_chain ? "跨链" : "普通");
    
    // 创建一个调用add函数的交易
    // add函数的签名是4字节的函数选择器: keccak256("add()")前4字节
    auto input = spectrum::from_hex("4f2be91f").value(); // "add()"的函数选择器
    
    // 创建预测的存储访问集合
    auto predicted_get_storage = std::unordered_set<std::tuple<evmc::address, evmc::bytes32>, KeyHasher>();
    auto predicted_set_storage = std::unordered_set<std::tuple<evmc::address, evmc::bytes32>, KeyHasher>();
    
    // 随机生成一些不同的地址，增加交易的多样性
    evmc::address contract_address = evmc::address{};
    contract_address.bytes[0] = static_cast<uint8_t>(current_count % 256);
    contract_address.bytes[1] = static_cast<uint8_t>((current_count / 256) % 256);
    
    // x变量会先被读取然后被修改
    predicted_get_storage.insert({contract_address, key});
    predicted_set_storage.insert({contract_address, key});
    
    // 创建交易
    auto tx = Transaction(this->evm_type, contract_address, contract_address, std::span{code}, std::span{input});
    tx.predicted_get_storage = predicted_get_storage;
    tx.predicted_set_storage = predicted_set_storage;
    
    // 为跨链交易添加更多的存储访问，增加复杂性
    if (is_cross_chain || std::rand() % 5 == 0) { // 跨链交易或20%的普通交易
        evmc::bytes32 extra_key = evmc::bytes32{};
        extra_key.bytes[0] = static_cast<uint8_t>(std::rand() % 256);
        predicted_get_storage.insert({contract_address, extra_key});
        predicted_set_storage.insert({contract_address, extra_key});
    }
    
    return tx;
}

} // namespace spectrum 
