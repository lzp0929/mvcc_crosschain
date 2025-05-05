#include "simple_counter.hpp"
#include "evmc/evmc.hpp"
#include "spectrum/transaction/evm-hash.hpp"
#include <spectrum/common/hex.hpp>
#include <fmt/core.h>
#include <glog/logging.h>

namespace spectrum {

const static char* CODE = 
    #include "../../contracts/simple_counter.bin"
;

SimpleCounter::SimpleCounter(): evm_type{EVMType::STRAWMAN} {
    LOG(INFO) << "SimpleCounter()";
    this->code = spectrum::from_hex(std::string{CODE}).value();
    
    // 创建x变量的存储键
    // 在Solidity中，公共状态变量的存储位置是keccak256(abi.encode(0))，其中0是变量的位置
    // 我们用一个简单表示，实际应用需要根据您的合约计算真实的键
    key = evmc::bytes32{};
    key.bytes[0] = 0; // 简化表示，这不是准确的Solidity存储布局
}

void SimpleCounter::SetEVMType(EVMType ty) { 
    this->evm_type = ty; 
}

Transaction SimpleCounter::Next() {
    LOG(INFO) << "simple_counter next";
    
    // 创建一个调用add函数的交易
    // add函数的签名是4字节的函数选择器: keccak256("add()")前4字节
    auto input = spectrum::from_hex("4f2be91f").value(); // "add()"的函数选择器
    
    // 创建预测的存储访问集合
    auto predicted_get_storage = std::unordered_set<std::tuple<evmc::address, evmc::bytes32>, KeyHasher>();
    auto predicted_set_storage = std::unordered_set<std::tuple<evmc::address, evmc::bytes32>, KeyHasher>();
    
    // x变量会先被读取然后被修改
    predicted_get_storage.insert({evmc::address{0x1}, key});
    predicted_set_storage.insert({evmc::address{0x1}, key});
    
    // 创建交易
    auto tx = Transaction(this->evm_type, evmc::address{0x1}, evmc::address{0x1}, std::span{code}, std::span{input});
    tx.predicted_get_storage = predicted_get_storage;
    tx.predicted_set_storage = predicted_set_storage;
    
    return tx;
}

} // namespace spectrum 