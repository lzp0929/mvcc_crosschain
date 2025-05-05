#include "simple_xy.hpp"
#include "evmc/evmc.hpp"
#include "spectrum/transaction/evm-hash.hpp"
#include <spectrum/common/hex.hpp>
#include <fmt/core.h>
#include <glog/logging.h>
#include <iomanip>
#include <sstream>
#include <algorithm>

namespace spectrum {

// Solidity合约编译后的运行时字节码（从build/contracts/SimpleXY.bin-runtime获取）
const static char* SOLIDITY_RUNTIME_BYTECODE = 
    "608060405234801561000f575f5ffd5b5060043610610060575f3560e01c80630b7f1665146100645780630c55699c146100825"
    "780630f1d4237146100a05780635197c7aa146100aa57806369d3db69146100c8578063a56dfe4a146100e4575b5f5ffd5b6100"
    "6c610102565b604051610079919061016c565b60405180910390f35b61008a61010b565b604051610097919061016c565b60405"
    "180910390f35b6100a8610110565b005b6100b261012f565b6040516100bf919061016c565b60405180910390f35b6100e26004"
    "8036038101906100dd91906101b3565b610137565b005b6100ec61014e565b6040516100f9919061016c565b60405180910390f"
    "35b5f600154905090565b5f5481565b60015f5461011e919061020b565b5f8190555061012d5f54610137565b565b5f5f549050"
    "90565b80600154610145919061020b565b60018190555050565b60015481565b5f819050919050565b61016681610154565b825"
    "25050565b5f60208201905061017f5f83018461015d565b92915050565b5f5ffd5b61019281610154565b811461019c575f5ffd"
    "5b50565b5f813590506101ad81610189565b92915050565b5f602082840312156101c8576101c7610185565b5b5f6101d584828"
    "50161019f565b91505092915050565b7f4e487b71000000000000000000000000000000000000000000000000000000005f5260"
    "1160045260245ffd5b5f61021582610154565b915061022083610154565b9250828201905080821115610238576102376101de5"
    "65b5b9291505056fea2646970667358221220bf72acd32e50c2e761cb91737589eb31fd48a4b725257b56d1c162609fa3778064";

SimpleXY::SimpleXY(): 
    evm_type{EVMType::STRAWMAN}
{
    LOG(INFO) << "SimpleXY()";
    
    // 加载运行时字节码，而不是部署字节码
    this->code = spectrum::from_hex(std::string{SOLIDITY_RUNTIME_BYTECODE}).value();
    
    // 初始化函数选择器，使用solc --hashes输出的正确选择器
    auto addX_tmp = spectrum::from_hex("0f1d4237").value();
    auto addY_tmp = spectrum::from_hex("69d3db69").value();
    auto getX_tmp = spectrum::from_hex("5197c7aa").value();
    auto getY_tmp = spectrum::from_hex("0b7f1665").value();
    
    // 将string类型转换为vector类型
    addX_selector = std::vector<uint8_t>(addX_tmp.begin(), addX_tmp.end());
    addY_selector = std::vector<uint8_t>(addY_tmp.begin(), addY_tmp.end());
    getX_selector = std::vector<uint8_t>(getX_tmp.begin(), getX_tmp.end());
    getY_selector = std::vector<uint8_t>(getY_tmp.begin(), getY_tmp.end());
}

// 生成函数调用数据
std::vector<uint8_t> SimpleXY::buildCalldata(const std::vector<uint8_t>& selector, const std::vector<std::vector<uint8_t>>& params) {
    std::vector<uint8_t> calldata(selector);
    
    // 添加所有参数
    for (const auto& param : params) {
        calldata.insert(calldata.end(), param.begin(), param.end());
    }
    
    return calldata;
}

// 将数值转换为字节数组
std::vector<uint8_t> SimpleXY::uint256ToBytes(size_t value) {
    std::vector<uint8_t> result(32, 0); // 初始化32字节全为0
    
    // 将值转换为大端序字节数组
    for (int i = 31; i >= 0 && value > 0; i--) {
        result[i] = value & 0xFF;
        value >>= 8;
    }
    
    return result;
}

void SimpleXY::SetEVMType(EVMType ty) { 
    this->evm_type = ty; 
}

Transaction SimpleXY::Next() {
    // 始终返回addX()函数调用交易
    LOG(INFO) << "SimpleXY next - 调用 addX() 函数";
    return CallAddX();
}

Transaction SimpleXY::CallAddX() {
    LOG(INFO) << "调用 addX() 函数 - 这会触发对addY(x)的函数间调用";
    
    // 构造函数调用数据：addX()
    auto input = buildCalldata(addX_selector);
    
    // 创建交易
    return Transaction(
        this->evm_type,             // 并发控制类型
        evmc::address{0},           // 发送者地址
        evmc::address{0x1},         // 合约地址
        std::span{code},            // 合约代码
        std::span{input.data(), input.size()} // 函数调用数据
    );
}

Transaction SimpleXY::CallAddY(size_t value) {
    LOG(INFO) << "调用 addY(" << value << ") 函数";
    
    // 将参数转换为字节数组
    auto value_bytes = uint256ToBytes(value);
    
    // 构造函数调用数据：addY(value)
    auto input = buildCalldata(addY_selector, {value_bytes});
    
    // 创建交易
    return Transaction(
        this->evm_type,             // 并发控制类型
        evmc::address{0},           // 发送者地址
        evmc::address{0x1},         // 合约地址
        std::span{code},            // 合约代码
        std::span{input.data(), input.size()} // 函数调用数据
    );
}

Transaction SimpleXY::CallGetX() {
    LOG(INFO) << "调用 getX() 函数";
    
    // 构造函数调用数据：getX()
    auto input = buildCalldata(getX_selector);
    
    // 创建交易
    return Transaction(
        this->evm_type,             // 并发控制类型
        evmc::address{0},           // 发送者地址
        evmc::address{0x1},         // 合约地址
        std::span{code},            // 合约代码
        std::span{input.data(), input.size()} // 函数调用数据
    );
}

Transaction SimpleXY::CallGetY() {
    LOG(INFO) << "调用 getY() 函数";
    
    // 构造函数调用数据：getY()
    auto input = buildCalldata(getY_selector);
    
    // 创建交易
    return Transaction(
        this->evm_type,             // 并发控制类型
        evmc::address{0},           // 发送者地址
        evmc::address{0x1},         // 合约地址
        std::span{code},            // 合约代码
        std::span{input.data(), input.size()} // 函数调用数据
    );
}

} // namespace spectrum 