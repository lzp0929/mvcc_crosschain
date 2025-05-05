#include "cross_contract.hpp"
#include "evmc/evmc.hpp"
#include "spectrum/transaction/evm-hash.hpp"
#include <spectrum/common/hex.hpp>
#include <fmt/core.h>
#include <glog/logging.h>
#include <iomanip>
#include <sstream>
#include <unordered_set>
#include <tuple>
#include <string>

namespace spectrum {

// 辅助函数：将evmc地址转换为字符串
std::string address_to_string(const evmc::address& addr) {
    std::stringstream ss;
    ss << "0x";
    for (int i = 0; i < 20; i++) {
        ss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(addr.bytes[i]);
    }
    return ss.str();
}

// 引入合约二进制代码
const static char* CODE_A = R"(608060405234801561000f575f5ffd5b506040516105b03803806105b0833981810160405281019061003191906100db565b5f5f819055508060015f6101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff16021790555050610106565b5f5ffd5b5f73ffffffffffffffffffffffffffffffffffffffff82169050919050565b5f6100aa82610081565b9050919050565b6100ba816100a0565b81146100c4575f5ffd5b50565b5f815190506100d5816100b1565b92915050565b5f602082840312156100f0576100ef61007d565b5b5f6100fd848285016100c7565b91505092915050565b61049d806101135f395ff3fe608060405234801561000f575f5ffd5b506004361061004a575f3560e01c80630c55699c1461004e5780634f2be91f1461006c578063a744bad11461008a578063b6f6915f146100a8575b5f5ffd5b6100566100c4565b60405161006391906102e9565b60405180910390f35b6100746100c9565b60405161008191906102e9565b60405180910390f35b6100926101fa565b60405161009f91906102e9565b60405180910390f35b6100c260048036038101906100bd9190610360565b61028e565b005b5f5481565b5f5f5f54905060015f546100dd91906103b8565b5f819055507fa77960334c50b44b56ab1b342b04dec3394c09a1c3c9fcd6534f4e158f885d0d815f546040516101149291906103eb565b60405180910390a15f60015f9054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16631003e2d25f546040518263ffffffff1660e01b815260040161017891906102e9565b6020604051808303815f875af1158015610194573d5f5f3e3d5ffd5b505050506040513d601f19601f820116820180604052508101906101b8919061043c565b90507fbb94f145954b87ced9c023e76ce98fb6b427e4719fdcd3b65c1f466b567fdbed816040516101e991906102e9565b60405180910390a15f549250505090565b5f60015f9054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16630b7f16656040518163ffffffff1660e01b8152600401602060405180830381865afa158015610265573d5f5f3e3d5ffd5b505050506040513d601f19601f82011682018060405250810190610289919061043c565b905090565b8060015f6101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff16021790555050565b5f819050919050565b6102e3816102d1565b82525050565b5f6020820190506102fc5f8301846102da565b92915050565b5f5ffd5b5f73ffffffffffffffffffffffffffffffffffffffff82169050919050565b5f61032f82610306565b9050919050565b61033f81610325565b8114610349575f5ffd5b50565b5f8135905061035a81610336565b92915050565b5f6020828403121561037557610374610302565b5b5f6103828482850161034c565b91505092915050565b7f4e487b71000000000000000000000000000000000000000000000000000000005f52601160045260245ffd5b5f6103c2826102d1565b91506103cd836102d1565b92508282019050808211156103e5576103e461038b565b5b92915050565b5f6040820190506103fe5f8301856102da565b61040b60208301846102da565b9392505050565b61041b816102d1565b8114610425575f5ffd5b50565b5f8151905061043681610412565b92915050565b5f6020828403121561045157610450610302565b5b5f61045e84828501610428565b91505092915050565b)";

const static char* CODE_B = R"(608060405234801561001057600080fd5b5060cc8061001f6000396000f3fe6080604052348015600f57600080fd5b506004361060325760003560e01c80630b7f16651460375780631003e2d214604c575b600080fd5b603d6062565b6040516048919060a7565b60405180910390f35b60606058366004608c565b6068565b005b60005490565b806000808282546078919060b9565b9250508190555050565b600060208284031215609d57600080fd5b5035919050565b60b18160db565b82525050565b602081016093828460aa565b60da8160db565b82525050565b600060020190506093828460cd565b92915050565b600060028201915060b1828483fe)";

// 足够高的gas限制，确保交易能够完成
const uint64_t GAS_LIMIT = 10000000;

CrossContract::CrossContract(): 
    evm_type{EVMType::STRAWMAN},
    call_count{0} 
{
    LOG(INFO) << "CrossContract()";
    
    // 加载合约字节码 - 这里我们把它们存下来，
    // 但在实际使用时，我们假设合约已经部署
    this->codeA = spectrum::from_hex(std::string{CODE_A}).value();
    this->codeB = spectrum::from_hex(std::string{CODE_B}).value();
    
    // 设置合约地址 - 假设合约已部署到这些地址
    addrA = evmc::address{0x1};
    addrB = evmc::address{0x2};
    
    // 预先创建函数调用输入数据
    // 这些是各函数的选择器（函数签名的keccak256前4字节）
    
    // ContractA.add()函数选择器: 0x4f2be91f
    call_a_add = spectrum::from_hex("4f2be91f").value();
    
    // ContractA.getYFromB()函数选择器: 0xa744bad1
    call_a_getY = spectrum::from_hex("a744bad1").value();
    
    // ContractB.add(uint256)函数选择器: 0x1003e2d2
    // 注意此处还需要附加参数值
    call_b_add_prefix = spectrum::from_hex("1003e2d2").value();
    
    // ContractB.getY()函数选择器: 0x0b7f1665
    call_b_getY = spectrum::from_hex("0b7f1665").value();
}

void CrossContract::SetEVMType(EVMType ty) { 
    this->evm_type = ty; 
}

// 辅助函数：格式化uint256参数
std::string to_hex_string(size_t value) {
    std::stringstream ss;
    ss << std::hex << std::setw(64) << std::setfill('0') << value;
    return ss.str();
}

Transaction CrossContract::CallContractA() {
    LOG(INFO) << "调用A合约的add()函数 - 会触发对B合约的调用";
    
    // 创建对A合约add()函数的调用交易
    // Transaction构造函数中已设置gas = 999999999
    return Transaction(
        this->evm_type,        // 并发控制类型
        evmc::address{0},      // 发送者地址
        addrA,                 // A合约地址
        std::span{codeA},      // A合约代码
        std::span{call_a_add}  // add()函数选择器
    );
}

Transaction CrossContract::CallContractB() {
    LOG(INFO) << "调用B合约的add(1)函数";
    
    // 创建参数 - 传递1作为参数
    auto param_value = spectrum::from_hex(to_hex_string(1)).value();
    
    // 拼接选择器和参数
    std::basic_string<uint8_t> input_data(call_b_add_prefix);
    input_data.insert(input_data.end(), param_value.begin(), param_value.end());
    
    // 创建对B合约add(uint256)函数的调用交易
    // Transaction构造函数中已设置gas = 999999999
    return Transaction(
        this->evm_type,       // 并发控制类型
        evmc::address{0},     // 发送者地址
        addrB,                // B合约地址
        std::span{codeB},     // B合约代码
        std::span{input_data} // add(1)函数调用数据
    );
}

Transaction CrossContract::CallCrossContract() {
    LOG(INFO) << "调用A合约的add()函数 - 它将跨合约调用B合约";
    
    // 这与CallContractA相同，因为A合约的add()函数内部会调用B合约
    // Transaction构造函数中已设置gas = 999999999
    return Transaction(
        this->evm_type,        // 并发控制类型
        evmc::address{0},      // 发送者地址
        addrA,                 // A合约地址
        std::span{codeA},      // A合约代码
        std::span{call_a_add}  // add()函数选择器
    );
}

Transaction CrossContract::Next() {
    // 实现一个轮询模式，依次产生三种不同的交易
    switch (call_count % 3) {
        case 0:
            LOG(INFO) << "生成对A合约的直接调用";
            call_count++;
            return CallContractA();
        case 1:
            LOG(INFO) << "生成对B合约的直接调用";
            call_count++;
            return CallContractB();
        case 2:
            LOG(INFO) << "生成跨合约调用：通过A合约调用B合约";
            call_count++;
            return CallCrossContract();
        default:
            // 不应该到达这里
            return CallContractA();
    }
}

} // namespace spectrum 