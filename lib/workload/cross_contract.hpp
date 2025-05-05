#pragma once

#include <spectrum/workload/abstraction.hpp>
#include <spectrum/common/random.hpp>
#include <cstdint>

namespace spectrum {

class CrossContract: public Workload {
private:
    std::basic_string<uint8_t>  codeA;             // A合约代码
    std::basic_string<uint8_t>  codeB;             // B合约代码
    EVMType                     evm_type;          // EVM类型
    evmc::address               addrA;             // A合约地址
    evmc::address               addrB;             // B合约地址
    size_t                      call_count;        // 调用计数
    
    // 函数选择器
    std::basic_string<uint8_t>  call_a_add;        // ContractA.add()
    std::basic_string<uint8_t>  call_a_getY;       // ContractA.getYFromB()
    std::basic_string<uint8_t>  call_b_add_prefix; // ContractB.add(uint256)
    std::basic_string<uint8_t>  call_b_getY;       // ContractB.getY()

public:
    CrossContract();
    Transaction Next() override;
    void SetEVMType(EVMType ty) override;
    
    // 构造调用A合约add方法的交易（会调用B合约）
    Transaction CallContractA();
    
    // 构造调用B合约add方法的交易
    Transaction CallContractB();
    
    // 构造A合约调用B合约的跨合约调用交易
    Transaction CallCrossContract();
};

} // namespace spectrum 