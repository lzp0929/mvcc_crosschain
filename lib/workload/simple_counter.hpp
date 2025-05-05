#pragma once

#include <spectrum/workload/abstraction.hpp>
#include <spectrum/common/random.hpp>

namespace spectrum {

class SimpleCounter: public Workload {
private:
    std::basic_string<uint8_t>  code;
    EVMType                     evm_type;
    evmc::bytes32               key;

public:
    SimpleCounter();
    Transaction Next() override;
    void SetEVMType(EVMType ty) override;
    
    // 获取合约字节码
    const std::basic_string<uint8_t>& GetCode() const {
        return code;
    }
};

} // namespace spectrum 