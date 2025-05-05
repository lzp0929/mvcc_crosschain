#pragma once

#include <spectrum/workload/abstraction.hpp>
#include <spectrum/transaction/evm-transaction.hpp>
#include <vector>

namespace spectrum {

class SimpleXY final: public Workload {
private:
    EVMType evm_type;
    std::basic_string<uint8_t> code;
    
    // 函数选择器
    std::vector<uint8_t> addX_selector;
    std::vector<uint8_t> addY_selector;
    std::vector<uint8_t> getX_selector;
    std::vector<uint8_t> getY_selector;
    
    // 辅助函数
    std::vector<uint8_t> buildCalldata(const std::vector<uint8_t>& selector, const std::vector<std::vector<uint8_t>>& params = {});
    std::vector<uint8_t> uint256ToBytes(size_t value);
    
public:
    SimpleXY();
    void SetEVMType(EVMType ty) override;
    Transaction Next() override;
    
    // 单独调用每个函数的方法
    Transaction CallAddX();
    Transaction CallAddY(size_t value);
    Transaction CallGetX();
    Transaction CallGetY();
}; // class SimpleXY

} // namespace spectrum 