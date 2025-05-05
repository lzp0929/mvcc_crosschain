#pragma once

#include <spectrum/workload/abstraction.hpp>
#include <spectrum/common/random.hpp>

namespace spectrum {

class BatchCounter: public Workload {
private:
    std::basic_string<uint8_t>  code;
    EVMType                     evm_type;
    evmc::bytes32               key;
    size_t                      num_transactions; // 要生成的交易总数
    size_t                      current_count;    // 当前已生成的交易数
    size_t                      cross_chain_ratio; // 跨链交易比例(百分比)

public:
    // 构造函数，可以指定要生成的交易数量和跨链交易比例（默认为10%）
    BatchCounter(size_t num_txs = 1000, size_t cross_chain_percent = 10);
    
    // 返回下一个交易
    Transaction Next() override;
    
    // 设置EVM类型
    void SetEVMType(EVMType ty) override;
    
    // 获取合约字节码
    const std::basic_string<uint8_t>& GetCode() const {
        return code;
    }
    
    // 返回是否还有更多交易
    bool HasMore() const {
        return current_count < num_transactions;
    }
    
    // 重置计数器，重新开始生成交易
    void Reset(size_t new_num_txs = 0) {
        current_count = 0;
        if (new_num_txs > 0) {
            num_transactions = new_num_txs;
        }
    }
    
    // 设置跨链交易比例
    void SetCrossChainRatio(size_t percent) {
        cross_chain_ratio = percent;
    }
    
    // 判断当前生成的交易是否应该是跨链交易
    bool ShouldBeCrossChain() const {
        // 根据设定的比例随机决定是否是跨链交易
        return (std::rand() % 100) < cross_chain_ratio;
    }
    
    // 获取当前已生成的交易数量
    size_t GetCurrentCount() const {
        return current_count;
    }
    
    // 获取要生成的交易总数
    size_t GetTotalTransactions() const {
        return num_transactions;
    }
};

} // namespace spectrum 
    