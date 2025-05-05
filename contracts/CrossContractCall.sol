// SPDX-License-Identifier: MIT
pragma solidity >=0.8.0 <0.9.0;

// B合约
contract ContractB {
    uint256 public y;
    
    event YUpdated(uint256 oldValue, uint256 newValue, uint256 addedX);
    
    constructor() {
        y = 0;
    }
    
    // 接收x参数并更新y
    function add(uint256 x) public returns (uint256) {
        uint256 oldY = y;
        y = y + x;
        emit YUpdated(oldY, y, x);
        return y;
    }
    
    function getY() public view returns (uint256) {
        return y;
    }
}

// A合约 - 调用B合约
contract ContractA {
    uint256 public x;
    ContractB private contractB;
    
    event XUpdated(uint256 oldValue, uint256 newValue);
    event CrossContractCallResult(uint256 newY);
    
    constructor(address contractBAddress) {
        x = 0;
        contractB = ContractB(contractBAddress);
    }
    
    // 增加x并调用B合约的add函数
    function add() public returns (uint256) {
        uint256 oldX = x;
        x = x + 1;
        emit XUpdated(oldX, x);
        
        // 调用B合约的add函数，传入更新后的x值
        uint256 newY = contractB.add(x);
        emit CrossContractCallResult(newY);
        
        return x;
    }
    
    // 更新B合约地址
    function updateContractB(address newContractBAddress) public {
        contractB = ContractB(newContractBAddress);
    }
    
    // 获取B合约中的y值
    function getYFromB() public view returns (uint256) {
        return contractB.getY();
    }
} 