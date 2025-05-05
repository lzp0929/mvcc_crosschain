// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

contract SimpleXY {
    uint256 public x;
    uint256 public y;
    
    function addX() public {
        x = x + 1;
        addY(x);
    }
    
    function addY(uint256 value) public {
        y = y + value;
    }
    
    // 添加getter函数便于测试
    function getX() public view returns (uint256) {
        return x;
    }
    
    function getY() public view returns (uint256) {
        return y;
    }
} 