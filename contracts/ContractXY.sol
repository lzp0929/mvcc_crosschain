// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

contract ContractX {
    uint256 public x;
    ContractY private contractY;
    
    constructor(address _contractYAddress) {
        contractY = ContractY(_contractYAddress);
    }
    
    function addX() public {
        x = x + 1;
        contractY.addY(x);
    }
    
    function getX() public view returns (uint256) {
        return x;
    }
}

contract ContractY {
    uint256 public y;
    
    function addY(uint256 value) public {
        y = y + value;
    }
    
    function getY() public view returns (uint256) {
        return y;
    }
} 