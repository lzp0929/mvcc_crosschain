pragma solidity >=0.7.0 <0.9.0;

contract SimpleCounter {
    uint256 public x;

    function add() public {
        x = x + 1;
    }

    function get() public view returns (uint256) {
        return x;
    }

    function init(uint256 value) public {
        x = value;
    }
} 