const crypto = require('crypto');

// 计算函数选择器
function getFunctionSelector(functionSignature) {
  // 创建keccak256哈希
  const hash = crypto.createHash('sha256').update(functionSignature).digest('hex');
  // 获取前8个字符（4个字节）
  return '0x' + hash.substring(0, 8);
}

// SimpleXY合约中的函数
const functions = [
  'addX()',
  'addY(uint256)',
  'getX()',
  'getY()',
  'x()',
  'y()'
];

// 输出所有函数选择器
console.log('Function Selectors:');
functions.forEach(func => {
  console.log(`${func}: ${getFunctionSelector(func)}`);
}); 