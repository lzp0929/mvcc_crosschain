import hashlib

# 计算函数选择器
def get_function_selector(function_signature):
    # 创建keccak256哈希
    keccak = hashlib.sha3_256()
    keccak.update(function_signature.encode('utf-8'))
    # 获取前4个字节（8个十六进制字符）
    return '0x' + keccak.hexdigest()[:8]

# SimpleXY合约中的函数
functions = [
    'addX()',
    'addY(uint256)',
    'getX()',
    'getY()',
    'x()',
    'y()'
]

# 输出所有函数选择器
print('Function Selectors:')
for func in functions:
    print(f'{func}: {get_function_selector(func)}') 