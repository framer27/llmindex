"""
通用工具函数模块，提供辅助功能
"""
import os
import logging
import time
import hashlib

def measure_time(func):
    """装饰器：测量函数执行时间"""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        logging.getLogger('performance').info(f'{func.__name__} 执行耗时: {end_time - start_time:.2f}秒')
        return result
    return wrapper

def load_api_key(cmd_api_key=None):
    """
    加载API密钥，优先级：
    1. 命令行参数
    2. 环境变量
    """
    # 优先使用命令行参数
    if cmd_api_key:
        return cmd_api_key
        
    # 尝试从环境变量加载
    for env_name in ['DEEPSEEK_API_KEY', 'DEEPSEEK_KEY', 'DEEPSEEK_TOKEN']:
        api_key = os.environ.get(env_name)
        if api_key and api_key.strip():
            # 移除可能的引号
            return api_key.strip().strip('"\'')
    
    # 如果无法获取API密钥，提示用户
    raise ValueError('请提供DeepSeek API密钥：通过环境变量DEEPSEEK_API_KEY或命令行参数--api-key设置')

def calculate_hash(file_path):
    """计算文件的SHA256哈希值"""
    with open(file_path, 'rb') as f:
        file_content = f.read()
    return hashlib.sha256(file_content).hexdigest()

def format_result(result):
    """格式化查询结果为可读字符串"""
    import json
    from pandas import DataFrame
    
    # 根据结果类型选择合适的格式化方法
    if isinstance(result, DataFrame):
        return result.to_string()
    elif isinstance(result, (list, dict)):
        return json.dumps(result, ensure_ascii=False, indent=2)
    else:
        return str(result) 