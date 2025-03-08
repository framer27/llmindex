"""
缓存管理模块，负责处理缓存相关功能
"""
import os
import time
import joblib
import logging
import hashlib
from src.config import CACHE_DIR, CACHE_VERSION
from src.logger import get_logger

logger = get_logger('cache')

def get_cache_key(tables_data_path):
    """生成缓存键，基于表结构文件的内容哈希和缓存版本"""
    # 读取表结构文件内容
    with open(tables_data_path, 'rb') as f:
        file_content = f.read()
        
    # 计算表结构文件的SHA256哈希值
    tables_hash = hashlib.sha256(file_content).hexdigest()
    
    # 组合缓存键
    cache_key = f"engine_cache_v{CACHE_VERSION}_{tables_hash}"
    return cache_key
    
def get_cache_path(cache_key):
    """获取缓存文件路径"""
    return os.path.join(CACHE_DIR, f"{cache_key}.joblib")
    
def check_cache(tables_data_path):
    """检查是否存在有效的缓存"""
    cache_key = get_cache_key(tables_data_path)
    cache_path = get_cache_path(cache_key)
    
    if os.path.exists(cache_path):
        logger.info(f"找到有效缓存")
        return cache_path
    else:
        logger.info("未找到缓存，将重新构建")
        return None
        
def save_cache(data, tables_data_path):
    """保存缓存数据"""
    cache_key = get_cache_key(tables_data_path)
    cache_path = get_cache_path(cache_key)
    
    logger.info("保存缓存中...")
    
    try:
        # 保存缓存文件
        joblib.dump(data, cache_path)
        logger.info("缓存保存成功")
        return True
    except Exception as e:
        logger.error(f"缓存保存失败: {str(e)}")
        # 如果保存失败，尝试删除可能的部分缓存文件
        if os.path.exists(cache_path):
            try:
                os.remove(cache_path)
            except:
                pass
        return False
        
def load_cache(cache_path):
    """加载缓存数据"""
    logger.info("加载缓存中...")
    
    try:
        # 加载缓存文件
        cache_data = joblib.load(cache_path)
        
        # 验证缓存版本
        if cache_data.get('cache_version') != CACHE_VERSION:
            logger.warning(f"缓存版本不匹配，将重新构建")
            return None
            
        # 计算缓存年龄
        cache_age = time.time() - cache_data.get('cached_time', 0)
        logger.info(f"缓存年龄: {cache_age/3600:.1f}小时")
        
        logger.info("缓存加载成功")
        return cache_data
    except Exception as e:
        logger.error(f"缓存加载失败: {str(e)}")
        # 如果加载失败，删除可能已损坏的缓存文件
        try:
            os.remove(cache_path)
        except:
            pass
        return None 