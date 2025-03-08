"""
日志配置模块，负责统一配置应用程序的日志
"""
import logging

# 全局日志配置标志
_LOGGING_CONFIGURED = False

def setup_logging():
    """配置全局日志设置，确保只配置一次"""
    global _LOGGING_CONFIGURED
    if _LOGGING_CONFIGURED:
        return
        
    # 移除所有根日志处理器
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
        
    # 设置根日志级别为INFO，只输出重要信息
    root_logger.setLevel(logging.INFO)
    
    # 创建简洁的控制台处理器
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(message)s'))
    root_logger.addHandler(handler)
    
    _LOGGING_CONFIGURED = True
    
    return root_logger

def get_logger(name):
    """获取指定名称的日志记录器，确保不会传播到根日志记录器"""
    logger = logging.getLogger(name)
    logger.propagate = False
    
    # 如果logger没有处理器，添加一个
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(message)s'))
        logger.addHandler(handler)
        
    return logger 