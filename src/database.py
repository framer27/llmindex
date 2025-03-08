"""
数据库连接池管理模块，负责创建和管理数据库连接
"""
import time
import logging
import yaml
import json
import os
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from src.config import DB_POOL_SIZE, DB_MAX_OVERFLOW, DB_POOL_TIMEOUT, DB_POOL_RECYCLE, CONFIG_DIR

# 全局数据库连接池
_ENGINE_POOL = None

def load_db_config():
    """
    加载数据库配置，支持JSON和YAML格式
    """
    logger = logging.getLogger('database')
    
    # 默认配置
    default_config = {
        "server": "localhost",
        "database": "mes",
        "username": "sa",
        "password": ""
    }
    
    # 尝试加载YAML格式配置
    yaml_path = os.path.join(CONFIG_DIR, 'db_config.yaml')
    if os.path.exists(yaml_path):
        try:
            with open(yaml_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                if config and isinstance(config, dict):
                    # 确保配置包含所有必要的键
                    for key in default_config:
                        if key not in config:
                            config[key] = default_config[key]
                            logger.warning(f"配置文件缺少'{key}'字段，使用默认值: {default_config[key]}")
                    return config
                else:
                    logger.warning("YAML配置无效，使用默认配置")
        except Exception as e:
            logger.warning(f"无法加载YAML配置文件: {e}")
            
    # 尝试加载JSON格式配置
    json_path = os.path.join(CONFIG_DIR, 'db_config.json')
    if os.path.exists(json_path):
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                if config and isinstance(config, dict):
                    # 确保配置包含所有必要的键
                    for key in default_config:
                        if key not in config:
                            config[key] = default_config[key]
                            logger.warning(f"配置文件缺少'{key}'字段，使用默认值: {default_config[key]}")
                    return config
                else:
                    logger.warning("JSON配置无效，使用默认配置")
        except Exception as e:
            logger.warning(f"无法加载JSON配置文件: {e}")
    
    # 尝试创建默认配置文件
    try:
        os.makedirs(CONFIG_DIR, exist_ok=True)
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(default_config, f, indent=2)
        logger.info(f"已创建默认数据库配置: {json_path}")
    except Exception as e:
        logger.warning(f"无法创建默认配置文件: {e}")
        
    # 返回默认配置
    logger.info("使用默认数据库配置")
    return default_config

def get_db_engine(db_config=None, test_mode=False):
    """
    获取数据库连接池中的引擎
    
    参数:
        db_config: 数据库配置字典，如果为None则加载配置文件
        test_mode: 如果为True，返回一个SQLite内存数据库引擎用于测试
    """
    global _ENGINE_POOL
    logger = logging.getLogger('database')
    
    # 如果启用测试模式，返回SQLite内存数据库
    if test_mode:
        logger.info("使用SQLite内存数据库（测试模式）")
        # 使用echo=True可以打印所有SQL语句，便于调试
        return create_engine('sqlite:///:memory:', echo=False)
    
    if _ENGINE_POOL is None:
        if db_config is None:
            db_config = load_db_config()
            
        # 创建连接URL
        password = str(db_config['password']) if 'password' in db_config else ""
        server = db_config['server']
        database = db_config['database']
        username = db_config['username']
        
        # 检查是否跳过驱动检查
        skip_driver_check = db_config.get('skip_driver_check', False)
        
        # 记录性能
        pool_start = time.time()
        
        try:
            # 构建连接字符串基础部分
            if '\\' in server:
                # 对于命名实例 (如: SERVER\INSTANCE)
                connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}"
            else:
                # 对于默认实例或使用端口 (如: SERVER 或 SERVER:1433)
                connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}"
            
            # 添加连接参数
            params = []
            
            # 如果不跳过驱动检查，添加驱动程序信息
            if not skip_driver_check:
                params.append("driver=ODBC+Driver+17+for+SQL+Server")
                
            # 添加其他常用参数，这些可能对某些环境有帮助
            params.append("TrustServerCertificate=yes")
            params.append("Encrypt=yes")  # 对于SQL Server 2022+，默认需要加密
            
            # 组合连接字符串
            if params:
                connection_string += "?" + "&".join(params)
            
            logger.info(f"正在连接到数据库: {server}/{database}")
            if skip_driver_check:
                logger.info("已跳过ODBC驱动检查")
            
            # 创建引擎
            _ENGINE_POOL = create_engine(
                connection_string,
                poolclass=QueuePool,
                pool_size=DB_POOL_SIZE,
                max_overflow=DB_MAX_OVERFLOW,
                pool_timeout=DB_POOL_TIMEOUT,
                pool_recycle=DB_POOL_RECYCLE,
                pool_pre_ping=True,
                # 增加连接超时设置，避免长时间挂起
                connect_args={"timeout": 30}
            )
            
            # 测试连接
            with _ENGINE_POOL.connect() as conn:
                pass  # 只是测试连接是否成功
                
            pool_time = time.time() - pool_start
            logger.info(f'创建数据库连接池成功，耗时: {pool_time:.2f}秒')
            
        except Exception as e:
            logger.error(f"数据库连接失败: {str(e)}")
            # 在连接失败时提供更多诊断信息
            logger.error(f"连接详情: 服务器={server}, 数据库={database}, 用户={username}")
            logger.error("请使用'python setup_db.py'配置正确的数据库参数，或使用--test-mode选项")
            raise
    
    return _ENGINE_POOL

def get_pool_status():
    """获取数据库连接池的当前状态"""
    if _ENGINE_POOL is None:
        return {
            "status": "未初始化",
            "connections": 0,
            "in_use": 0,
            "available": 0
        }
    
    try:
        # 尝试获取连接池状态信息
        pool = _ENGINE_POOL.pool
        return {
            "status": "活跃",
            "connections": pool.checkedin() + pool.checkedout(),
            "in_use": pool.checkedout(),
            "available": pool.checkedin(),
            "overflow": pool.overflow(),
            "max_size": pool.size() + pool.overflow()
        }
    except Exception as e:
        # 如果无法获取详细信息，返回基本状态
        return {
            "status": "活跃",
            "error": str(e),
            "connections": "未知"
        }

def test_connection(engine=None):
    """测试数据库连接是否正常"""
    if engine is None:
        engine = get_db_engine()
        
    try:
        connection = engine.connect()
        connection.close()
        return True, "连接成功"
    except Exception as e:
        return False, str(e)

def execute_query(query, params=None):
    """执行SQL查询"""
    engine = get_db_engine()
    logger = logging.getLogger('database')
    
    start_time = time.time()
    logger.debug(f"执行查询: {query}")
    
    try:
        with engine.connect() as connection:
            if params:
                result = connection.execute(text(query), params)
            else:
                result = connection.execute(text(query))
                
            # 将结果转换为字典列表
            columns = result.keys()
            data = [dict(zip(columns, row)) for row in result]
            
            elapsed = time.time() - start_time
            logger.debug(f"查询执行完成，耗时: {elapsed:.2f}秒，返回{len(data)}行")
            
            return data
    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"查询执行失败，耗时: {elapsed:.2f}秒，错误: {str(e)}")
        raise

if __name__ == "__main__":
    # 测试数据库连接
    success, info = test_connection()
    print(f"连接测试结果: {'成功' if success else '失败'}")
    print(f"详细信息: {info}")
    
    # 测试查询
    if success:
        try:
            data = execute_query("SELECT TOP 5 * FROM INFORMATION_SCHEMA.TABLES")
            print(f"查询结果 (前5张表):")
            for row in data:
                print(row)
        except Exception as e:
            print(f"查询测试失败: {str(e)}")