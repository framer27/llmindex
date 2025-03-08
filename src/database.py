import os
import json
import logging
import time
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool

# 设置日志
logger = logging.getLogger('database')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

# 全局连接池
_DB_ENGINE_POOL = None

def load_db_config():
    """加载数据库配置，支持YAML和JSON格式"""
    # 首先尝试YAML格式
    yaml_config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'db_config.yaml')
    # 然后尝试JSON格式
    json_config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'db_config.json')
    
    # 默认配置
    default_config = {
        "server": "localhost",
        "database": "master",
        "username": "sa",
        "password": "yourpassword",
        "driver": "ODBC Driver 17 for SQL Server"
    }
    
    # 检查配置目录是否存在
    config_dir = os.path.dirname(yaml_config_path)
    if not os.path.exists(config_dir):
        os.makedirs(config_dir)
        logger.info(f"创建配置目录: {config_dir}")
    
    # 优先尝试读取YAML格式配置
    if os.path.exists(yaml_config_path):
        try:
            # 尝试导入yaml模块
            try:
                import yaml
            except ImportError:
                logger.warning("未安装PyYAML库，无法读取YAML配置文件。建议安装: pip install pyyaml")
                logger.info("尝试直接解析YAML文件...")
                # 尝试手动解析YAML
                with open(yaml_config_path, 'r', encoding='utf-8') as f:
                    yaml_content = f.read()
                    config = {}
                    current_section = None
                    for line in yaml_content.splitlines():
                        line = line.strip()
                        if not line or line.startswith('#'):
                            continue
                        if ':' in line and not line.startswith(' '):
                            # 主节点
                            section_name = line.split(':', 1)[0].strip()
                            current_section = {}
                            config[section_name] = current_section
                        elif ':' in line and line.startswith(' ') and current_section is not None:
                            # 子项
                            key, value = line.strip().split(':', 1)
                            current_section[key.strip()] = value.strip()
                    
                    # 如果找到database节点，则使用它
                    if 'database' in config and isinstance(config['database'], dict):
                        db_config = config['database']
                        # 转换YAML格式到我们程序使用的格式
                        result_config = {
                            "server": db_config.get('host', 'localhost'),
                            "database": db_config.get('database', 'master'),
                            "username": db_config.get('username', 'sa'),
                            "password": db_config.get('password', ''),
                            "driver": db_config.get('driver', 'ODBC Driver 17 for SQL Server')
                        }
                        logger.info(f"成功手动解析YAML配置文件: {yaml_config_path}")
                        return result_config
            else:
                # 如果PyYAML库可用，使用它来解析
                with open(yaml_config_path, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f)
                    if 'database' in config and isinstance(config['database'], dict):
                        db_config = config['database']
                        # 转换YAML格式到我们程序使用的格式
                        result_config = {
                            "server": db_config.get('host', 'localhost'),
                            "database": db_config.get('database', 'master'),
                            "username": db_config.get('username', 'sa'),
                            "password": db_config.get('password', ''),
                            "driver": db_config.get('driver', 'ODBC Driver 17 for SQL Server')
                        }
                        logger.info(f"使用YAML配置文件: {yaml_config_path}")
                        return result_config
        except Exception as e:
            logger.warning(f"解析YAML配置文件失败: {str(e)}")
    
    # 尝试读取JSON格式配置
    if os.path.exists(json_config_path):
        try:
            with open(json_config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            logger.info(f"使用JSON配置文件: {json_config_path}")
            return config
        except Exception as e:
            logger.warning(f"解析JSON配置文件失败: {str(e)}")
    
    # 如果都失败了，创建默认配置文件
    if not os.path.exists(json_config_path):
        try:
            with open(json_config_path, 'w', encoding='utf-8') as f:
                json.dump(default_config, f, indent=4, ensure_ascii=False)
            logger.info(f"已创建默认数据库配置文件: {json_config_path}")
            logger.warning("请修改默认数据库配置文件以匹配您的环境")
            print(f"\n注意: 已创建默认数据库配置文件: {json_config_path}")
            print("请修改该文件以匹配您的实际数据库配置\n")
        except Exception as e:
            logger.error(f"创建默认配置文件失败: {str(e)}")
    
    # 如果找不到配置文件或解析失败，使用默认配置
    logger.info("使用默认数据库配置")
    return default_config

def get_db_engine(force_new=False):
    """
    获取数据库引擎，优先使用连接池
    
    参数:
        force_new: 是否强制创建新连接，而不使用池中的连接
    """
    global _DB_ENGINE_POOL
    
    # 如果强制创建新连接，或连接池尚未初始化
    if force_new or _DB_ENGINE_POOL is None:
        # 加载配置
        db_config = load_db_config()
        
        # 构建连接字符串
        # 首先检查是否有完整的dialect+driver配置（YAML格式可能有）
        if 'dialect' in db_config:
            dialect = db_config['dialect']
        else:
            dialect = 'mssql+pyodbc'
            
        # 获取服务器地址
        server = db_config.get('server', db_config.get('host', 'localhost'))
        
        # 获取数据库名
        database = db_config.get('database', 'master')
        
        # 获取认证信息
        if 'trusted_connection' in db_config and db_config['trusted_connection'].lower() in ('yes', 'true'):
            # Windows 认证
            auth_part = "trusted_connection=yes"
        else:
            # SQL 认证
            username = db_config.get('username', 'sa')
            password = str(db_config.get('password', ''))
            auth_part = f"{username}:{password}@"
        
        # 获取端口（如果指定）
        port = db_config.get('port', '')
        port_part = f":{port}" if port else ""
        
        # 获取驱动程序
        driver = db_config.get('driver', 'ODBC Driver 17 for SQL Server')
        driver_part = f"?driver={driver}"
        
        # 完整连接字符串
        if 'trusted_connection' in db_config and db_config['trusted_connection'].lower() in ('yes', 'true'):
            # Windows 认证格式不同
            connection_string = f"{dialect}://{server}{port_part}/{database}?{auth_part}&driver={driver}"
        else:
            # SQL 认证
            connection_string = f"{dialect}://{auth_part}{server}{port_part}/{database}{driver_part}"
            
        logger.debug(f"连接字符串（已隐藏密码）: {connection_string.replace(password, '********') if password else connection_string}")
        
        # 记录开始时间
        start_time = time.time()
        
        if force_new:
            # 创建单独的引擎，不使用池
            logger.info("创建新的数据库连接...")
            engine = create_engine(connection_string)
            elapsed = time.time() - start_time
            logger.info(f"新连接创建完成，耗时: {elapsed:.2f}秒")
            return engine
        else:
            # 创建或返回连接池
            logger.info("初始化数据库连接池...")
            _DB_ENGINE_POOL = create_engine(
                connection_string,
                poolclass=QueuePool,
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
                pool_recycle=1800,
                pool_pre_ping=True
            )
            elapsed = time.time() - start_time
            logger.info(f"连接池初始化完成，耗时: {elapsed:.2f}秒")
    
    return _DB_ENGINE_POOL

def get_pool_status():
    """获取连接池状态"""
    global _DB_ENGINE_POOL
    
    if _DB_ENGINE_POOL is None:
        return {"status": "未初始化"}
    
    try:
        pool = _DB_ENGINE_POOL.pool
        return {
            "status": "活跃",
            "size": pool.size(),
            "checkedin": pool.checkedin(),
            "checkedout": pool.checkedout(),
            "overflow": pool.overflow()
        }
    except Exception as e:
        return {"status": "错误", "error": str(e)}

def execute_query(query, params=None):
    """执行SQL查询"""
    engine = get_db_engine()
    
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

def test_connection():
    """测试数据库连接"""
    try:
        engine = get_db_engine()
        start_time = time.time()
        
        with engine.connect() as connection:
            result = connection.execute(text("SELECT @@VERSION"))
            version = result.scalar()
            
        elapsed = time.time() - start_time
        logger.info(f"数据库连接测试成功，耗时: {elapsed:.2f}秒")
        logger.info(f"数据库版本: {version}")
        
        return True, version
    except Exception as e:
        logger.error(f"数据库连接测试失败: {str(e)}")
        return False, str(e)

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