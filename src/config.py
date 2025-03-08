"""
全局配置文件，包含应用程序的各种配置参数
"""
import os

# 基本路径配置
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_DIR = os.path.join(ROOT_DIR, 'config')
CACHE_DIR = os.path.join(ROOT_DIR, 'cache')
DOTENV_PATH = os.path.join(ROOT_DIR, '.env')

# 确保目录存在
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)
if not os.path.exists(CONFIG_DIR):
    os.makedirs(CONFIG_DIR)

# 缓存配置
CACHE_VERSION = "1.0"

# 嵌入模型配置
EMBED_MODEL = 'local:BAAI/bge-small-en-v1.5'

# 数据库连接池配置
DB_POOL_SIZE = 5
DB_MAX_OVERFLOW = 10
DB_POOL_TIMEOUT = 30
DB_POOL_RECYCLE = 1800

# SQL验证配置
SQL_FORBIDDEN_KEYWORDS = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'EXEC', 'sp_']

# LLM提示模板
SQL_PROMPT_TEMPLATE = """
你是SQL Server专家，请将以下自然语言问题转换为SQL查询语句。

数据库表结构:
{schema}

用户问题: {query}

请生成标准的SQL SELECT查询语句，遵循以下规则：
1. 严格使用上述schema中提供的表名和字段名
2. 只能使用SELECT语句，禁止使用任何DML语句(如INSERT/UPDATE/DELETE)
3. 禁止使用存储过程调用，不要使用EXEC或sp_前缀的函数
4. 如果需要限制结果数量，请使用"SELECT TOP N"语法
5. 请确保SQL语法正确且字段名匹配
6. 直接返回SQL语句，不要使用```sql代码块标记
7. 不要包含任何解释文字

SQL查询:
"""

# 表关键词映射配置
TABLE_KEYWORD_MAP = {
    'WmsDeliverynoteDetail': ['送货单', '送货明细', '送货单明细', '送货'],
    'MesMachineMaintain': ['设备维修', '维修记录', '设备维修记录', '维修'],
} 