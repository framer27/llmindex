from llama_index.core.query_engine import SQLTableRetrieverQueryEngine
from llama_index.llms.deepseek import DeepSeek
from llama_index.core.objects import SQLTableSchema
from llama_index.core import SQLDatabase
from llama_index.core.retrievers import SQLRetriever
from functools import lru_cache
from sqlalchemy import inspect, create_engine
from sqlalchemy.pool import QueuePool
from llama_index.core import Settings
import logging
import json
import os
import joblib
import hashlib
import time

# 全局日志配置标志
_LOGGING_CONFIGURED = False

# 缓存版本 - 如果更改了代码逻辑，请增加此版本号以使缓存失效
CACHE_VERSION = "1.0"
CACHE_DIR = "cache"

# 配置全局日志
def setup_logging():
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

# 初始化日志
setup_logging()

# 全局数据库连接池
_ENGINE_POOL = None

def get_db_engine(db_config=None):
    """获取数据库连接池中的引擎"""
    global _ENGINE_POOL
    
    if _ENGINE_POOL is None:
        if db_config is None:
            # 如果没有提供配置，则加载默认配置
            from src.database import load_db_config
            db_config = load_db_config()
            
        # 创建连接URL
        password = str(db_config['password']) if 'password' in db_config else ""
        server = db_config['server']
        database = db_config['database']
        username = db_config['username']
        
        # 用于性能监控的日志记录器
        logger = logging.getLogger('QueryBot')
        pool_start = time.time()
        
        # 构建连接字符串并创建带有连接池的引擎
        connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
        _ENGINE_POOL = create_engine(
            connection_string,
            poolclass=QueuePool,
            pool_size=5,  # 初始连接数
            max_overflow=10,  # 允许的额外连接数
            pool_timeout=30,  # 等待连接的超时时间
            pool_recycle=1800,  # 连接回收时间（秒）
            pool_pre_ping=True  # 使用前检查连接是否有效
        )
        pool_time = time.time() - pool_start
        logger.info(f'[性能] 创建数据库连接池耗时: {pool_time:.2f}秒')
        logger.info(f'数据库连接池初始化完成: 大小={5}, 最大溢出={10}')
    
    return _ENGINE_POOL

class QueryBot:
    def __init__(self, api_key):
        # 加载数据库配置
        from src.database import load_db_config
        self.db_config = load_db_config()
        self.db_password = str(self.db_config['password']) if 'password' in self.db_config else ""
        
        # 获取日志记录器但不添加新的处理器
        self.logger = logging.getLogger('QueryBot')
        
        # 性能统计收集器
        self.performance_stats = {
            'initialization': {},
            'queries': []
        }

        # 设置全局嵌入模型
        Settings.embed_model = 'local:BAAI/bge-small-en-v1.5'
        
        self.llm = DeepSeek(
            model="deepseek-chat",
            api_key=api_key
        )
        
        # 精简的提示模板
        self.prompt_template = """
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
        
        # 确保缓存目录存在
        if not os.path.exists(CACHE_DIR):
            os.makedirs(CACHE_DIR)
            
        # 初始化连接池
        self.logger.info("正在初始化数据库连接...")
        _ = get_db_engine()
            
    def _get_cache_key(self, tables_data_path):
        """生成缓存键，基于表结构文件的内容哈希和缓存版本"""
        # 读取表结构文件内容
        with open(tables_data_path, 'rb') as f:
            file_content = f.read()
            
        # 计算表结构文件的SHA256哈希值
        tables_hash = hashlib.sha256(file_content).hexdigest()
        
        # 组合缓存键
        cache_key = f"engine_cache_v{CACHE_VERSION}_{tables_hash}"
        return cache_key
        
    def _get_cache_path(self, cache_key):
        """获取缓存文件路径"""
        return os.path.join(CACHE_DIR, f"{cache_key}.joblib")
        
    def _check_cache(self, tables_data_path):
        """检查是否存在有效的缓存"""
        cache_key = self._get_cache_key(tables_data_path)
        cache_path = self._get_cache_path(cache_key)
        
        if os.path.exists(cache_path):
            self.logger.info(f"找到有效缓存: {cache_path}")
            return cache_path
        else:
            self.logger.info("未找到有效缓存，将重新构建查询引擎")
            return None
            
    def _save_cache(self, cache_data, tables_data_path):
        """保存缓存数据"""
        cache_key = self._get_cache_key(tables_data_path)
        cache_path = self._get_cache_path(cache_key)
        
        self.logger.info(f"正在保存查询引擎缓存到: {cache_path}")
        start_time = time.time()
        
        try:
            # 创建可序列化的缓存数据步骤
            prepare_start = time.time()
            serializable_cache = {
                'tables_data': self.tables_data,
                'schema': self.schema,
                'cached_time': time.time(),
                'cache_version': CACHE_VERSION
            }
            
            # 如果存在table_retriever，尝试缓存嵌入向量
            if hasattr(self, '_last_table_embeddings') and self._last_table_embeddings:
                serializable_cache['table_embeddings'] = self._last_table_embeddings
                self.logger.info(f"正在缓存{len(self._last_table_embeddings)}个表的向量嵌入")
            prepare_time = time.time() - prepare_start
            self.logger.info(f'[性能] 准备缓存数据耗时: {prepare_time:.2f}秒')
            
            # 保存缓存文件
            dump_start = time.time()
            joblib.dump(serializable_cache, cache_path)
            dump_time = time.time() - dump_start
            self.logger.info(f'[性能] 写入缓存文件耗时: {dump_time:.2f}秒')
            
            # 记录总时间
            elapsed = time.time() - start_time
            self.logger.info(f"缓存保存成功，总耗时: {elapsed:.2f}秒")
            
            # 输出详细的性能统计
            self.logger.info("===== 缓存保存性能明细 =====")
            self.logger.info(f"准备缓存数据: {prepare_time:.2f}秒")
            self.logger.info(f"写入缓存文件: {dump_time:.2f}秒")
            self.logger.info(f"总耗时: {elapsed:.2f}秒")
            self.logger.info("=========================")
            
            return True
        except Exception as e:
            self.logger.error(f"缓存保存失败: {str(e)}")
            # 如果保存失败，尝试删除可能的部分缓存文件
            if os.path.exists(cache_path):
                try:
                    os.remove(cache_path)
                except:
                    pass
            return False
            
    def _load_cache(self, cache_path):
        """加载缓存数据"""
        self.logger.info(f"正在加载查询引擎缓存: {cache_path}")
        start_time = time.time()
        
        try:
            # 加载缓存文件
            load_start = time.time()
            cache_data = joblib.load(cache_path)
            load_time = time.time() - load_start
            self.logger.info(f'[性能] 读取缓存文件耗时: {load_time:.2f}秒')
            
            # 验证缓存
            validate_start = time.time()
            # 检查缓存版本
            if cache_data.get('cache_version') != CACHE_VERSION:
                self.logger.warning(f"缓存版本不匹配: 期望 {CACHE_VERSION}，实际 {cache_data.get('cache_version')}")
                return None
                
            # 计算缓存年龄
            cache_age = time.time() - cache_data.get('cached_time', 0)
            self.logger.info(f"缓存年龄: {cache_age/3600:.1f}小时")
            validate_time = time.time() - validate_start
            self.logger.info(f'[性能] 验证缓存耗时: {validate_time:.2f}秒')
            
            # 记录总时间
            total_time = time.time() - start_time
            self.logger.info(f"缓存加载成功，总耗时: {total_time:.2f}秒")
            
            # 输出详细的性能统计
            self.logger.info("===== 缓存加载性能明细 =====")
            self.logger.info(f"读取缓存文件: {load_time:.2f}秒")
            self.logger.info(f"验证缓存: {validate_time:.2f}秒")
            self.logger.info(f"总耗时: {total_time:.2f}秒")
            self.logger.info("=========================")
            
            return cache_data
        except Exception as e:
            self.logger.error(f"缓存加载失败: {str(e)}")
            # 如果加载失败，删除可能已损坏的缓存文件
            try:
                os.remove(cache_path)
            except:
                pass
            return None

    def _validate_columns(self, sql, tables_data):
        """验证SQL语句中的字段是否实际存在"""
        try:
            from sqlparse import parse
            from sqlparse.sql import IdentifierList, Identifier, Token
            import re
            
            self.logger.debug(f'开始验证SQL字段: {sql}')
            
            # SQL关键字列表 - 常见的SQL Server关键字
            sql_keywords = {
                'SELECT', 'FROM', 'WHERE', 'ORDER', 'BY', 'GROUP', 'HAVING', 'JOIN',
                'INNER', 'OUTER', 'LEFT', 'RIGHT', 'FULL', 'ON', 'AS', 'AND', 'OR',
                'NOT', 'IN', 'BETWEEN', 'LIKE', 'IS', 'NULL', 'TOP', 'DISTINCT', 
                'ALL', 'UNION', 'EXCEPT', 'INTERSECT', 'WITH', 'CASE', 'WHEN', 'THEN',
                'ELSE', 'END', 'ASC', 'DESC', 'LIMIT', 'OFFSET'
            }
            
            # 获取所有表名，用于排除表名被当作字段名验证
            table_names = {table['name'] for table in tables_data}
            
            # 解析SQL可能会失败，使用try-except包装整个过程
            parsed = parse(sql)
            if not parsed:
                self.logger.warning('SQL解析失败，跳过字段验证')
                return  # 解析失败时直接返回，不中断执行
                
            stmt = parsed[0]
            
            # 提取所有可能的字段名
            potential_columns = []
            for token in stmt.tokens:
                if isinstance(token, (IdentifierList, Identifier)):
                    # 尝试获取token的真实名称
                    try:
                        col_name = token.get_real_name()
                        if col_name:
                            # 排除SQL关键字和表名
                            if (col_name.upper() not in sql_keywords and 
                                col_name not in table_names):
                                potential_columns.append(col_name)
                    except Exception as e:
                        self.logger.debug(f'无法获取标识符名称: {str(e)}')
                
                # 处理表达式中的字段名，如SELECT a.column_name
                elif hasattr(token, 'tokens'):
                    for subtoken in token.tokens:
                        if isinstance(subtoken, Identifier):
                            try:
                                col_name = subtoken.get_real_name()
                                if col_name:
                                    # 去除可能的表名前缀 (如 table.column 中的column)
                                    if '.' in col_name:
                                        _, col_name = col_name.split('.', 1)
                                    
                                    # 排除SQL关键字和表名
                                    if (col_name.upper() not in sql_keywords and 
                                        col_name not in table_names):
                                        potential_columns.append(col_name)
                            except Exception as e:
                                self.logger.debug(f'无法获取子标识符名称: {str(e)}')
            
            # 对每个提取的字段名进行验证
            for column_name in potential_columns:
                self.logger.debug(f'检查字段: {column_name}')
                # 检查字段是否存在于任何表中
                if not any(column['name'] == column_name for table in tables_data for column in table.get('columns', [])):
                    self.logger.warning(f'未找到匹配字段: {column_name}，但将继续执行')
            
            self.logger.debug('字段验证完成')
            
        except Exception as e:
            # 字段验证失败不应该阻止查询执行
            self.logger.warning(f'字段验证过程出错: {str(e)}')
            self.logger.warning('继续执行查询，但可能存在字段不匹配风险')
    
    def _validate_sql(self, sql):
        """验证SQL语句，防止危险操作"""
        import re  # 将re导入移到函数开头，确保在任何情况下都可用
        start_time = time.time()
        
        forbidden_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'EXEC', 'sp_']
        self.logger.debug(f'开始验证SQL语句安全性: {sql}')
        
        # 预处理SQL，移除可能的markdown代码块标记
        clean_start = time.time()
        cleaned_sql = sql
        
        # 移除代码块标记
        if '```' in cleaned_sql:
            # 移除所有```sql标记
            cleaned_sql = re.sub(r'```\w*\n', '', cleaned_sql)  # 移除开头的```sql\n
            cleaned_sql = re.sub(r'```\s*$', '', cleaned_sql)  # 移除结尾的```
        
        # 替换多个换行符为单个空格，便于检测开头的SELECT
        compact_sql = ' '.join([line.strip() for line in cleaned_sql.split('\n') if line.strip()])
        clean_time = time.time() - clean_start
        
        self.logger.debug(f'清理后的SQL: {cleaned_sql[:50]}...')
        self.logger.debug(f'紧凑的SQL: {compact_sql[:50]}...')
        
        # 验证SQL是否以SELECT开头
        validate_start = time.time()
        if not compact_sql.upper().startswith('SELECT'):
            self.logger.error('SQL验证失败：非SELECT语句')
            raise ValueError("仅允许SELECT查询，请检查语句是否以SELECT开头")
        
        # 单独检查禁用关键字
        detected_keywords = []
        for kw in forbidden_keywords:
            # 使用单词边界匹配，避免误报
            pattern = r'\b' + kw + r'\b'
            if re.search(pattern, cleaned_sql.upper()):
                detected_keywords.append(kw)
                self.logger.error(f'SQL中检测到禁用关键字: {kw}')
        
        if detected_keywords:
            self.logger.error(f'SQL验证失败：包含禁用关键字: {", ".join(detected_keywords)}')
            raise ValueError(f"SQL包含禁用关键字: {', '.join(detected_keywords)}")
        validate_time = time.time() - validate_start
        
        # 记录总时间
        total_time = time.time() - start_time
        self.logger.debug(f'[性能] SQL验证总耗时: {total_time:.2f}秒 (清理: {clean_time:.2f}秒, 验证: {validate_time:.2f}秒)')
        
        # 返回清理后的SQL
        self.logger.debug('SQL安全性验证通过')
        return cleaned_sql
    
    def build_engine(self, engine=None, use_cache=True, force_rebuild=False, tables_data_path='data/tables.json'):
        """构建查询引擎"""
        self.logger.info('开始构建查询引擎')
        init_start_time = time.time()
        
        # 初始化性能统计
        perf_stats = {}
        
        # 使用连接池而不是新建连接
        if engine is None:
            engine = get_db_engine()
        
        self.sql_database = SQLDatabase(engine=engine)
        
        # 检查缓存
        cache_path = None
        if use_cache and not force_rebuild:
            cache_path = self._check_cache(tables_data_path)
        
        # 如果存在缓存且未强制重建，则尝试加载缓存
        cached_embeddings = None
        if cache_path and use_cache and not force_rebuild:
            # 加载缓存
            cache_data = self._load_cache(cache_path)
            
            if cache_data:
                # 从缓存恢复数据
                self.tables_data = cache_data['tables_data']
                self.schema = cache_data['schema']
                
                # 提取缓存的嵌入向量
                if 'table_embeddings' in cache_data:
                    cached_embeddings = cache_data['table_embeddings']
                    self.logger.info(f"从缓存加载了{len(cached_embeddings)}个表的嵌入向量")
                
                # 创建查询引擎
                engine_perf = self._create_query_engine(engine, cached_embeddings)
                
                # 计算初始化时间
                init_time = time.time() - init_start_time
                self.logger.info(f'查询引擎从缓存加载完成，总耗时: {init_time:.2f}秒')
                perf_stats['total_time'] = init_time
                perf_stats['from_cache'] = True
                
                # 保存性能统计
                self.performance_stats['initialization'] = perf_stats
                
                return self
        
        # 如果没有缓存或强制重建，则重新构建查询引擎
        self.logger.info('从头构建查询引擎')
        
        # 从tables.json加载表结构
        from src.schema_loader import load_schema_from_json
        with open(tables_data_path, 'r', encoding='utf-8') as f:
            self.tables_data = json.load(f)
        self.schema = load_schema_from_json(tables_data_path)
        
        # 创建查询引擎
        engine_perf = self._create_query_engine(engine)
        
        # 保存缓存
        if use_cache:
            self._save_cache(None, tables_data_path)
        
        # 计算总初始化时间
        init_time = time.time() - init_start_time
        self.logger.info(f'查询引擎构建完成，总耗时: {init_time:.2f}秒')
        perf_stats['total_time'] = init_time
        perf_stats['from_cache'] = False
        
        # 保存性能统计
        self.performance_stats['initialization'] = perf_stats
        
        return self
        
    def _create_query_engine(self, engine, cached_embeddings=None):
        """创建查询引擎，可复用缓存的嵌入向量
        
        参数:
            engine: SQLAlchemy引擎
            cached_embeddings: 缓存的表嵌入向量，格式为{table_name: embedding_vector}
        
        返回:
            性能统计信息字典
        """
        # 记录开始时间
        engine_start = time.time()
        
        # 收集性能统计信息
        perf_stats = {}
        
        # 创建SQLTableSchema对象
        schemas_start = time.time()
        table_schemas = [SQLTableSchema(table_name=table['name']) for table in self.tables_data]
        schemas_time = time.time() - schemas_start
        self.logger.info(f'[性能] 创建表schema耗时: {schemas_time:.2f}秒')
        self.logger.debug(f'创建的表schema数量: {len(table_schemas)}')
        perf_stats['create_schemas'] = schemas_time
        
        # 创建表检索器
        retriever_start = time.time()
        retriever_stats = {}
        
        if cached_embeddings:
            # 使用缓存的嵌入向量
            self.logger.info(f"使用缓存的表嵌入向量({len(cached_embeddings)}个)")
            
            # 保存临时引用以便在保存缓存时重复使用
            self._last_table_embeddings = cached_embeddings
            
            # 创建具有预计算嵌入向量的检索器
            table_retriever = self._create_retriever_with_embeddings(
                self.sql_database, 
                table_schemas, 
                cached_embeddings
            )
            retriever_stats['used_cache'] = True
            retriever_stats['vector_count'] = len(cached_embeddings)
        else:
            # 从头创建嵌入向量
            self.logger.info("重新计算表嵌入向量")
            
            # 创建标准检索器
            embed_model_start = time.time()
            self.logger.info("正在加载嵌入模型...")
            table_retriever = SQLRetriever(
                sql_database=self.sql_database,
                table_schemas=table_schemas,
                embed_model='local'
            )
            embed_model_time = time.time() - embed_model_start
            self.logger.info(f'[性能] 加载嵌入模型并计算表嵌入向量耗时: {embed_model_time:.2f}秒')
            retriever_stats['embed_model_time'] = embed_model_time
            retriever_stats['used_cache'] = False
            
            # 提取并保存嵌入向量，以便后续保存到缓存
            self._last_table_embeddings = self._extract_retriever_embeddings(table_retriever)
            retriever_stats['vector_count'] = len(self._last_table_embeddings) if self._last_table_embeddings else 0
            
        retriever_time = time.time() - retriever_start
        self.logger.info(f'[性能] 创建表检索器总耗时: {retriever_time:.2f}秒')
        perf_stats['create_retriever'] = retriever_time
        perf_stats['retriever_details'] = retriever_stats
        
        # 创建提示模板
        prompt_start = time.time()
        from llama_index.core.prompts import PromptTemplate
        self.text_to_sql_prompt = PromptTemplate(self.prompt_template)
        prompt_time = time.time() - prompt_start
        self.logger.info(f'[性能] 创建提示模板耗时: {prompt_time:.2f}秒')
        perf_stats['create_prompt'] = prompt_time
        
        # 尝试创建查询引擎
        query_engine_start = time.time()
        try:
            self.logger.info("正在初始化SQLTableRetrieverQueryEngine...")
            self.query_engine = SQLTableRetrieverQueryEngine(
                sql_database=self.sql_database,
                table_retriever=table_retriever,
                text_to_sql_prompt=self.text_to_sql_prompt,
                llm=self.llm
            )
            self.logger.debug('SQLTableRetrieverQueryEngine初始化成功')
            perf_stats['engine_created'] = True
        except Exception as e:
            self.logger.error(f'创建SQLTableRetrieverQueryEngine失败: {str(e)}')
            self.logger.info('将使用直接LLM方法作为备选')
            self.query_engine = None
            perf_stats['engine_created'] = False
            perf_stats['engine_error'] = str(e)
        query_engine_time = time.time() - query_engine_start
        self.logger.info(f'[性能] 创建SQLTableRetrieverQueryEngine耗时: {query_engine_time:.2f}秒')
        perf_stats['create_query_engine'] = query_engine_time
        
        # 记录总时间
        total_time = time.time() - engine_start
        perf_stats['total_time'] = total_time
        
        # 输出详细的性能统计
        self.logger.info("===== 引擎创建性能明细 =====")
        self.logger.info(f"创建表schema: {schemas_time:.2f}秒")
        self.logger.info(f"创建表检索器: {retriever_time:.2f}秒")
        if not cached_embeddings and 'embed_model_time' in retriever_stats:
            self.logger.info(f"└─ 加载嵌入模型和计算向量: {retriever_stats['embed_model_time']:.2f}秒")
        self.logger.info(f"创建提示模板: {prompt_time:.2f}秒")
        self.logger.info(f"创建查询引擎: {query_engine_time:.2f}秒")
        self.logger.info(f"总耗时: {total_time:.2f}秒")
        self.logger.info("============================")
        
        # 返回性能统计信息
        return perf_stats
    
    def _extract_retriever_embeddings(self, retriever):
        """从表检索器中提取嵌入向量"""
        embeddings = {}
        
        # 尝试提取嵌入向量 - 这依赖于SQLRetriever的内部结构
        try:
            if hasattr(retriever, '_table_node_mapping'):
                for table_name, node in retriever._table_node_mapping.items():
                    if hasattr(node, 'embedding'):
                        embeddings[table_name] = node.embedding
                        
            self.logger.info(f"成功提取{len(embeddings)}个表的嵌入向量")
        except Exception as e:
            self.logger.warning(f"提取嵌入向量失败: {str(e)}")
            
        return embeddings
        
    def _create_retriever_with_embeddings(self, sql_database, table_schemas, cached_embeddings):
        """使用缓存的嵌入向量创建表检索器"""
        start_time = time.time()
        from llama_index.core.schema import TextNode
        
        self.logger.info("开始创建表检索器（使用缓存向量）...")
        
        # 创建基础检索器时间
        basic_retriever_start = time.time()
        retriever = SQLRetriever(
            sql_database=sql_database,
            table_schemas=table_schemas,
            embed_model='local'
        )
        basic_retriever_time = time.time() - basic_retriever_start
        self.logger.info(f'[性能] 创建基础检索器耗时: {basic_retriever_time:.2f}秒')
        
        # 尝试将缓存的嵌入向量注入到检索器中
        inject_start = time.time()
        try:
            # 重置节点映射
            retriever._table_node_mapping = {}
            
            # 为每个表创建带有缓存嵌入向量的节点
            for schema in table_schemas:
                table_name = schema.table_name
                if table_name in cached_embeddings:
                    # 创建节点并设置嵌入向量
                    node = TextNode(text=table_name)
                    node.embedding = cached_embeddings[table_name]
                    # 添加到检索器的映射中
                    retriever._table_node_mapping[table_name] = node
                    
            self.logger.info(f"成功将{len(retriever._table_node_mapping)}个缓存嵌入向量注入到检索器")
            inject_time = time.time() - inject_start
            self.logger.info(f'[性能] 注入缓存嵌入向量耗时: {inject_time:.2f}秒')
            
        except Exception as e:
            self.logger.warning(f"注入缓存嵌入向量失败: {str(e)}")
            # 如果失败，返回标准检索器
            self.logger.info("回退到标准检索器创建...")
            fallback_start = time.time()
            retriever = SQLRetriever(
                sql_database=sql_database,
                table_schemas=table_schemas,
                embed_model='local'
            )
            fallback_time = time.time() - fallback_start
            self.logger.info(f'[性能] 创建回退检索器耗时: {fallback_time:.2f}秒')
            
        # 总时间
        total_time = time.time() - start_time
        self.logger.info(f'[性能] 创建检索器总耗时: {total_time:.2f}秒')
        
        return retriever

    def _direct_llm_query(self, query_str):
        """直接使用LLM生成SQL查询"""
        self.logger.info('使用备选方法：直接通过LLM生成SQL')
        start_time = time.time()
        
        # 为当前查询选择最相关的表
        table_select_start = time.time()
        relevant_tables = self._select_relevant_tables(query_str)
        table_select_time = time.time() - table_select_start
        self.logger.info(f'[性能] 选择相关表耗时: {table_select_time:.2f}秒')
        
        # 构建精简的schema字符串
        schema_build_start = time.time()
        compact_schema = self._build_compact_schema(relevant_tables)
        schema_build_time = time.time() - schema_build_start
        self.logger.info(f'[性能] 构建schema耗时: {schema_build_time:.2f}秒')
        self.logger.debug(f'精简后的schema长度: {len(compact_schema)}字符')
        
        # 构建包含schema的提示
        prompt_build_start = time.time()
        prompt = self.prompt_template.format(
            schema=compact_schema,
            query=query_str
        )
        prompt_build_time = time.time() - prompt_build_start
        self.logger.info(f'[性能] 构建提示耗时: {prompt_build_time:.2f}秒')
        self.logger.debug(f'发送到LLM的提示开头:\n{prompt[:500]}...')
        self.logger.debug(f'提示总长度: {len(prompt)}字符')
        
        # 调用LLM生成SQL
        self.logger.debug('开始调用LLM...')
        llm_call_start = time.time()
        response = self.llm.complete(prompt)
        raw_sql = response.text.strip()
        llm_call_time = time.time() - llm_call_start
        self.logger.info(f'[性能] LLM调用耗时: {llm_call_time:.2f}秒')
        
        # 记录完整的原始SQL (转换为字符代码可以查看不可见字符)
        self.logger.debug(f'LLM返回的原始响应 [完整]: {raw_sql}')
        self.logger.debug(f'原始SQL长度: {len(raw_sql)}字符')
        self.logger.debug(f'原始SQL字符代码: {[ord(c) for c in raw_sql[:20]]}...')
        
        # 打印更详细的编码信息，帮助诊断问题
        if any(ord(c) > 127 for c in raw_sql):
            self.logger.warning('SQL包含非ASCII字符，可能导致解析问题')
            non_ascii = [(i, c, ord(c)) for i, c in enumerate(raw_sql) if ord(c) > 127]
            self.logger.warning(f'非ASCII字符位置和编码: {non_ascii}')
        
        # 验证SQL (会移除代码块标记并返回清理后的SQL)
        validate_start = time.time()
        cleaned_sql = self._validate_sql(raw_sql)
        validate_time = time.time() - validate_start
        self.logger.info(f'[性能] SQL验证耗时: {validate_time:.2f}秒')
        self.logger.debug(f'清理并验证后的SQL [完整]: {cleaned_sql}')
        
        # 执行SQL查询
        execute_start = time.time()
        try:
            from pandas import DataFrame
            self.logger.debug(f'开始执行SQL: {cleaned_sql}')
            
            # 使用连接池状态监控
            try:
                if hasattr(self.sql_database.engine.pool, 'status'):
                    pool_status = self.sql_database.engine.pool.status()
                    self.logger.debug(f'连接池状态: 使用中={pool_status.get("checkedin", 0)}, 可用={pool_status.get("checkedout", 0)}')
            except Exception as pool_error:
                self.logger.debug(f'获取连接池状态出错: {str(pool_error)}')
                
            # 执行SQL查询
            result = self.sql_database.run_sql(cleaned_sql)
            
            # 确保结果是字符串格式
            if isinstance(result, DataFrame):
                result_str = result.to_string()
                self.logger.debug(f'SQL执行结果(DataFrame): {len(result)}行')
            elif isinstance(result, (list, dict)):
                # 对于列表或字典类型的结果，使用json格式化
                import json
                result_str = json.dumps(result, ensure_ascii=False, indent=2)
                self.logger.debug(f'SQL执行结果(JSON): {len(result_str)}字符')
            else:
                # 对于其他类型，安全地转换为字符串
                result_str = str(result)
                self.logger.debug(f'SQL执行结果: {result_str[:100]}...')
                
            execute_time = time.time() - execute_start
            self.logger.info(f'[性能] SQL执行耗时: {execute_time:.2f}秒')
            
            # 记录总耗时
            total_time = time.time() - start_time
            
            # 输出详细的性能统计
            self.logger.info("===== SQL生成执行性能明细 =====")
            self.logger.info(f"选择相关表: {table_select_time:.2f}秒")
            self.logger.info(f"构建schema: {schema_build_time:.2f}秒")
            self.logger.info(f"构建提示: {prompt_build_time:.2f}秒")
            self.logger.info(f"LLM调用: {llm_call_time:.2f}秒")
            self.logger.info(f"SQL验证: {validate_time:.2f}秒")
            self.logger.info(f"SQL执行: {execute_time:.2f}秒")
            self.logger.info(f"总耗时: {total_time:.2f}秒")
            self.logger.info("===============================")
            
            return cleaned_sql, result_str
        except Exception as e:
            execute_time = time.time() - execute_start
            self.logger.error(f'执行SQL失败: {str(e)}')
            self.logger.info(f'[性能] SQL执行失败，耗时: {execute_time:.2f}秒')
            raise ValueError(f"SQL执行失败: {str(e)}")
        
    def _select_relevant_tables(self, query_str):
        """选择与查询相关的表"""
        self.logger.debug(f'为查询选择相关表: {query_str}')
        
        # 表别名/关键词映射配置 - 可以扩展为从配置文件加载
        table_keyword_map = {
            'WmsDeliverynoteDetail': ['送货单', '送货明细', '送货单明细', '送货'],
            'MesMachineMaintain': ['设备维修', '维修记录', '设备维修记录', '维修'],
            # 可以根据需要添加更多表映射...
        }
        
        # 步骤1: 基于关键词映射直接查找匹配的表
        for table_name, keywords in table_keyword_map.items():
            for keyword in keywords:
                if keyword in query_str:
                    matched_tables = [table for table in self.tables_data if table['name'] == table_name]
                    if matched_tables:
                        self.logger.debug(f'通过关键词"{keyword}"直接匹配到表: {table_name}')
                        return matched_tables
        
        # 步骤2: 基于表注释进行匹配
        for table in self.tables_data:
            comment = table.get('comment', '')
            # 检查查询中的关键词是否出现在表注释中
            for word in query_str.split():
                if len(word) >= 2 and word in comment:  # 只考虑长度>=2的词
                    self.logger.debug(f'通过表注释匹配到表: {table["name"]}，关键词: {word}')
                    return [table]
        
        # 步骤3: 对所有表进行评分
        query_keywords = [w for w in query_str.lower().split() if len(w) >= 2]
        scored_tables = []
        
        for table in self.tables_data:
            score = 0
            table_name = table['name'].lower()
            comment = table.get('comment', '').lower()
            
            # 表名匹配得分
            for keyword in query_keywords:
                if keyword in table_name:
                    score += 3
            
            # 表注释匹配得分
            for keyword in query_keywords:
                if keyword in comment:
                    score += 5
            
            # 字段名和字段注释匹配得分
            for column in table.get('columns', []):
                col_name = column['name'].lower()
                col_comment = column.get('comment', '').lower()
                
                for keyword in query_keywords:
                    if keyword in col_name:
                        score += 1
                    if keyword in col_comment:
                        score += 2
            
            if score > 0:
                scored_tables.append((table, score))
        
        # 按得分排序，选择最相关的表(最多5个)
        scored_tables.sort(key=lambda x: x[1], reverse=True)
        selected_tables = [table for table, _ in scored_tables[:5]]
        
        if selected_tables:
            self.logger.debug(f'选择了{len(selected_tables)}个相关表: {[t["name"] for t in selected_tables]}')
            return selected_tables
        
        # 如果没有找到任何匹配，返回前3个表作为备选
        self.logger.debug('未找到相关表，返回前3个表作为备选')
        return self.tables_data[:3]
    
    def _build_compact_schema(self, tables):
        """从选定的表构建紧凑的schema字符串"""
        schema_info = []
        for table in tables:
            # 添加表的基本信息
            table_info = f"表名：{table['name']}\n表说明：{table.get('comment', '')}\n\n字段列表："
            schema_info.append(table_info)
            
            # 添加列信息（精简版）
            for column in table.get('columns', []):
                col_type = f"{column['type']}"
                if column.get('length'):
                    col_type += f"({column['length']})"
                
                col_info = f"- {column['name']}: {column.get('comment', '无说明')} ({col_type})"
                schema_info.append(col_info)
            
            schema_info.append("\n")
        
        final_schema = "\n".join(schema_info)
        return final_schema

    # @lru_cache(maxsize=100)  # 移除缓存装饰器，确保每次都通过LLM处理
    def _cached_query(self, query_str):
        """执行查询，确保通过LLM生成SQL"""
        self.logger.debug(f'执行查询: {query_str}')
        
        # 记录开始时间
        start_time = time.time()
        
        # 直接使用备选方法生成和执行SQL
        llm_start_time = time.time()
        sql, result = self._direct_llm_query(query_str)
        llm_time = time.time() - llm_start_time
        self.logger.info(f'[性能] LLM生成SQL耗时: {llm_time:.2f}秒')
        
        # 构造与query_engine兼容的返回格式
        from types import SimpleNamespace
        response = SimpleNamespace()
        response.result = result
        response.metadata = {'sql_query': sql}
        
        # 记录总时间
        total_time = time.time() - start_time
        self.logger.info(f'[性能] 查询总耗时: {total_time:.2f}秒')
        
        return response
            
    def query(self, query_str):
        try:
            self.logger.info(f'开始处理查询: {query_str}')
            query_start_time = time.time()
            
            # 初始化性能统计
            perf_stats = {
                'query_text': query_str,
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # 记录将要发送到LLM的信息
            self.logger.debug(f'发送到LLM的查询: {query_str}')
            
            # 调用LLM获取SQL (通过_cached_query)
            llm_start_time = time.time()
            response = self._cached_query(query_str)
            llm_time = time.time() - llm_start_time
            perf_stats['llm_time'] = llm_time
            
            # 从返回结果中提取SQL
            if hasattr(response, 'metadata') and 'sql_query' in response.metadata:
                sql = response.metadata['sql_query']
                self.logger.debug(f'LLM生成的SQL: {sql}')
                perf_stats['sql'] = sql
                
                # 验证SQL
                validate_start_time = time.time()
                self._validate_sql(sql)
                
                # 尝试验证字段存在性
                try:
                    self._validate_columns(sql, self.tables_data)
                except Exception as e:
                    self.logger.warning(f'字段验证警告: {str(e)}')
                validate_time = time.time() - validate_start_time
                self.logger.info(f'[性能] SQL验证耗时: {validate_time:.2f}秒')
                perf_stats['validate_time'] = validate_time
                
                # 记录成功日志
                self.logger.info(
                    json.dumps({
                        "type": "query",
                        "status": "success",
                        "query": query_str.replace("密码", "***"),
                        "sql": sql.replace(str(self.db_password), "***") if self.db_password else sql, 
                        "result_size": len(str(response.result))
                    }, ensure_ascii=False)
                )
                
                # 记录总耗时
                total_time = time.time() - query_start_time
                self.logger.info(f'[性能] 查询处理总耗时: {total_time:.2f}秒')
                perf_stats['total_time'] = total_time
                perf_stats['status'] = 'success'
                
                # 控制台简洁输出
                self.logger.info("===== 查询性能明细 =====")
                self.logger.info(f"LLM生成SQL: {llm_time:.2f}秒")
                self.logger.info(f"SQL验证: {validate_time:.2f}秒")
                self.logger.info(f"总耗时: {total_time:.2f}秒")
                self.logger.info("=======================")
                
                # 保存性能统计信息
                self.performance_stats['queries'].append(perf_stats)
                
                return {
                    'sql': sql,
                    'result': response.result,
                    'status': 'success',
                    'performance': {
                        'llm_time': llm_time,
                        'validate_time': validate_time,
                        'total_time': total_time
                    }
                }
            else:
                self.logger.error(f'LLM未返回SQL查询，返回数据: {str(response)}')
                error_msg = "LLM未能生成有效的SQL查询"
                perf_stats['status'] = 'error'
                perf_stats['error'] = error_msg
                self.performance_stats['queries'].append(perf_stats)
                raise ValueError(error_msg)
            
        except Exception as e:
            # 记录错误日志
            self.logger.error(
                json.dumps({
                    "type": "query",
                    "status": "error",
                    "query": query_str,
                    "error": str(e),
                    "sql": locals().get('sql', '')
                }, ensure_ascii=False)
            )
            
            # 记录总耗时
            total_time = time.time() - query_start_time
            self.logger.info(f'[性能] 查询失败，总耗时: {total_time:.2f}秒')
            
            # 保存性能统计信息
            if 'perf_stats' not in locals():
                perf_stats = {
                    'query_text': query_str,
                    'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                    'status': 'error',
                    'error': str(e),
                    'total_time': total_time
                }
                self.performance_stats['queries'].append(perf_stats)
            
            return {
                'error': str(e),
                'status': 'error',
                'performance': {
                    'total_time': total_time
                }
            }

    def print_performance_report(self):
        """打印性能统计报告到控制台"""
        print("\n" + "="*50)
        print("性能统计报告")
        print("="*50)
        
        # 初始化性能统计
        init_stats = self.performance_stats.get('initialization', {})
        if init_stats:
            print("\n【初始化性能】")
            print(f"总初始化时间: {init_stats.get('total_time', 0):.2f}秒")
            print(f"数据来源: {'缓存' if init_stats.get('from_cache') else '完整构建'}")
            print(f"创建数据库连接: {init_stats.get('create_db', 0):.2f}秒")
            if init_stats.get('from_cache'):
                print(f"检查和加载缓存: {init_stats.get('check_cache', 0) + init_stats.get('load_cache', 0):.2f}秒")
                print(f"恢复缓存数据: {init_stats.get('restore_data', 0):.2f}秒")
            else:
                print(f"加载表结构: {init_stats.get('load_schema', 0):.2f}秒")
                print(f"保存缓存: {init_stats.get('save_cache', 0):.2f}秒")
            
            engine_details = init_stats.get('engine_details', {})
            print(f"创建查询引擎: {init_stats.get('create_engine', 0):.2f}秒")
            if 'retriever_details' in engine_details:
                retriever = engine_details.get('retriever_details', {})
                if retriever.get('used_cache'):
                    print(f" └─ 使用了缓存向量: {retriever.get('vector_count', 0)}个")
                else:
                    print(f" └─ 重新计算向量: {retriever.get('vector_count', 0)}个")
                    print(f"    └─ 嵌入模型加载和计算: {retriever.get('embed_model_time', 0):.2f}秒")
        
        # 查询性能统计
        queries = self.performance_stats.get('queries', [])
        if queries:
            print("\n【查询性能】")
            print(f"执行查询数量: {len(queries)}")
            
            # 平均统计
            total_times = [q.get('total_time', 0) for q in queries if 'total_time' in q]
            llm_times = [q.get('llm_time', 0) for q in queries if 'llm_time' in q]
            validate_times = [q.get('validate_time', 0) for q in queries if 'validate_time' in q]
            
            if total_times:
                print(f"平均查询时间: {sum(total_times)/len(total_times):.2f}秒")
            if llm_times:
                print(f"平均LLM响应时间: {sum(llm_times)/len(llm_times):.2f}秒")
            if validate_times:
                print(f"平均SQL验证时间: {sum(validate_times)/len(validate_times):.2f}秒")
            
            # 最后5次查询详情
            print("\n最近查询详情:")
            for i, q in enumerate(queries[-5:]):
                print(f"\n  查询 #{len(queries)-len(queries[-5:])+i+1} ({q.get('timestamp', '')})")
                print(f"  文本: {q.get('query_text', '')[:50]}{'...' if len(q.get('query_text', '')) > 50 else ''}")
                print(f"  状态: {q.get('status', '未知')}")
                if q.get('status') == 'success':
                    print(f"  SQL: {q.get('sql', '')[:50]}{'...' if len(q.get('sql', '')) > 50 else ''}")
                    print(f"  总耗时: {q.get('total_time', 0):.2f}秒")
                    print(f"  └─ LLM时间: {q.get('llm_time', 0):.2f}秒")
                    print(f"  └─ 验证时间: {q.get('validate_time', 0):.2f}秒")
                else:
                    print(f"  错误: {q.get('error', '未知错误')}")
                    print(f"  耗时: {q.get('total_time', 0):.2f}秒")
        
        print("\n" + "="*50)

# 添加一个函数用于获取连接池状态
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