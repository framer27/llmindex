"""
查询机器人核心模块，整合所有功能
"""
import json
import time
import logging
from types import SimpleNamespace

from llama_index.core import SQLDatabase, Settings
from llama_index.core.objects import SQLTableSchema
from llama_index.core.retrievers import SQLRetriever
from llama_index.core.schema import TextNode
from llama_index.core.query_engine import SQLTableRetrieverQueryEngine

from src.logger import get_logger
from src.config import EMBED_MODEL, CACHE_VERSION
from src.cache_manager import check_cache, load_cache, save_cache
from src.llm_handler import LLMHandler
from src.sql_utils import validate_sql, validate_columns

# 设置全局嵌入模型
Settings.embed_model = EMBED_MODEL

class QueryBot:
    def __init__(self, api_key):
        """初始化查询机器人"""
        # 获取日志记录器
        self.logger = get_logger('QueryBot')
        
        # 加载数据库配置
        from src.database import load_db_config
        self.db_config = load_db_config()
        self.db_password = str(self.db_config['password']) if 'password' in self.db_config else ""
        
        # 初始化LLM处理器
        self.llm_handler = LLMHandler(api_key)
        
        # 性能统计收集器
        self.performance_stats = {
            'initialization': {},
            'queries': []
        }
        
        # 初始化变量
        self.sql_database = None
        self.query_engine = None
        self.tables_data = None
        self.schema = None
        
    def build_engine(self, engine=None, use_cache=True, force_rebuild=False, tables_data_path='data/tables.json'):
        """构建查询引擎"""
        self.logger.info('开始构建查询引擎')
        init_start_time = time.time()
        
        # 初始化性能统计
        perf_stats = {}
        
        # 使用提供的数据库引擎或获取连接池引擎
        if engine is None:
            from src.database import get_db_engine
            engine = get_db_engine()
        
        self.sql_database = SQLDatabase(engine=engine)
        
        # 检查缓存
        cache_path = None
        if use_cache and not force_rebuild:
            cache_path = check_cache(tables_data_path)
        
        # 如果存在缓存且未强制重建，则尝试加载缓存
        cached_embeddings = None
        if cache_path and use_cache and not force_rebuild:
            # 加载缓存
            cache_data = load_cache(cache_path)
            
            if cache_data:
                # 从缓存恢复数据
                self.tables_data = cache_data['tables_data']
                self.schema = cache_data['schema']
                
                # 提取缓存的嵌入向量
                if 'table_embeddings' in cache_data:
                    cached_embeddings = cache_data['table_embeddings']
                    self.logger.info(f"从缓存加载了{len(cached_embeddings)}个表的嵌入向量")
                
                # 创建查询引擎
                self._create_query_engine(engine, cached_embeddings)
                
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
        self._create_query_engine(engine)
        
        # 创建缓存
        if use_cache:
            cache_data = {
                'tables_data': self.tables_data,
                'schema': self.schema,
                'cached_time': time.time(),
                'cache_version': CACHE_VERSION
            }
            
            # 如果存在嵌入向量，添加到缓存
            if hasattr(self, '_last_table_embeddings') and self._last_table_embeddings:
                cache_data['table_embeddings'] = self._last_table_embeddings
            
            save_cache(cache_data, tables_data_path)
        
        # 计算总初始化时间
        init_time = time.time() - init_start_time
        self.logger.info(f'查询引擎构建完成，总耗时: {init_time:.2f}秒')
        perf_stats['total_time'] = init_time
        perf_stats['from_cache'] = False
        
        # 保存性能统计
        self.performance_stats['initialization'] = perf_stats
        
        return self
        
    def _create_query_engine(self, engine, cached_embeddings=None):
        """创建查询引擎，可复用缓存的嵌入向量"""
        # 创建SQLTableSchema对象
        table_schemas = [SQLTableSchema(table_name=table['name']) for table in self.tables_data]
        
        # 创建表检索器
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
        else:
            # 从头创建嵌入向量
            self.logger.info("重新计算表嵌入向量")
            
            # 创建标准检索器
            self.logger.info("加载嵌入模型...")
            table_retriever = SQLRetriever(
                sql_database=self.sql_database,
                table_schemas=table_schemas,
                embed_model='local'
            )
            
            # 提取并保存嵌入向量，以便后续保存到缓存
            self._last_table_embeddings = self._extract_retriever_embeddings(table_retriever)
            
        # 尝试创建查询引擎
        try:
            self.logger.info("初始化SQLTableRetrieverQueryEngine...")
            self.query_engine = SQLTableRetrieverQueryEngine(
                sql_database=self.sql_database,
                table_retriever=table_retriever,
                text_to_sql_prompt=self.llm_handler.prompt_template,
                llm=self.llm_handler.llm
            )
        except Exception as e:
            self.logger.error(f'创建SQLTableRetrieverQueryEngine失败: {str(e)}')
            self.logger.info('将使用直接LLM方法作为备选')
            self.query_engine = None
    
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
        from llama_index.core.schema import TextNode
        
        self.logger.info("创建表检索器（使用缓存向量）...")
        
        # 创建基础检索器
        retriever = SQLRetriever(
            sql_database=sql_database,
            table_schemas=table_schemas,
            embed_model='local'
        )
        
        # 尝试将缓存的嵌入向量注入到检索器中
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
            
        except Exception as e:
            self.logger.warning(f"注入缓存嵌入向量失败: {str(e)}")
            # 如果失败，返回标准检索器
            self.logger.info("回退到标准检索器创建...")
            retriever = SQLRetriever(
                sql_database=sql_database,
                table_schemas=table_schemas,
                embed_model='local'
            )
            
        return retriever
            
    def query(self, query_str):
        """处理用户查询"""
        try:
            self.logger.info(f'处理查询: {query_str}')
            query_start_time = time.time()
            
            # 初始化性能统计
            perf_stats = {
                'query_text': query_str,
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # 生成SQL
            try:
                llm_start_time = time.time()
                sql, result = self.llm_handler.generate_sql(
                    query_str, 
                    self.tables_data, 
                    self.sql_database
                )
                llm_time = time.time() - llm_start_time
                perf_stats['llm_time'] = llm_time
            except Exception as e:
                self.logger.error(f'SQL生成失败: {str(e)}')
                raise ValueError(f"SQL生成失败: {str(e)}")
            
            # 记录SQL
            perf_stats['sql'] = sql
            
            # 尝试验证字段存在性
            try:
                validate_columns(sql, self.tables_data)
            except Exception as e:
                self.logger.warning(f'字段验证警告: {str(e)}')
            
            # 记录总耗时
            total_time = time.time() - query_start_time
            self.logger.info(f'查询处理完成，耗时: {total_time:.2f}秒')
            perf_stats['total_time'] = total_time
            perf_stats['status'] = 'success'
            
            # 保存性能统计信息
            self.performance_stats['queries'].append(perf_stats)
            
            return {
                'sql': sql,
                'result': result,
                'status': 'success',
                'performance': {
                    'llm_time': llm_time,
                    'total_time': total_time
                }
            }
            
        except Exception as e:
            # 记录错误日志
            self.logger.error(f'查询失败: {str(e)}')
            
            # 记录总耗时
            total_time = time.time() - query_start_time
            
            # 保存性能统计信息
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
        
        # 查询性能统计
        queries = self.performance_stats.get('queries', [])
        if queries:
            print("\n【查询性能】")
            print(f"执行查询数量: {len(queries)}")
            
            # 平均统计
            total_times = [q.get('total_time', 0) for q in queries if 'total_time' in q]
            llm_times = [q.get('llm_time', 0) for q in queries if 'llm_time' in q]
            
            if total_times:
                print(f"平均查询时间: {sum(total_times)/len(total_times):.2f}秒")
            if llm_times:
                print(f"平均LLM响应时间: {sum(llm_times)/len(llm_times):.2f}秒")
        
        print("\n" + "="*50) 