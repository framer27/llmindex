"""
LLM处理模块，负责与LLM的交互和管理
"""
import time
import logging
from llama_index.llms.deepseek import DeepSeek
from llama_index.core.prompts import PromptTemplate
from src.logger import get_logger
from src.config import SQL_PROMPT_TEMPLATE, TABLE_KEYWORD_MAP
from src.sql_utils import build_compact_schema, validate_sql
from src.utils import format_result

logger = get_logger('llm')

class LLMHandler:
    """LLM处理类，处理与语言模型的交互"""
    
    def __init__(self, api_key):
        """初始化LLM处理器"""
        self.llm = DeepSeek(
            model="deepseek-chat",
            api_key=api_key
        )
        self.prompt_template = PromptTemplate(SQL_PROMPT_TEMPLATE)
        
    def select_relevant_tables(self, query_str, tables_data):
        """选择与查询相关的表"""
        logger.debug(f'为查询选择相关表')
        
        # 步骤1: 基于关键词映射直接查找匹配的表
        for table_name, keywords in TABLE_KEYWORD_MAP.items():
            for keyword in keywords:
                if keyword in query_str:
                    matched_tables = [table for table in tables_data if table['name'] == table_name]
                    if matched_tables:
                        logger.debug(f'通过关键词"{keyword}"直接匹配到表: {table_name}')
                        return matched_tables
        
        # 步骤2: 基于表注释进行匹配
        for table in tables_data:
            comment = table.get('comment', '')
            # 检查查询中的关键词是否出现在表注释中
            for word in query_str.split():
                if len(word) >= 2 and word in comment:  # 只考虑长度>=2的词
                    logger.debug(f'通过表注释匹配到表')
                    return [table]
        
        # 步骤3: 对所有表进行评分
        query_keywords = [w for w in query_str.lower().split() if len(w) >= 2]
        scored_tables = []
        
        for table in tables_data:
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
            logger.debug(f'选择了{len(selected_tables)}个相关表')
            return selected_tables
        
        # 如果没有找到任何匹配，返回前3个表作为备选
        logger.debug('未找到相关表，返回前3个表作为备选')
        return tables_data[:3]
    
    def generate_sql(self, query_str, tables_data, sql_database):
        """生成SQL并执行查询"""
        logger.info('生成SQL查询')
        start_time = time.time()
        
        # 为当前查询选择最相关的表
        relevant_tables = self.select_relevant_tables(query_str, tables_data)
        
        # 构建精简的schema字符串
        compact_schema = build_compact_schema(relevant_tables)
        
        # 构建包含schema的提示
        prompt = self.prompt_template.format(
            schema=compact_schema,
            query=query_str
        )
        
        # 调用LLM生成SQL
        logger.info('调用LLM中...')
        llm_start = time.time()
        response = self.llm.complete(prompt)
        raw_sql = response.text.strip()
        llm_time = time.time() - llm_start
        logger.info(f'LLM响应时间: {llm_time:.2f}秒')
        
        # 验证SQL
        cleaned_sql = validate_sql(raw_sql)
        
        # 执行SQL查询
        try:
            # 执行SQL查询
            result = sql_database.run_sql(cleaned_sql)
            
            # 格式化结果
            result_str = format_result(result)
            
            total_time = time.time() - start_time
            logger.info(f'SQL生成和执行完成，总耗时: {total_time:.2f}秒')
            
            return cleaned_sql, result_str
        except Exception as e:
            logger.error(f'SQL执行失败: {str(e)}')
            raise ValueError(f"SQL执行失败: {str(e)}") 