"""
SQL处理工具模块，负责SQL验证和处理
"""
import re
import logging
from src.config import SQL_FORBIDDEN_KEYWORDS
from src.logger import get_logger

logger = get_logger('sql')

def validate_sql(sql):
    """验证SQL语句，防止危险操作"""
    # 预处理SQL，移除可能的markdown代码块标记
    cleaned_sql = sql
    
    # 移除代码块标记
    if '```' in cleaned_sql:
        # 移除所有```sql标记
        cleaned_sql = re.sub(r'```\w*\n', '', cleaned_sql)  # 移除开头的```sql\n
        cleaned_sql = re.sub(r'```\s*$', '', cleaned_sql)  # 移除结尾的```
    
    # 替换多个换行符为单个空格，便于检测开头的SELECT
    compact_sql = ' '.join([line.strip() for line in cleaned_sql.split('\n') if line.strip()])
    
    # 验证SQL是否以SELECT开头
    if not compact_sql.upper().startswith('SELECT'):
        logger.error('SQL验证失败：非SELECT语句')
        raise ValueError("仅允许SELECT查询，请检查语句是否以SELECT开头")
    
    # 单独检查禁用关键字
    detected_keywords = []
    for kw in SQL_FORBIDDEN_KEYWORDS:
        # 使用单词边界匹配，避免误报
        pattern = r'\b' + kw + r'\b'
        if re.search(pattern, cleaned_sql.upper()):
            detected_keywords.append(kw)
            logger.error(f'SQL中检测到禁用关键字: {kw}')
    
    if detected_keywords:
        logger.error(f'SQL验证失败：包含禁用关键字: {", ".join(detected_keywords)}')
        raise ValueError(f"SQL包含禁用关键字: {', '.join(detected_keywords)}")
    
    # 返回清理后的SQL
    logger.debug('SQL安全性验证通过')
    return cleaned_sql

def validate_columns(sql, tables_data):
    """验证SQL语句中的字段是否实际存在"""
    try:
        from sqlparse import parse
        from sqlparse.sql import IdentifierList, Identifier
        
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
            logger.warning('SQL解析失败，跳过字段验证')
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
                except Exception:
                    pass
            
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
                        except Exception:
                            pass
        
        # 对每个提取的字段名进行验证
        for column_name in potential_columns:
            # 检查字段是否存在于任何表中
            if not any(column['name'] == column_name for table in tables_data for column in table.get('columns', [])):
                logger.warning(f'未找到匹配字段: {column_name}，但将继续执行')
        
    except Exception as e:
        # 字段验证失败不应该阻止查询执行
        logger.warning(f'字段验证过程出错: {str(e)}')
        logger.warning('继续执行查询，但可能存在字段不匹配风险')

def build_compact_schema(tables):
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