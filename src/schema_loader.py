"""
表结构加载模块，负责从不同来源加载表结构
"""
import json
import logging
from src.logger import get_logger

logger = get_logger('schema')

def load_schema_from_json(file_path):
    """从JSON文件加载表结构信息"""
    logger.info(f"从{file_path}加载表结构")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            tables = json.load(f)
        
        # 构建schema字符串
        schema_lines = []
        
        for table in tables:
            table_name = table.get('name', '')
            table_comment = table.get('comment', '')
            
            # 添加表信息
            schema_lines.append(f"表名：{table_name}")
            if table_comment:
                schema_lines.append(f"表说明：{table_comment}")
            schema_lines.append("")
            
            # 添加字段信息
            schema_lines.append("字段列表：")
            for column in table.get('columns', []):
                col_name = column.get('name', '')
                col_type = column.get('type', '')
                col_length = column.get('length', '')
                col_comment = column.get('comment', '')
                
                # 构建字段类型
                type_str = col_type
                if col_length:
                    type_str += f"({col_length})"
                
                # 添加字段行
                field_line = f"- {col_name}: {col_comment} ({type_str})"
                schema_lines.append(field_line)
            
            # 表之间添加空行
            schema_lines.append("")
            schema_lines.append("")
            
        logger.info(f"成功加载{len(tables)}个表的结构")
        return "\n".join(schema_lines)
        
    except Exception as e:
        logger.error(f"加载表结构失败: {str(e)}")
        raise ValueError(f"加载表结构失败: {str(e)}")

def load_schema_from_database(engine):
    """从数据库直接加载表结构信息"""
    from sqlalchemy import inspect
    
    logger.info("从数据库加载表结构")
    
    try:
        inspector = inspect(engine)
        schema_lines = []
        
        # 获取所有表名
        table_names = inspector.get_table_names()
        
        for table_name in table_names:
            # 添加表信息
            schema_lines.append(f"表名：{table_name}")
            schema_lines.append("")
            
            # 获取表的列信息
            columns = inspector.get_columns(table_name)
            
            # 添加字段信息
            schema_lines.append("字段列表：")
            for column in columns:
                col_name = column.get('name', '')
                col_type = str(column.get('type', ''))
                
                # 添加字段行
                field_line = f"- {col_name}: ({col_type})"
                schema_lines.append(field_line)
            
            # 表之间添加空行
            schema_lines.append("")
            schema_lines.append("")
            
        logger.info(f"成功从数据库加载{len(table_names)}个表的结构")
        return "\n".join(schema_lines)
        
    except Exception as e:
        logger.error(f"从数据库加载表结构失败: {str(e)}")
        raise ValueError(f"从数据库加载表结构失败: {str(e)}")