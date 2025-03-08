import json
from llama_index.core import SQLDatabase
from sqlalchemy import MetaData
import logging

# 配置日志记录器
logger = logging.getLogger('SchemaLoader')
logger.setLevel(logging.INFO)  # 将默认级别从DEBUG改为INFO，减少调试输出
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# 创建控制台处理器
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# 添加文件处理器，将详细日志写入文件而不是控制台
file_handler = logging.FileHandler('schema_loader.log')
file_handler.setLevel(logging.DEBUG)  # 文件日志仍保持DEBUG级别
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

def load_schema_from_json(json_path):
    logger.info(f'开始从{json_path}加载表结构')
    with open(json_path, 'r', encoding='utf-8') as f:
        tables_data = json.load(f)
    
    logger.info(f'成功加载{len(tables_data)}个表的定义')
    
    schema_info = []
    for table in tables_data:
        # 添加表的基本信息
        table_info = f"表名：{table['name']}\n表说明：{table.get('comment', '')}\n\n字段列表："
        schema_info.append(table_info)
        logger.debug(f'处理表: {table["name"]}, 字段数: {len(table.get("columns", []))}')
        
        # 添加列信息
        for column in table.get('columns', []):
            col_type = f"{column['type']}"
            if column.get('length'):
                col_type += f"({column['length']})"
            
            col_info = f"- 字段名（{column['name']}）: {column.get('comment', '无说明')} \n  英文名称: {column['name']} \n  类型: {col_type} \n  允许空值: {'是' if column.get('nullable', True) else '否'}"
            schema_info.append(col_info)
            
            # 将字段详细信息的日志级别改为DEBUG
            # 这样在控制台(INFO级别)不会显示，但在日志文件中会保留
            logger.debug(f'添加字段信息: {column["name"]} ({col_type})')
        
        schema_info.append("\n")
    
    final_schema = "\n".join(schema_info)
    logger.info(f'表结构加载完成，总长度: {len(final_schema)}')
    return final_schema

def load_schema(engine):
    logger.info('开始从数据库加载表结构')
    metadata = MetaData()
    metadata.reflect(bind=engine)
    
    schema_info = []
    for table in metadata.tables.values():
        cols = [f"{col.name} ({col.type})" for col in table.columns]
        schema_info.append(f"Table {table.name}: {', '.join(cols)}")
        logger.debug(f'加载表: {table.name}, 字段数: {len(cols)}')
    
    final_schema = "\n".join(schema_info)
    logger.info(f'数据库表结构加载完成，共{len(metadata.tables)}张表')
    return final_schema