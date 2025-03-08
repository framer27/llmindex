"""
测试工具模块，提供测试相关功能
"""
import logging
import json
import os
from src.logger import get_logger
from src.config import ROOT_DIR
from sqlalchemy import text

logger = get_logger('test')

def create_test_db(engine):
    """
    在测试模式下创建示例数据库和表
    
    Args:
        engine: SQLAlchemy引擎
    """
    logger.info("正在创建测试数据库和表...")
    
    try:
        # 创建一些示例表
        conn = engine.connect()
        conn.execute(text("CREATE TABLE IF NOT EXISTS Products (ProductID INTEGER PRIMARY KEY, ProductName TEXT NOT NULL, Category TEXT, Price REAL, Stock INTEGER)"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS Orders (OrderID INTEGER PRIMARY KEY, CustomerName TEXT NOT NULL, OrderDate TEXT, TotalAmount REAL)"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS OrderDetails (OrderDetailID INTEGER PRIMARY KEY, OrderID INTEGER, ProductID INTEGER, Quantity INTEGER, UnitPrice REAL)"))
        
        # 清空表中的数据
        conn.execute(text("DELETE FROM OrderDetails"))
        conn.execute(text("DELETE FROM Orders"))
        conn.execute(text("DELETE FROM Products"))
        
        # 添加产品数据
        conn.execute(text("INSERT INTO Products VALUES (1, '笔记本电脑', '电子产品', 5999.00, 50)"))
        conn.execute(text("INSERT INTO Products VALUES (2, '智能手机', '电子产品', 3999.00, 100)"))
        conn.execute(text("INSERT INTO Products VALUES (3, '办公桌', '家具', 899.00, 20)"))
        conn.execute(text("INSERT INTO Products VALUES (4, '办公椅', '家具', 499.00, 30)"))
        conn.execute(text("INSERT INTO Products VALUES (5, '打印机', '办公设备', 1299.00, 15)"))
        
        # 添加订单数据
        conn.execute(text("INSERT INTO Orders VALUES (1, '张三', '2024-01-15', 9998.00)"))
        conn.execute(text("INSERT INTO Orders VALUES (2, '李四', '2024-01-20', 4498.00)"))
        conn.execute(text("INSERT INTO Orders VALUES (3, '王五', '2024-02-05', 7997.00)"))
        
        # 添加订单明细数据
        conn.execute(text("INSERT INTO OrderDetails VALUES (1, 1, 1, 1, 5999.00)"))
        conn.execute(text("INSERT INTO OrderDetails VALUES (2, 1, 2, 1, 3999.00)"))
        conn.execute(text("INSERT INTO OrderDetails VALUES (3, 2, 3, 1, 899.00)"))
        conn.execute(text("INSERT INTO OrderDetails VALUES (4, 2, 4, 1, 499.00)"))
        conn.execute(text("INSERT INTO OrderDetails VALUES (5, 2, 2, 1, 3999.00)"))
        conn.execute(text("INSERT INTO OrderDetails VALUES (6, 3, 2, 2, 3999.00)"))
        
        # 提交更改
        conn.commit()
        conn.close()
            
        # 生成示例表结构文件，供查询引擎使用
        generate_test_schema(engine)
        
        logger.info("测试数据库和表创建完成")
    except Exception as e:
        logger.error(f"创建测试数据库失败: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

def generate_test_schema(engine):
    """从测试数据库生成表结构JSON文件"""
    logger.info("生成测试表结构文件...")
    
    # 检查是否存在data目录
    data_dir = os.path.join(ROOT_DIR, 'data')
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    
    # 创建表结构数据
    tables = []
    
    # 产品表
    tables.append({
        "name": "Products",
        "comment": "产品信息表，存储所有销售产品的详细信息",
        "columns": [
            {"name": "ProductID", "type": "INTEGER", "comment": "产品ID，主键"},
            {"name": "ProductName", "type": "TEXT", "comment": "产品名称"},
            {"name": "Category", "type": "TEXT", "comment": "产品类别"},
            {"name": "Price", "type": "REAL", "comment": "产品单价"},
            {"name": "Stock", "type": "INTEGER", "comment": "库存数量"}
        ]
    })
    
    # 订单表
    tables.append({
        "name": "Orders",
        "comment": "订单信息表，存储客户订单的基本信息",
        "columns": [
            {"name": "OrderID", "type": "INTEGER", "comment": "订单ID，主键"},
            {"name": "CustomerName", "type": "TEXT", "comment": "客户姓名"},
            {"name": "OrderDate", "type": "TEXT", "comment": "订单日期"},
            {"name": "TotalAmount", "type": "REAL", "comment": "订单总金额"}
        ]
    })
    
    # 订单明细表
    tables.append({
        "name": "OrderDetails",
        "comment": "订单明细表，存储订单中包含的产品详情",
        "columns": [
            {"name": "OrderDetailID", "type": "INTEGER", "comment": "订单明细ID，主键"},
            {"name": "OrderID", "type": "INTEGER", "comment": "订单ID，外键关联Orders表"},
            {"name": "ProductID", "type": "INTEGER", "comment": "产品ID，外键关联Products表"},
            {"name": "Quantity", "type": "INTEGER", "comment": "购买数量"},
            {"name": "UnitPrice", "type": "REAL", "comment": "单价"}
        ]
    })
    
    # 保存表结构到JSON文件
    schema_path = os.path.join(data_dir, 'tables.json')
    with open(schema_path, 'w', encoding='utf-8') as f:
        json.dump(tables, f, ensure_ascii=False, indent=2)
    
    logger.info(f"测试表结构文件已保存至: {schema_path}") 