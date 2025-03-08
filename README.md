# SQL查询助手

一个基于自然语言处理的SQL查询工具，允许用户使用日常语言查询数据库，无需编写复杂的SQL语句。

## 功能特性

- **自然语言查询**：使用中文自然语言描述需求，系统自动转换为SQL查询
- **多种运行模式**：支持真实数据库连接和测试模式（SQLite内存数据库）
- **向量缓存**：通过缓存表结构的向量表示来加快初始化速度
- **交互式界面**：支持命令行交互模式和单次查询模式
- **性能监控**：提供详细的性能统计信息，监控查询执行时间
- **连接池管理**：优化数据库连接性能，支持连接状态查看

## 安装指南

### 前提条件

- Python 3.8+
- 用于连接到数据库的ODBC驱动程序（仅在真实数据库模式下需要）
- DeepSeek API密钥（用于自然语言处理）

### 安装步骤

1. **克隆项目**

```bash
git clone https://github.com/yourusername/sql-query-assistant.git
cd sql-query-assistant
```

2. **安装依赖**

```bash
pip install -r requirements.txt
```

3. **环境配置**

创建`.env`文件或设置环境变量：

```
DEEPSEEK_API_KEY=your_api_key_here
```

4. **数据库配置**

运行配置向导：

```bash
python setup_db.py
```

按照提示输入数据库连接信息。

## 使用方法

### 基本用法

直接输入查询：

```bash
python main.py 查询所有产品信息
```

使用查询参数：

```bash
python main.py --query "查询价格超过1000元的电子产品"
```

### 交互模式

```bash
python main.py --interactive
```

在提示符下输入查询，输入`exit`退出。

### 测试模式

无需真实数据库连接，使用内存中的SQLite数据库：

```bash
python main.py --test-mode
```

### 其他选项

- `--no-cache`: 禁用缓存
- `--rebuild-cache`: 重建缓存
- `--list-tables`: 列出可用表
- `--show-pool-status`: 显示数据库连接池状态
- `--api-key`: 直接提供API密钥

## 配置项目

### 检查当前配置

使用配置检查工具查看和管理当前配置：

```bash
python check_config.py
```

### 数据库配置文件

支持JSON和YAML格式：
- `config/db_config.json` 或 `config/db_config.yaml`

配置示例：
```yaml
server: your_server
database: mes
trusted_connection: true
driver: "ODBC Driver 17 for SQL Server"
```

### 测试模式配置

创建`config/test_mode.json`文件启用测试模式：
```json
{
  "mode": "test"
}
```

## 故障排除

### 常见问题

1. **数据库连接错误**
   - 检查数据库配置文件
   - 确认ODBC驱动程序已安装
   - 使用`setup_db.py`测试连接

2. **API密钥问题**
   - 确保在`.env`文件中或环境变量中设置了`DEEPSEEK_API_KEY`
   - 或使用`--api-key`参数直接提供

3. **程序启动时进入测试模式**
   - 运行`python check_config.py`检查并删除`test_mode.json`
   - 使用`--no-test-mode`参数强制使用真实数据库

4. **性能问题**
   - 使用`--show-pool-status`检查连接池状态
   - 观察性能报告，识别瓶颈

## 项目结构

```
sql-query-assistant/
├── config/               # 配置文件目录
├── data/                 # 缓存和表结构数据
├── src/                  # 源代码
│   ├── database.py       # 数据库连接管理
│   ├── query_bot.py      # 查询处理核心类
│   ├── logger.py         # 日志管理
│   ├── config.py         # 配置管理
│   ├── utils.py          # 工具函数
│   └── test_utils.py     # 测试工具
├── main.py               # 主程序入口
├── setup_db.py           # 数据库配置工具
├── check_config.py       # 配置检查工具
└── README.md             # 本文档
```

## 示例查询

```
查询价格超过1000元的电子产品
统计每个类别的产品平均价格
查询张三的所有订单及订单明细
查询库存少于20的产品
统计每个客户的订单总金额并按金额降序排列
```
