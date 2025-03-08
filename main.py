import os
import time
import argparse
import logging
from datetime import datetime
from src.database import get_db_engine, test_connection, get_pool_status
from src.query_engine import QueryBot, get_db_engine as get_query_engine

# 尝试加载.env文件中的环境变量
try:
    from dotenv import load_dotenv
    # 尝试加载项目根目录下的.env文件
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
    if os.path.exists(env_path):
        load_dotenv(env_path)
        print(f"已从 {env_path} 加载环境变量")
    else:
        # 尝试加载当前目录的.env文件
        load_dotenv()
        print("已尝试加载.env文件")
except ImportError:
    print("提示: 安装python-dotenv库可以自动加载.env文件中的环境变量")
    print("安装命令: pip install python-dotenv")

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('main')

def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='SQL查询助手')
    
    # 缓存相关参数
    parser.add_argument('--no-cache', action='store_true', help='禁用缓存')
    parser.add_argument('--rebuild-cache', action='store_true', help='强制重建缓存')
    parser.add_argument('--cache-info', action='store_true', help='显示缓存信息')
    parser.add_argument('--list-tables', action='store_true', help='列出可用表')
    
    # 查询相关参数
    parser.add_argument('--query', type=str, help='要执行的自然语言查询')
    parser.add_argument('--interactive', action='store_true', help='交互模式')
    
    # 性能监控参数
    parser.add_argument('--show-pool-status', action='store_true', help='显示连接池状态')
    
    # API密钥参数
    parser.add_argument('--api-key', type=str, help='直接提供DeepSeek API密钥')
    
    # 添加可选的位置参数用于直接接收查询文本
    parser.add_argument('query_text', nargs='*', help='直接输入查询文本')
    
    args = parser.parse_args()
    
    # 如果同时提供了位置参数和--query参数，优先使用--query
    if not args.query and args.query_text:
        args.query = ' '.join(args.query_text)
        
    return args

def init_environment(cmd_api_key=None):
    """初始化环境，获取API密钥
    
    参数:
        cmd_api_key: 命令行提供的API密钥
    
    尝试以下方式获取API密钥（按优先级）:
    1. 命令行参数
    2. 配置文件
    3. 环境变量
    4. .env文件
    """
    # 方法1: 使用命令行提供的API密钥
    if cmd_api_key:
        logger.info('使用命令行提供的API密钥')
        return cmd_api_key
        
    # 方法2: 从配置文件读取API密钥
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config', 'api_key.txt')
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r') as f:
                api_key = f.read().strip()
            if api_key:
                logger.info('从配置文件读取API密钥')
                return api_key
        except Exception as e:
            logger.warning(f'读取配置文件失败: {str(e)}')
    
    # 方法3: 从环境变量读取API密钥
    # 尝试多种环境变量名，增加兼容性
    for env_name in ['DEEPSEEK_API_KEY', 'DEEPSEEK_KEY', 'DEEPSEEK_TOKEN']:
        api_key = os.environ.get(env_name)
        if api_key and api_key.strip():
            # 移除可能的引号
            api_key = api_key.strip().strip('"\'')
            if api_key:
                logger.info(f'使用环境变量 {env_name}')
                return api_key
    
    # 方法4: 直接尝试读取.env文件 (以防dotenv库未安装或加载失败)
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
    if os.path.exists(env_path):
        try:
            with open(env_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        parts = line.split('=', 1)
                        if len(parts) == 2:
                            key, value = parts
                            if key in ['DEEPSEEK_API_KEY', 'DEEPSEEK_KEY', 'DEEPSEEK_TOKEN']:
                                # 移除可能的引号
                                value = value.strip().strip('"\'')
                                if value:
                                    logger.info(f'直接从.env文件读取API密钥')
                                    return value
        except Exception as e:
            logger.warning(f'读取.env文件失败: {str(e)}')
    
    # 如果都失败了，提示用户
    logger.error('未找到有效的API密钥')
    raise ValueError('请提供DeepSeek API密钥: 可通过环境变量DEEPSEEK_API_KEY、命令行参数--api-key、.env文件或配置文件config/api_key.txt设置')

def main():
    """主函数"""
    start_time = time.time()
    print(f"程序开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 解析命令行参数
    args = parse_arguments()
    
    # 初始化环境
    try:
        api_key = init_environment(cmd_api_key=args.api_key)
    except ValueError as e:
        print(f"错误: {str(e)}")
        return
    
    # 显示连接池状态
    if args.show_pool_status:
        pool_status = get_pool_status()
        print("\n=== 数据库连接池状态 ===")
        for key, value in pool_status.items():
            print(f"{key}: {value}")
        print("=========================\n")
    
    # 测试数据库连接
    print("数据库连接初始化...")
    connection_start = time.time()
    # 预先初始化连接池配置，但尚未实际建立连接
    engine = get_db_engine()
    print(f"数据库连接初始化完成，耗时: {time.time() - connection_start:.2f}秒")
    
    # 缓存参数
    use_cache = not args.no_cache
    force_rebuild = args.rebuild_cache
    print(f"缓存状态: {'禁用' if args.no_cache else '启用'}{', 强制重建' if force_rebuild else ''}")
    
    # 初始化查询机器人
    bot_init_start = time.time()
    query_bot = QueryBot(api_key=api_key)
    query_bot.build_engine(
        engine=engine,
        use_cache=use_cache, 
        force_rebuild=force_rebuild
    )
    bot_init_time = time.time() - bot_init_start
    print("查询机器人初始化完成")
    
    # 获取可用表列表
    available_tables = query_bot.tables_data
    table_previews = [f"{t['name']}({t.get('comment', '')})" for t in available_tables[:5]]
    print(f"可用表: {len(available_tables)} 张，包括: {', '.join(table_previews)}... 等\n")
    
    # 列出所有表
    if args.list_tables:
        print("\n=== 可用表列表 ===")
        for table in available_tables:
            print(f"{table['name']} - {table.get('comment', '无描述')}")
        print("==================\n")
    
    # 显示初始化时间
    init_time = time.time() - start_time
    print(f"系统初始化耗时: {init_time:.2f} 秒")
    
    # 交互模式
    if args.interactive:
        print("\n=== 进入交互模式 ===")
        print("输入SQL查询 (输入'exit'退出):")
        
        while True:
            user_input = input("\n请输入查询: ")
            if user_input.lower() in ('exit', 'quit', 'q'):
                break
                
            query_start = time.time()
            result = query_bot.query(user_input)
            query_time = time.time() - query_start
            
            if result['status'] == 'success':
                print("\n生成的SQL:")
                print(result['sql'])
                print("\n查询结果:")
                print(result['result'])
                print(f"\n查询耗时: {query_time:.2f}秒")
                # 简化的性能输出，详细报告将在程序结束时显示
                if 'performance' in result:
                    perf = result['performance']
                    print(f"其中: LLM生成SQL: {perf.get('llm_time', 0):.2f}秒")
            else:
                print(f"\n查询失败: {result.get('error', '未知错误')}")
        
        print("退出交互模式")
    
    # 单次查询
    elif args.query:
        query_start = time.time()
        result = query_bot.query(args.query)
        query_time = time.time() - query_start
        
        if result['status'] == 'success':
            print("\n生成的SQL:")
            print(result['sql'])
            print("\n查询结果:")
            print(result['result'])
            print(f"\n查询耗时: {query_time:.2f}秒")
            # 简化的性能输出
            if 'performance' in result:
                perf = result['performance']
                print(f"其中: LLM生成SQL: {perf.get('llm_time', 0):.2f}秒")
        else:
            print(f"\n查询失败: {result.get('error', '未知错误')}")
    
    # 显示连接池状态
    if args.show_pool_status or args.interactive:
        pool_status = get_pool_status()
        print("\n=== 程序结束时数据库连接池状态 ===")
        for key, value in pool_status.items():
            print(f"{key}: {value}")
        print("====================================\n")
    
    # 打印性能报告
    query_bot.print_performance_report()
    
    total_time = time.time() - start_time
    print(f"\n程序总运行时间: {total_time:.2f}秒")

if __name__ == "__main__":
    # 如果API密钥配置文件不存在，自动创建配置目录
    config_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config')
    if not os.path.exists(config_dir):
        os.makedirs(config_dir)
        print(f"已创建配置目录: {config_dir}")
        
    # 检查API密钥配置
    api_key_path = os.path.join(config_dir, 'api_key.txt')
    if not os.path.exists(api_key_path):
        print("提示: 您可以在以下位置创建API密钥配置文件，避免每次都需要设置环境变量:")
        print(f"  {api_key_path}")
        print("文件中只需包含您的DeepSeek API密钥字符串即可。")
        print("或者使用命令行参数: --api-key YOUR_API_KEY")
        print("或者设置环境变量: DEEPSEEK_API_KEY=YOUR_API_KEY\n")
    
    # 检查数据库配置
    db_config_path = os.path.join(config_dir, 'db_config.json')
    if not os.path.exists(db_config_path):
        # 数据库配置文件会在database.py中自动创建
        print("注意: 程序将创建默认数据库配置文件，首次运行可能需要您修改配置。")
        print(f"配置文件位置: {db_config_path}\n")
    
    main()