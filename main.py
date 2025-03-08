import os
import time
import argparse
from src.logger import setup_logging
from src.database import get_db_engine, get_pool_status
from src.query_bot import QueryBot
from src.config import DOTENV_PATH
from src.utils import load_api_key

# 初始化日志
setup_logging()

# 尝试加载.env文件中的环境变量
try:
    from dotenv import load_dotenv
    if os.path.exists(DOTENV_PATH):
        load_dotenv(DOTENV_PATH)
except ImportError:
    print("提示: 安装python-dotenv库可自动加载环境变量")

def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='SQL查询助手')
    
    # 缓存相关参数
    parser.add_argument('--no-cache', action='store_true', help='禁用缓存')
    parser.add_argument('--rebuild-cache', action='store_true', help='强制重建缓存')
    parser.add_argument('--list-tables', action='store_true', help='列出可用表')
    
    # 查询相关参数
    parser.add_argument('--query', type=str, help='要执行的自然语言查询')
    parser.add_argument('--interactive', action='store_true', help='交互模式')
    
    # 性能监控参数
    parser.add_argument('--show-pool-status', action='store_true', help='显示连接池状态')
    
    # API密钥参数
    parser.add_argument('--api-key', type=str, help='直接提供DeepSeek API密钥')
    
    # 测试模式
    parser.add_argument('--test-mode', action='store_true', help='使用测试模式（不需要真实数据库）')
    
    # 添加可选的位置参数用于直接接收查询文本
    parser.add_argument('query_text', nargs='*', help='直接输入查询文本')
    
    args = parser.parse_args()
    
    # 如果同时提供了位置参数和--query参数，优先使用--query
    if not args.query and args.query_text:
        args.query = ' '.join(args.query_text)
        
    return args

def is_test_mode_enabled(cmd_arg=False):
    """检查是否启用了测试模式"""
    # 首先检查命令行参数，这是最高优先级
    if cmd_arg:
        print("\n测试模式已通过命令行参数启用")
        return True
        
    # 然后检查配置文件，但前提是没有提供查询参数
    # 如果提供了查询参数，我们假设用户想要使用真实数据库
    import sys
    if len(sys.argv) <= 1 or (len(sys.argv) > 1 and sys.argv[1].startswith('--')):
        import os
        import json
        from src.config import CONFIG_DIR
        
        test_config_path = os.path.join(CONFIG_DIR, 'test_mode.json')
        if os.path.exists(test_config_path):
            try:
                with open(test_config_path, 'r') as f:
                    config = json.load(f)
                    if config.get('mode') == 'test':
                        print("\n测试模式已通过配置文件启用")
                        print("如需使用真实数据库，请删除或重命名文件:", test_config_path)
                        return True
            except:
                pass
    
    return False

def main():
    """主函数"""
    start_time = time.time()
    print(f"\n=== SQL查询助手 ===")
    
    # 解析命令行参数
    args = parse_arguments()
    
    # 检查是否有查询参数和测试模式同时存在，优先使用真实数据库
    has_query = args.query or args.interactive
    force_real_db = has_query and not args.test_mode
    
    # 检查测试模式，但如果存在查询且未明确指定测试模式，则使用真实数据库
    test_mode = False if force_real_db else is_test_mode_enabled(args.test_mode)
    
    if test_mode:
        print("\n⚠️ 运行在测试模式 - 使用SQLite内存数据库")
    else:
        print("\n使用真实数据库连接")
        
    # 初始化环境
    try:
        api_key = load_api_key(args.api_key)
    except ValueError as e:
        print(f"错误: {str(e)}")
        return

    # 初始化数据库连接
    try:
        engine = get_db_engine(test_mode=test_mode)
        if test_mode:
            # 在测试模式下创建一些示例表
            from src.test_utils import create_test_db
            create_test_db(engine)
    except Exception as e:
        print(f"\n错误: 无法连接到数据库 - {str(e)}")
        print("\n您有以下选项:")
        print("1. 运行 'python setup_db.py' 配置数据库连接参数")
        print("2. 使用 'python main.py --test-mode' 在测试模式下运行")
        return
    
    # 显示连接池状态（如果需要）
    if args.show_pool_status:
        pool_status = get_pool_status()
        print("\n=== 数据库连接池状态 ===")
        for key, value in pool_status.items():
            print(f"{key}: {value}")
    
    # 初始化查询机器人
    try:
        print("\n正在初始化系统...")
        bot_init_start = time.time()
        query_bot = QueryBot(api_key=api_key)
        query_bot.build_engine(
            engine=engine,
            use_cache=not args.no_cache,
            force_rebuild=args.rebuild_cache
        )
        bot_init_time = time.time() - bot_init_start
        print(f"系统初始化完成，耗时: {bot_init_time:.2f}秒")
    except Exception as e:
        print(f"\n错误: 初始化查询系统失败 - {str(e)}")
        import traceback
        traceback.print_exc()
        return
    
    # 列出所有表（如果需要）
    if args.list_tables:
        print("\n=== 可用表列表 ===")
        for table in query_bot.tables_data:
            print(f"{table['name']} - {table.get('comment', '无描述')}")
        print("==================")
    
    # 交互模式
    if args.interactive:
        print("\n=== 进入交互模式 ===")
        print("输入查询内容（输入'exit'退出）:")
        
        while True:
            user_input = input("\n> ")
            if user_input.lower() in ('exit', 'quit', 'q'):
                break
                
            result = query_bot.query(user_input)
            
            if result['status'] == 'success':
                print("\n生成的SQL:")
                print(result['sql'])
                print("\n查询结果:")
                print(result['result'])
                if 'performance' in result:
                    perf = result['performance']
                    print(f"\n查询耗时: {perf.get('total_time', 0):.2f}秒")
            else:
                print(f"\n查询失败: {result.get('error', '未知错误')}")
        
        print("\n已退出交互模式")
    
    # 单次查询
    elif args.query:
        result = query_bot.query(args.query)
        
        if result['status'] == 'success':
            print("\n生成的SQL:")
            print(result['sql'])
            print("\n查询结果:")
            print(result['result'])
            if 'performance' in result:
                perf = result['performance']
                print(f"\n查询耗时: {perf.get('total_time', 0):.2f}秒")
        else:
            print(f"\n查询失败: {result.get('error', '未知错误')}")
    
    # 显示连接池状态（如果需要）
    if args.show_pool_status or args.interactive:
        pool_status = get_pool_status()
        print("\n=== 连接池状态 ===")
        for key, value in pool_status.items():
            print(f"{key}: {value}")
    
    # 打印简要的性能报告
    print("\n=== 性能统计 ===")
    init_stats = query_bot.performance_stats.get('initialization', {})
    if init_stats:
        print(f"初始化总时间: {init_stats.get('total_time', 0):.2f}秒")
        print(f"数据来源: {'缓存' if init_stats.get('from_cache') else '完整构建'}")
    
    queries = query_bot.performance_stats.get('queries', [])
    if queries:
        total_times = [q.get('total_time', 0) for q in queries if 'total_time' in q]
        if total_times:
            print(f"平均查询时间: {sum(total_times)/len(total_times):.2f}秒")
    
    print(f"程序总运行时间: {time.time() - start_time:.2f}秒")
    print("="*20)

if __name__ == "__main__":
    main()