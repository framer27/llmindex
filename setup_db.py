"""
数据库配置设置脚本

运行此脚本以手动设置数据库连接配置。
"""
import os
import json
import yaml
import sys
from src.config import CONFIG_DIR

def show_banner():
    """显示欢迎信息"""
    print("\n" + "="*60)
    print("            SQL查询助手 - 数据库配置工具")
    print("="*60)
    print("本工具将帮助您设置数据库连接参数。\n")
    print("您需要以下信息：")
    print("1. SQL Server 服务器地址 (格式: 服务器名称或IP地址)")
    print("2. 数据库名称")
    print("3. 用户名和密码\n")
    print("注意：对于命名实例，请使用 SERVER\\INSTANCE 格式")
    print("="*60)

def setup_database_config():
    """设置数据库连接参数"""
    show_banner()
    
    print("\n=== 数据库连接配置 ===")
    print("请输入以下信息来配置数据库连接:")
    
    # 获取服务器配置
    server = input("\n数据库服务器地址 [localhost]: ") or "localhost"
    
    # 检查是否是命名实例
    if '\\' not in server and ':' not in server:
        use_instance = input("是否使用命名实例? (y/n) [n]: ").lower() or "n"
        if use_instance == "y":
            instance = input("输入实例名称: ")
            server = f"{server}\\{instance}"
    
    database = input("\n数据库名称 [master]: ") or "master"
    username = input("\n用户名 [sa]: ") or "sa"
    password = input("\n密码: ")
    
    # 创建配置对象
    config = {
        "server": server,
        "database": database,
        "username": username,
        "password": password
    }
    
    # 确保配置目录存在
    os.makedirs(CONFIG_DIR, exist_ok=True)
    
    # 选择配置文件格式
    format_choice = input("\n选择配置文件格式 (1=JSON, 2=YAML) [1]: ") or "1"
    
    if format_choice == "2":
        # 创建YAML配置
        config_path = os.path.join(CONFIG_DIR, 'db_config.yaml')
        with open(config_path, 'w', encoding='utf-8') as f:
            yaml.dump(config, f, default_flow_style=False)
        print(f"\n✓ YAML配置文件已保存至: {config_path}")
    else:
        # 创建JSON配置
        config_path = os.path.join(CONFIG_DIR, 'db_config.json')
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2)
        print(f"\n✓ JSON配置文件已保存至: {config_path}")
    
    print("\n数据库配置已设置完成。")
    print("配置详情:")
    print(f"- 服务器: {server}")
    print(f"- 数据库: {database}")
    print(f"- 用户名: {username}")
    print(f"- 密码: {'*' * len(password) if password else '(空)'}")

def test_connection():
    """测试数据库连接"""
    try:
        print("\n正在测试数据库连接...")
        
        # 导入必要的模块
        from src.database import get_db_engine, test_connection
        
        try:
            # 获取引擎并测试连接
            engine = get_db_engine()
            success, message = test_connection(engine)
            
            if success:
                print("\n✓ 连接成功！数据库配置有效。")
                print("\n您现在可以运行 'python main.py' 来使用SQL查询助手。")
            else:
                print(f"\n✗ 连接失败: {message}")
                print("\n可能的原因:")
                print("1. 服务器地址不正确")
                print("2. SQL Server服务未运行")
                print("3. 数据库名称不存在")
                print("4. 用户名或密码错误")
                print("5. 网络连接问题")
                print("6. SQL Server不允许远程连接")
                print("\n请检查以上原因并重新运行此脚本。")
                handle_connection_failure()
        except Exception as e:
            print(f"\n✗ 数据库连接失败: {str(e)}")
            print("\n详细错误信息:")
            import traceback
            traceback.print_exc()
            handle_connection_failure()
            
    except ImportError as e:
        print(f"\n✗ 导入模块失败: {str(e)}")
        print("\n请确保已安装所有必要的依赖项:")
        print("pip install sqlalchemy pyodbc pyyaml")
        handle_connection_failure()
    except Exception as e:
        print(f"\n✗ 测试连接时出错: {str(e)}")
        print("\n这通常是由于以下原因之一:")
        print("1. SQL Server ODBC驱动程序未安装")
        print("2. 数据库服务器不可达")
        print("3. 连接配置错误")
        print("\n推荐的解决方案:")
        print("- 安装Microsoft ODBC Driver 17 for SQL Server")
        print("- 检查服务器是否在运行并可以访问")
        print("- 验证您的用户名和密码")
        handle_connection_failure()

def handle_connection_failure():
    """处理连接失败的情况"""
    print("\n您有以下选项:")
    print("1. 重新配置数据库连接")
    print("2. 使用测试模式运行 (不需要真实数据库)")
    print("3. 退出")
    
    choice = input("\n请选择一个选项 [1]: ") or "1"
    
    if choice == "1":
        setup_database_config()
        test_choice = input("\n是否再次测试连接? (y/n) [y]: ").lower() or "y"
        if test_choice == "y":
            test_connection()
    elif choice == "2":
        setup_test_mode()
    else:
        print("\n退出配置工具。")
        sys.exit(0)

def setup_test_mode():
    """设置测试模式"""
    print("\n=== 测试模式设置 ===")
    print("在测试模式下，程序将使用SQLite内存数据库，无需连接到SQL Server。")
    print("这适用于测试和演示目的，但不会访问您的真实数据。")
    
    # 创建测试模式配置文件
    test_config = {
        "mode": "test",
        "use_sqlite": True
    }
    
    config_path = os.path.join(CONFIG_DIR, 'test_mode.json')
    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(test_config, f, indent=2)
    
    print(f"\n✓ 测试模式配置已保存至: {config_path}")
    print("\n您现在可以运行 'python main.py --test-mode' 来使用SQL查询助手的测试模式。")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--test-mode":
        setup_test_mode()
        sys.exit(0)
        
    setup_database_config()
    
    # 询问是否要测试连接
    test_choice = input("\n是否测试数据库连接? (y/n) [y]: ").lower() or "y"
    if test_choice == "y":
        test_connection()
    
    print("\n配置完成！") 