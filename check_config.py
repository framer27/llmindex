"""
检查并管理配置文件
"""
import os
import json
import sys

# 获取配置目录路径
script_dir = os.path.dirname(os.path.abspath(__file__))
config_dir = os.path.join(script_dir, 'config')

def check_config_files():
    """检查配置目录中的文件"""
    print("\n=== 配置文件检查 ===")
    
    if not os.path.exists(config_dir):
        print(f"配置目录不存在: {config_dir}")
        return
        
    print(f"配置目录: {config_dir}")
    
    # 列出所有配置文件
    files = os.listdir(config_dir)
    if not files:
        print("配置目录为空")
        return
        
    print("\n发现的配置文件:")
    for file in files:
        file_path = os.path.join(config_dir, file)
        file_size = os.path.getsize(file_path)
        print(f"- {file} ({file_size} 字节)")
        
        # 检查测试模式配置
        if file == 'test_mode.json':
            try:
                with open(file_path, 'r') as f:
                    config = json.load(f)
                    mode = config.get('mode', 'unknown')
                    print(f"  [测试模式配置] 模式: {mode}")
                    
                    if mode == 'test':
                        print(f"  这个文件会让系统默认使用测试模式而不是真实数据库。")
                        action = input("  是否删除此文件以使用真实数据库? (y/n): ").lower()
                        if action == 'y':
                            os.remove(file_path)
                            print(f"  ✓ 已删除 {file}")
            except:
                print(f"  [错误] 无法读取测试模式配置")
                
        # 检查数据库配置
        elif file == 'db_config.json' or file == 'db_config.yaml':
            print(f"  [数据库配置] 此文件包含数据库连接信息")
    
    print("\n配置检查完成!")

if __name__ == "__main__":
    check_config_files()
    
    print("\n您现在可以尝试以下命令来使用真实数据库:")
    print("python main.py --query \"查询所有产品\"")
    print("或者直接使用:")
    print("python main.py 查询所有产品") 