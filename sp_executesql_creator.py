# -*- coding: utf-8 -*-
"""
使用 sp_executesql 建立資料庫
"""

import pyodbc
from config import DB_CONFIG, DATABASES

def create_database_with_sp_executesql(db_name):
    """使用 sp_executesql 建立資料庫"""
    try:
        # 連接到 master 資料庫
        conn_str = (
            f"DRIVER={{{DB_CONFIG['driver']}}};"
            f"SERVER={DB_CONFIG['server']};"
            f"UID={DB_CONFIG['username']};"
            f"PWD={DB_CONFIG['password']};"
            f"Trusted_Connection={DB_CONFIG['trusted_connection']};"
            f"Encrypt={DB_CONFIG['encrypt']};"
            "Database=master;"
        )
        
        print(f"🔍 嘗試建立資料庫: {db_name}")
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        # 檢查資料庫是否已存在
        cursor.execute(f"SELECT name FROM sys.databases WHERE name = '{db_name}'")
        if cursor.fetchone():
            print(f"ℹ️ 資料庫 {db_name} 已存在")
            conn.close()
            return True
        
        # 使用 sp_executesql 執行 CREATE DATABASE
        print(f"📝 正在建立資料庫 {db_name}...")
        
        # 方法 1: 使用 sp_executesql
        sql = f"EXEC sp_executesql N'CREATE DATABASE [{db_name}]'"
        cursor.execute(sql)
        
        print(f"✅ 資料庫 {db_name} 建立成功")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ 建立資料庫 {db_name} 失敗: {str(e)}")
        return False

def create_database_with_batch(db_name):
    """使用批次執行建立資料庫"""
    try:
        # 連接到 master 資料庫
        conn_str = (
            f"DRIVER={{{DB_CONFIG['driver']}}};"
            f"SERVER={DB_CONFIG['server']};"
            f"UID={DB_CONFIG['username']};"
            f"PWD={DB_CONFIG['password']};"
            f"Trusted_Connection={DB_CONFIG['trusted_connection']};"
            f"Encrypt={DB_CONFIG['encrypt']};"
            "Database=master;"
        )
        
        print(f"🔍 嘗試建立資料庫: {db_name}")
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        # 檢查資料庫是否已存在
        cursor.execute(f"SELECT name FROM sys.databases WHERE name = '{db_name}'")
        if cursor.fetchone():
            print(f"ℹ️ 資料庫 {db_name} 已存在")
            conn.close()
            return True
        
        # 使用批次執行
        print(f"📝 正在建立資料庫 {db_name}...")
        
        # 設定自動提交
        conn.autocommit = True
        
        # 執行 CREATE DATABASE
        cursor.execute(f"CREATE DATABASE [{db_name}]")
        
        print(f"✅ 資料庫 {db_name} 建立成功")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ 建立資料庫 {db_name} 失敗: {str(e)}")
        return False

def main():
    """主函數"""
    print("🚀 sp_executesql 資料庫建立工具")
    print("=" * 50)
    
    print("方法 1: 使用 sp_executesql")
    print("-" * 30)
    
    success_count = 0
    total_count = len(DATABASES)
    
    for db_name in DATABASES.values():
        if create_database_with_sp_executesql(db_name):
            success_count += 1
        print()
    
    if success_count == total_count:
        print("🎉 所有資料庫建立完成！")
        return
    
    print("方法 1 失敗，嘗試方法 2...")
    print("\n方法 2: 使用批次執行")
    print("-" * 30)
    
    success_count = 0
    for db_name in DATABASES.values():
        if create_database_with_batch(db_name):
            success_count += 1
        print()
    
    print("=" * 50)
    print(f"📊 結果: {success_count}/{total_count} 個資料庫建立成功")
    
    if success_count == total_count:
        print("🎉 所有資料庫建立完成！")
    else:
        print("⚠️ 部分資料庫建立失敗")

if __name__ == "__main__":
    main()
