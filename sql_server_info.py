# -*- coding: utf-8 -*-
"""
檢查 SQL Server 詳細資訊
"""

import pyodbc
from config import DB_CONFIG

def check_sql_server_info():
    """檢查 SQL Server 資訊"""
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
        
        print("🔍 連接到 SQL Server...")
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        print("✅ 連線成功！")
        print("\n📊 SQL Server 資訊:")
        print("=" * 50)
        
        # 檢查版本
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()[0]
        print(f"版本: {version}")
        
        # 檢查伺服器名稱
        cursor.execute("SELECT @@SERVERNAME")
        server_name = cursor.fetchone()[0]
        print(f"伺服器名稱: {server_name}")
        
        # 檢查執行個體名稱
        cursor.execute("SELECT SERVERPROPERTY('InstanceName')")
        instance_name = cursor.fetchone()[0]
        print(f"執行個體名稱: {instance_name}")
        
        # 檢查資料庫引擎版本
        cursor.execute("SELECT SERVERPROPERTY('ProductVersion')")
        product_version = cursor.fetchone()[0]
        print(f"產品版本: {product_version}")
        
        # 檢查資料庫引擎版本
        cursor.execute("SELECT SERVERPROPERTY('ProductLevel')")
        product_level = cursor.fetchone()[0]
        print(f"產品等級: {product_level}")
        
        # 檢查資料庫引擎版本
        cursor.execute("SELECT SERVERPROPERTY('Edition')")
        edition = cursor.fetchone()[0]
        print(f"版本類型: {edition}")
        
        # 檢查是否為 Express 版本
        cursor.execute("SELECT SERVERPROPERTY('EngineEdition')")
        engine_edition = cursor.fetchone()[0]
        print(f"引擎版本: {engine_edition}")
        
        # 檢查現有資料庫
        print("\n📁 現有資料庫:")
        print("=" * 50)
        cursor.execute("SELECT name, database_id, create_date FROM sys.databases ORDER BY name")
        databases = cursor.fetchall()
        for db in databases:
            print(f"  {db[0]} (ID: {db[1]}, 建立日期: {db[2]})")
        
        # 檢查使用者權限
        print("\n👤 使用者權限:")
        print("=" * 50)
        cursor.execute("SELECT name, type_desc, is_disabled FROM sys.server_principals WHERE name = 'microsys'")
        user_info = cursor.fetchone()
        if user_info:
            print(f"  使用者: {user_info[0]}")
            print(f"  類型: {user_info[1]}")
            print(f"  是否停用: {user_info[2]}")
        else:
            print("  找不到使用者 'microsys'")
        
        # 檢查伺服器角色
        cursor.execute("""
            SELECT r.name as role_name
            FROM sys.server_role_members rm
            JOIN sys.server_principals r ON rm.role_principal_id = r.principal_id
            JOIN sys.server_principals m ON rm.member_principal_id = m.principal_id
            WHERE m.name = 'microsys'
        """)
        roles = cursor.fetchall()
        if roles:
            print("  伺服器角色:")
            for role in roles:
                print(f"    - {role[0]}")
        else:
            print("  沒有伺服器角色")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ 檢查失敗: {str(e)}")

if __name__ == "__main__":
    print("🚀 SQL Server 資訊檢查工具")
    print("=" * 60)
    check_sql_server_info()
    print("\n" + "=" * 60)
    print("檢查完成")
