# -*- coding: utf-8 -*-
"""
資料庫連線測試腳本
用於測試 SQL Server 連線是否正常
"""

import pyodbc
import sys
from config import DB_CONFIG

def test_basic_connection():
    """測試基本資料庫連線"""
    print("🔍 測試 SQL Server 連線...")
    
    # 建立連線字串
    conn_str = (
        f"DRIVER={{{DB_CONFIG['driver']}}};"
        f"SERVER={DB_CONFIG['server']};"
        f"UID={DB_CONFIG['username']};"
        f"PWD={DB_CONFIG['password']};"
        f"Trusted_Connection={DB_CONFIG['trusted_connection']};"
        f"Encrypt={DB_CONFIG['encrypt']};"
    )
    
    try:
        # 嘗試連接到 master 資料庫
        print(f"📡 連接到: {DB_CONFIG['server']}")
        print(f"👤 使用者: {DB_CONFIG['username']}")
        print(f"🔑 驅動程式: {DB_CONFIG['driver']}")
        
        conn = pyodbc.connect(conn_str + "Database=master;")
        cursor = conn.cursor()
        
        # 測試查詢
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()[0]
        print(f"✅ 連線成功！")
        print(f"📊 SQL Server 版本: {version}")
        
        # 檢查可用的驅動程式
        print("\n🔧 可用的 ODBC 驅動程式:")
        drivers = pyodbc.drivers()
        for driver in drivers:
            print(f"   - {driver}")
        
        conn.close()
        return True
        
    except pyodbc.Error as e:
        print(f"❌ 連線失敗: {str(e)}")
        
        # 提供常見問題的解決方案
        print("\n💡 常見問題解決方案:")
        print("1. 確認 SQL Server 服務是否正在執行")
        print("2. 確認防火牆設定是否允許連線")
        print("3. 確認 SQL Server 是否允許遠端連線")
        print("4. 檢查 ODBC 驅動程式是否已安裝")
        print("5. 嘗試使用 'SQL Server Native Client 11.0' 驅動程式")
        
        return False
        
    except Exception as e:
        print(f"❌ 未知錯誤: {str(e)}")
        return False

def test_driver_alternatives():
    """測試替代的驅動程式"""
    print("\n🔄 測試替代驅動程式...")
    
    alternative_drivers = [
        'SQL Server Native Client 11.0',
        'SQL Server',
        'ODBC Driver 18 for SQL Server',
        'ODBC Driver 13 for SQL Server'
    ]
    
    for driver in alternative_drivers:
        try:
            conn_str = (
                f"DRIVER={{{driver}}};"
                f"SERVER={DB_CONFIG['server']};"
                f"UID={DB_CONFIG['username']};"
                f"PWD={DB_CONFIG['password']};"
                f"Trusted_Connection={DB_CONFIG['trusted_connection']};"
                f"Encrypt={DB_CONFIG['encrypt']};"
            )
            
            conn = pyodbc.connect(conn_str + "Database=master;")
            print(f"✅ 驅動程式 {driver} 可用")
            conn.close()
            return driver
            
        except pyodbc.Error:
            print(f"❌ 驅動程式 {driver} 不可用")
            continue
    
    return None

if __name__ == "__main__":
    print("🚀 LVR 資料庫連線測試工具")
    print("=" * 50)
    
    # 測試基本連線
    if test_basic_connection():
        print("\n🎉 連線測試成功！可以繼續進行資料庫設定")
    else:
        print("\n⚠️ 基本連線失敗，嘗試替代驅動程式...")
        
        # 嘗試替代驅動程式
        working_driver = test_driver_alternatives()
        if working_driver:
            print(f"\n✅ 找到可用的驅動程式: {working_driver}")
            print("請更新 config.py 中的 driver 設定")
        else:
            print("\n💥 所有驅動程式都無法連線")
            print("請檢查 SQL Server 設定或網路連線")
    
    print("\n" + "=" * 50)
    print("測試完成")
