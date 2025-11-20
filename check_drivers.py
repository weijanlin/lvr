# -*- coding: utf-8 -*-
"""
檢查系統中可用的 ODBC 驅動程式
"""

import pyodbc

def check_available_drivers():
    """檢查可用的 ODBC 驅動程式"""
    print("🔍 檢查系統中可用的 ODBC 驅動程式...")
    print("=" * 60)
    
    try:
        drivers = pyodbc.drivers()
        if drivers:
            print("✅ 找到以下 ODBC 驅動程式:")
            for i, driver in enumerate(drivers, 1):
                print(f"   {i:2d}. {driver}")
        else:
            print("❌ 沒有找到任何 ODBC 驅動程式")
            
    except Exception as e:
        print(f"❌ 檢查驅動程式時發生錯誤: {str(e)}")
    
    print("\n" + "=" * 60)
    
    # 檢查 SQL Server 相關的驅動程式
    sql_server_drivers = []
    for driver in drivers:
        if 'SQL Server' in driver or 'ODBC Driver' in driver:
            sql_server_drivers.append(driver)
    
    if sql_server_drivers:
        print("🎯 SQL Server 相關驅動程式:")
        for driver in sql_server_drivers:
            print(f"   ✅ {driver}")
    else:
        print("⚠️ 沒有找到 SQL Server 相關的驅動程式")
        print("\n💡 建議安裝以下驅動程式之一:")
        print("   - Microsoft ODBC Driver 18 for SQL Server")
        print("   - Microsoft ODBC Driver 17 for SQL Server")
        print("   - SQL Server Native Client 11.0")

def check_sql_server_status():
    """檢查 SQL Server 服務狀態的建議"""
    print("\n🔧 SQL Server 連線問題診斷:")
    print("=" * 60)
    print("1. 確認 SQL Server 服務狀態:")
    print("   - 開啟 '服務' (services.msc)")
    print("   - 尋找 'SQL Server (SQLEXPRESS)' 或 'SQL Server (MSSQLSERVER)'")
    print("   - 確認服務狀態為 '正在執行'")
    
    print("\n2. 確認 SQL Server 設定:")
    print("   - 開啟 SQL Server Configuration Manager")
    print("   - 確認 'SQL Server Network Configuration' 中的 TCP/IP 已啟用")
    print("   - 確認 'SQL Server Services' 中的 SQL Server 服務正在執行")
    
    print("\n3. 確認防火牆設定:")
    print("   - 檢查 Windows 防火牆是否允許 SQL Server 埠口 (預設 1433)")
    print("   - 或暫時關閉防火牆進行測試")
    
    print("\n4. 確認連線字串:")
    print("   - 預設執行個體: 127.0.0.1 或 localhost")
    print("   - 命名執行個體: 127.0.0.1\\SQLEXPRESS")
    print("   - 確認執行個體名稱是否正確")

if __name__ == "__main__":
    print("🚀 ODBC 驅動程式檢查工具")
    print("=" * 60)
    
    check_available_drivers()
    check_sql_server_status()
    
    print("\n" + "=" * 60)
    print("檢查完成")
