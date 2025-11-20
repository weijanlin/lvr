# -*- coding: utf-8 -*-
"""
檢查資料庫結構和資料表
"""

import pyodbc
from config import DB_CONFIG, DATABASES

def check_database_structure():
    """檢查資料庫結構"""
    try:
        print("🔍 檢查資料庫結構...")
        print("=" * 60)
        
        for db_name in DATABASES.values():
            print(f"\n📊 檢查資料庫: {db_name}")
            print("-" * 40)
            
            try:
                # 連接到指定資料庫
                conn_str = (
                    f"DRIVER={{{DB_CONFIG['driver']}}};"
                    f"SERVER={DB_CONFIG['server']};"
                    f"UID={DB_CONFIG['username']};"
                    f"PWD={DB_CONFIG['password']};"
                    f"Trusted_Connection={DB_CONFIG['trusted_connection']};"
                    f"Encrypt={DB_CONFIG['encrypt']};"
                    f"Database={db_name};"
                )
                
                conn = pyodbc.connect(conn_str)
                cursor = conn.cursor()
                
                # 檢查資料表
                cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'")
                tables = cursor.fetchall()
                
                if tables:
                    print(f"✅ 找到 {len(tables)} 個資料表:")
                    for table in tables:
                        table_name = table[0]
                        print(f"   📋 {table_name}")
                        
                        # 檢查資料表結構
                        cursor.execute(f"SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position")
                        columns = cursor.fetchall()
                        
                        print(f"      📊 欄位數: {len(columns)}")
                        for col in columns[:5]:  # 只顯示前5個欄位
                            print(f"         - {col[0]} ({col[1]}, {'可空' if col[2] == 'YES' else '不可空'})")
                        
                        if len(columns) > 5:
                            print(f"         ... 還有 {len(columns) - 5} 個欄位")
                        
                        # 檢查資料筆數
                        try:
                            cursor.execute(f"SELECT COUNT(*) FROM [{table_name}]")
                            row_count = cursor.fetchone()[0]
                            print(f"      📝 資料筆數: {row_count}")
                        except Exception as e:
                            print(f"      ❌ 無法取得資料筆數: {str(e)}")
                        
                        print()
                else:
                    print(f"⚠️ 沒有找到任何資料表")
                
                conn.close()
                
            except Exception as e:
                print(f"❌ 檢查資料庫 {db_name} 失敗: {str(e)}")
        
        print("=" * 60)
        print("✅ 資料庫結構檢查完成")
        
    except Exception as e:
        print(f"❌ 檢查失敗: {str(e)}")

def create_missing_tables():
    """建立缺少的資料表"""
    try:
        print("🔧 建立缺少的資料表...")
        print("=" * 60)
        
        # 為每個資料庫建立資料表
        for db_name in DATABASES.values():
            print(f"\n📊 處理資料庫: {db_name}")
            print("-" * 40)
            
            try:
                conn_str = (
                    f"DRIVER={{{DB_CONFIG['driver']}}};"
                    f"SERVER={DB_CONFIG['server']};"
                    f"UID={DB_CONFIG['username']};"
                    f"PWD={DB_CONFIG['password']};"
                    f"Trusted_Connection={DB_CONFIG['trusted_connection']};"
                    f"Encrypt={DB_CONFIG['encrypt']};"
                    f"Database={db_name};"
                )
                
                conn = pyodbc.connect(conn_str)
                cursor = conn.cursor()
        
                # 建立主要資料表
                tables = {
                    'main_data': [
                        '鄉鎮市區 NVARCHAR(50)',
                        '交易標的 NVARCHAR(100)',
                        '土地位置建物門牌 NVARCHAR(200)',
                        '土地移轉總面積平方公尺 DECIMAL(10,2)',
                        '都市土地使用分區 NVARCHAR(100)',
                        '非都市土地使用分區 NVARCHAR(100)',
                        '非都市土地使用編定 NVARCHAR(100)',
                        '交易年月日 NVARCHAR(20)',
                        '交易筆棟數 INT',
                        '移轉層次 NVARCHAR(50)',
                        '總樓層數 INT',
                        '建物型態 NVARCHAR(100)',
                        '主要用途 NVARCHAR(100)',
                        '主要建材 NVARCHAR(100)',
                        '建築完成年月 NVARCHAR(20)',
                        '建物移轉總面積平方公尺 DECIMAL(10,2)',
                        '建物現況格局-房 INT',
                        '建物現況格局-廳 INT',
                        '建物現況格局-衛 INT',
                        '建物現況格局-隔間 NVARCHAR(50)',
                        '有無管理組織 NVARCHAR(20)',
                        '總價元 DECIMAL(15,2)',
                        '單價元平方公尺 DECIMAL(15,2)',
                        '車位類別 NVARCHAR(50)',
                        '車位移轉總面積平方公尺 DECIMAL(10,2)',
                        '車位總價元 DECIMAL(15,2)',
                        '備註 NVARCHAR(500)',
                        '編號 NVARCHAR(50)',
                        '主建物面積 DECIMAL(10,2)',
                        '附屬建物面積 DECIMAL(10,2)',
                        '陽台面積 DECIMAL(10,2)',
                        '電梯 NVARCHAR(20)',
                        '移轉編號 NVARCHAR(50)'
                    ],
                    'build_data': [
                        '編號 NVARCHAR(50)',
                        '屋齡 INT',
                        '建物移轉面積平方公尺 DECIMAL(10,2)',
                        '主要用途 NVARCHAR(100)',
                        '主要建材 NVARCHAR(100)',
                        '建築完成年月 NVARCHAR(20)',
                        '建物現況格局-房 INT',
                        '建物現況格局-廳 INT',
                        '建物現況格局-衛 INT',
                        '建物現況格局-隔間 NVARCHAR(50)'
                    ],
                    'land_data': [
                        '編號 NVARCHAR(50)',
                        '土地位置 NVARCHAR(200)',
                        '土地移轉面積平方公尺 DECIMAL(10,2)',
                        '使用分區或編定 NVARCHAR(100)',
                        '權利人持分分母 INT',
                        '權利人持分分子 INT',
                        '移轉情形 NVARCHAR(100)',
                        '地號 NVARCHAR(50)'
                    ],
                    'park_data': [
                        '編號 NVARCHAR(50)',
                        '車位類別 NVARCHAR(50)',
                        '車位價格 DECIMAL(15,2)',
                        '車位面積平方公尺 DECIMAL(10,2)',
                        '車位所在樓層 NVARCHAR(50)'
                    ]
                }
                
                for table_name, columns in tables.items():
                    try:
                        # 檢查資料表是否存在
                        cursor.execute(f"SELECT table_name FROM information_schema.tables WHERE table_name = '{table_name}'")
                        if not cursor.fetchone():
                            # 建立資料表
                            create_sql = f"""
                            CREATE TABLE [{table_name}] (
                                [id] BIGINT IDENTITY(1,1) PRIMARY KEY,
                                {', '.join([f'[{col.split()[0]}] {col.split()[1]}' for col in columns])},
                                [created_at] DATETIME2 DEFAULT GETDATE(),
                                [source_file] NVARCHAR(200),
                                [quarter] NVARCHAR(10)
                            )
                            """
                            
                            cursor.execute(create_sql)
                            print(f"✅ 已建立資料表: {table_name}")
                        else:
                            print(f"ℹ️ 資料表已存在: {table_name}")
                            
                    except Exception as e:
                        print(f"❌ 建立資料表 {table_name} 失敗: {str(e)}")
                
                conn.commit()
                conn.close()
                
            except Exception as e:
                print(f"❌ 處理資料庫 {db_name} 失敗: {str(e)}")
        
        print("=" * 60)
        print("✅ 資料表建立完成")
        
    except Exception as e:
        print(f"❌ 建立資料表失敗: {str(e)}")

def main():
    """主函數"""
    print("🚀 資料庫結構檢查工具")
    print("=" * 60)
    
    print("請選擇:")
    print("1. 檢查資料庫結構")
    print("2. 建立缺少的資料表")
    print("3. 退出")
    
    choice = input("\n請輸入選擇 (1-3): ").strip()
    
    if choice == '1':
        check_database_structure()
    elif choice == '2':
        create_missing_tables()
    elif choice == '3':
        print("👋 再見！")
    else:
        print("❌ 無效的選擇")


if __name__ == "__main__":
    main()
