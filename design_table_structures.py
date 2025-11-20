# -*- coding: utf-8 -*-
"""
設計專用的資料表結構
為預售屋和租屋建立正確的資料表
"""

from typing import Dict, List, Tuple

def get_table_structures() -> Dict[str, Dict[str, List[str]]]:
    """取得所有資料表的結構定義"""
    
    # 中古屋資料表結構 (已存在，需要更新)
    used_house_tables = {
        'main_data': [
            '鄉鎮市區 NVARCHAR(200)',
            '交易標的 NVARCHAR(200)',
            '土地位置建物門牌 NVARCHAR(500)',
            '土地移轉總面積平方公尺 DECIMAL(15,2)',
            '都市土地使用分區 NVARCHAR(500)',
            '非都市土地使用分區 NVARCHAR(200)',
            '非都市土地使用編定 NVARCHAR(200)',
            '交易年月日 NVARCHAR(20)',
            '交易筆棟數 INT',
            '移轉層次 NVARCHAR(50)',
            '總樓層數 INT',
            '建物型態 NVARCHAR(200)',
            '主要用途 NVARCHAR(200)',
            '主要建材 NVARCHAR(200)',
            '建築完成年月 NVARCHAR(20)',
            '建物移轉總面積平方公尺 DECIMAL(15,2)',
            '建物現況格局-房 INT',
            '建物現況格局-廳 INT',
            '建物現況格局-衛 INT',
            '建物現況格局-隔間 NVARCHAR(50)',
            '有無管理組織 NVARCHAR(20)',
            '總價元 DECIMAL(15,2)',
            '單價元平方公尺 DECIMAL(15,2)',
            '車位類別 NVARCHAR(50)',
            '車位移轉總面積平方公尺 DECIMAL(15,2)',
            '車位總價元 DECIMAL(15,2)',
            '備註 NVARCHAR(1000)',
            '編號 NVARCHAR(100)',
            '主建物面積 DECIMAL(15,2)',
            '附屬建物面積 DECIMAL(15,2)',
            '陽台面積 DECIMAL(15,2)',
            '電梯 NVARCHAR(20)',
            '移轉編號 NVARCHAR(100)',
            'source_file NVARCHAR(200)',
            'quarter NVARCHAR(20)'
        ],
        'build_data': [
            '編號 NVARCHAR(100)',
            '屋齡 INT',
            '建物移轉面積平方公尺 DECIMAL(15,2)',
            '主要用途 NVARCHAR(200)',
            '主要建材 NVARCHAR(200)',
            '建築完成日期 NVARCHAR(20)',
            '總層數 INT',
            '建物分層 NVARCHAR(100)',
            '移轉情形 NVARCHAR(200)',
            'source_file NVARCHAR(200)',
            'quarter NVARCHAR(20)'
        ],
        'land_data': [
            '編號 NVARCHAR(100)',
            '土地位置 NVARCHAR(200)',
            '土地移轉面積平方公尺 DECIMAL(15,2)',
            '使用分區或編定 NVARCHAR(500)',
            '權利人持分分母 DECIMAL(15,2)',
            '權利人持分分子 DECIMAL(15,2)',
            '移轉情形 NVARCHAR(200)',
            '地號 NVARCHAR(100)',
            'source_file NVARCHAR(200)',
            'quarter NVARCHAR(20)'
        ],
        'park_data': [
            '編號 NVARCHAR(100)',
            '車位類別 NVARCHAR(50)',
            '車位價格 DECIMAL(15,2)',
            '車位面積平方公尺 DECIMAL(15,2)',
            '車位所在樓層 NVARCHAR(50)',
            'source_file NVARCHAR(200)',
            'quarter NVARCHAR(20)'
        ]
    }
    
    # 預售屋資料表結構 (新建)
    presale_tables = {
        'presale_data': [
            '鄉鎮市區 NVARCHAR(200)',
            '交易標的 NVARCHAR(200)',
            '土地位置建物門牌 NVARCHAR(500)',
            '土地移轉總面積平方公尺 DECIMAL(15,2)',
            '都市土地使用分區 NVARCHAR(500)',
            '非都市土地使用分區 NVARCHAR(200)',
            '非都市土地使用編定 NVARCHAR(200)',
            '交易年月日 NVARCHAR(20)',
            '交易筆棟數 INT',
            '移轉層次 NVARCHAR(50)',
            '總樓層數 INT',
            '建物型態 NVARCHAR(200)',
            '主要用途 NVARCHAR(200)',
            '主要建材 NVARCHAR(200)',
            '建築完成年月 NVARCHAR(20)',
            '建物移轉總面積平方公尺 DECIMAL(15,2)',
            '建物現況格局-房 INT',
            '建物現況格局-廳 INT',
            '建物現況格局-衛 INT',
            '建物現況格局-隔間 NVARCHAR(50)',
            '有無管理組織 NVARCHAR(20)',
            '總價元 DECIMAL(15,2)',
            '單價元平方公尺 DECIMAL(15,2)',
            '車位類別 NVARCHAR(50)',
            '車位移轉總面積平方公尺 DECIMAL(15,2)',
            '車位總價元 DECIMAL(15,2)',
            '備註 NVARCHAR(1000)',
            '編號 NVARCHAR(100)',
            '建案名稱 NVARCHAR(200)',
            '棟及號 NVARCHAR(100)',
            '解約情形 NVARCHAR(50)',
            'source_file NVARCHAR(200)',
            'quarter NVARCHAR(20)'
        ],
        'build_data': [
            '編號 NVARCHAR(100)',
            '屋齡 INT',
            '建物移轉面積平方公尺 DECIMAL(15,2)',
            '主要用途 NVARCHAR(200)',
            '主要建材 NVARCHAR(200)',
            '建築完成日期 NVARCHAR(20)',
            '總層數 INT',
            '建物分層 NVARCHAR(100)',
            '移轉情形 NVARCHAR(200)',
            'source_file NVARCHAR(200)',
            'quarter NVARCHAR(20)'
        ],
        'land_data': [
            '編號 NVARCHAR(100)',
            '土地位置 NVARCHAR(200)',
            '土地移轉面積平方公尺 DECIMAL(15,2)',
            '使用分區或編定 NVARCHAR(500)',
            '權利人持分分母 DECIMAL(15,2)',
            '權利人持分分子 DECIMAL(15,2)',
            '移轉情形 NVARCHAR(200)',
            '地號 NVARCHAR(100)',
            'source_file NVARCHAR(200)',
            'quarter NVARCHAR(20)'
        ],
        'park_data': [
            '編號 NVARCHAR(100)',
            '車位類別 NVARCHAR(50)',
            '車位價格 DECIMAL(15,2)',
            '車位面積平方公尺 DECIMAL(15,2)',
            '車位所在樓層 NVARCHAR(50)',
            'source_file NVARCHAR(200)',
            'quarter NVARCHAR(20)'
        ]
    }
    
    # 租屋資料表結構 (新建)
    rental_tables = {
        'rental_data': [
            '鄉鎮市區 NVARCHAR(200)',
            '交易標的 NVARCHAR(200)',
            '土地位置建物門牌 NVARCHAR(500)',
            '土地面積平方公尺 DECIMAL(15,2)',
            '都市土地使用分區 NVARCHAR(500)',
            '非都市土地使用分區 NVARCHAR(200)',
            '非都市土地使用編定 NVARCHAR(200)',
            '租賃年月日 NVARCHAR(20)',
            '租賃筆棟數 INT',
            '租賃層次 NVARCHAR(50)',
            '總樓層數 INT',
            '建物型態 NVARCHAR(200)',
            '主要用途 NVARCHAR(200)',
            '主要建材 NVARCHAR(200)',
            '建築完成年月 NVARCHAR(20)',
            '建物總面積平方公尺 DECIMAL(15,2)',
            '建物現況格局-房 INT',
            '建物現況格局-廳 INT',
            '建物現況格局-衛 INT',
            '建物現況格局-隔間 NVARCHAR(50)',
            '有無管理組織 NVARCHAR(20)',
            '有無附傢俱 NVARCHAR(20)',
            '總額元 DECIMAL(15,2)',
            '單價元平方公尺 DECIMAL(15,2)',
            '車位類別 NVARCHAR(50)',
            '車位面積平方公尺 DECIMAL(15,2)',
            '車位總額元 DECIMAL(15,2)',
            '備註 NVARCHAR(1000)',
            '編號 NVARCHAR(100)',
            '出租型態 NVARCHAR(50)',
            '有無管理員 NVARCHAR(20)',
            '租賃期間 NVARCHAR(50)',
            '有無電梯 NVARCHAR(20)',
            '附屬設備 NVARCHAR(500)',
            '租賃住宅服務 NVARCHAR(200)',
            'source_file NVARCHAR(200)',
            'quarter NVARCHAR(20)'
        ],
        'build_data': [
            '編號 NVARCHAR(100)',
            '屋齡 INT',
            '建物移轉面積平方公尺 DECIMAL(15,2)',
            '主要用途 NVARCHAR(200)',
            '主要建材 NVARCHAR(200)',
            '建築完成日期 NVARCHAR(20)',
            '總層數 INT',
            '建物分層 NVARCHAR(100)',
            '移轉情形 NVARCHAR(200)',
            'source_file NVARCHAR(200)',
            'quarter NVARCHAR(20)'
        ],
        'land_data': [
            '編號 NVARCHAR(100)',
            '土地位置 NVARCHAR(200)',
            '土地移轉面積平方公尺 DECIMAL(15,2)',
            '使用分區或編定 NVARCHAR(500)',
            '權利人持分分母 DECIMAL(15,2)',
            '權利人持分分子 DECIMAL(15,2)',
            '移轉情形 NVARCHAR(200)',
            '地號 NVARCHAR(100)',
            'source_file NVARCHAR(200)',
            'quarter NVARCHAR(20)'
        ],
        'park_data': [
            '編號 NVARCHAR(100)',
            '車位類別 NVARCHAR(50)',
            '車位價格 DECIMAL(15,2)',
            '車位面積平方公尺 DECIMAL(15,2)',
            '車位所在樓層 NVARCHAR(50)',
            'source_file NVARCHAR(200)',
            'quarter NVARCHAR(20)'
        ]
    }
    
    return {
        'used_house': used_house_tables,
        'presale': presale_tables,
        'rental': rental_tables
    }

def generate_create_table_sql(database_name: str, table_name: str, columns: List[str]) -> str:
    """生成CREATE TABLE SQL語句"""
    sql = f"CREATE TABLE [{database_name}].[dbo].[{table_name}] (\n"
    sql += "    id INT IDENTITY(1,1) PRIMARY KEY,\n"
    
    for column in columns:
        sql += f"    {column},\n"
    
    sql = sql.rstrip(",\n") + "\n"
    sql += ");"
    
    return sql

def generate_drop_table_sql(database_name: str, table_name: str) -> str:
    """生成DROP TABLE SQL語句"""
    return f"DROP TABLE IF EXISTS [{database_name}].[dbo].[{table_name}];"

def print_table_structures():
    """列印所有資料表結構"""
    print("🏗️ 資料表結構設計")
    print("=" * 80)
    
    structures = get_table_structures()
    
    for db_type, tables in structures.items():
        print(f"\n📊 {db_type.upper()} 資料庫結構:")
        print("-" * 60)
        
        for table_name, columns in tables.items():
            print(f"\n🔸 資料表: {table_name}")
            print(f"   欄位數: {len(columns)}")
            print("   欄位列表:")
            for i, column in enumerate(columns, 1):
                print(f"     {i:2d}. {column}")

def generate_sql_scripts():
    """生成SQL腳本"""
    print(f"\n💾 生成SQL腳本")
    print("=" * 80)
    
    structures = get_table_structures()
    database_mapping = {
        'used_house': 'LVR_UsedHouse',
        'presale': 'LVR_PreSale', 
        'rental': 'LVR_Rental'
    }
    
    # 生成DROP TABLE腳本
    drop_script = "-- 刪除現有資料表\n"
    drop_script += "-- 注意：這會刪除所有現有資料！\n\n"
    
    for db_type, tables in structures.items():
        db_name = database_mapping[db_type]
        drop_script += f"-- {db_name} 資料庫\n"
        for table_name in tables.keys():
            drop_script += generate_drop_table_sql(db_name, table_name) + "\n"
        drop_script += "\n"
    
    # 生成CREATE TABLE腳本
    create_script = "-- 建立新的資料表\n\n"
    
    for db_type, tables in structures.items():
        db_name = database_mapping[db_type]
        create_script += f"-- {db_name} 資料庫\n"
        for table_name, columns in tables.items():
            create_script += generate_create_table_sql(db_name, table_name, columns) + "\n\n"
    
    # 儲存腳本
    try:
        with open('drop_tables.sql', 'w', encoding='utf-8') as f:
            f.write(drop_script)
        print("✅ DROP TABLE 腳本已儲存到: drop_tables.sql")
        
        with open('create_tables.sql', 'w', encoding='utf-8') as f:
            f.write(create_script)
        print("✅ CREATE TABLE 腳本已儲存到: create_tables.sql")
        
    except Exception as e:
        print(f"❌ 儲存SQL腳本失敗: {str(e)}")

def compare_with_existing():
    """與現有資料表結構比較"""
    print(f"\n🔍 與現有資料表結構比較")
    print("=" * 80)
    
    # 現有的資料表結構 (從check_database_structure.py)
    existing_tables = {
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
            '編號 NVARCHAR(100)',
            '主建物面積 DECIMAL(10,2)',
            '附屬建物面積 DECIMAL(10,2)',
            '陽台面積 DECIMAL(10,2)',
            '電梯 NVARCHAR(20)',
            '移轉編號 NVARCHAR(100)'
        ]
    }
    
    new_structures = get_table_structures()
    
    print("📋 主要變更:")
    print("1. 增加字串欄位長度 (NVARCHAR(50) → NVARCHAR(200))")
    print("2. 增加長文字欄位長度 (NVARCHAR(200) → NVARCHAR(500))")
    print("3. 增加備註欄位長度 (NVARCHAR(500) → NVARCHAR(1000))")
    print("4. 為預售屋新增專用欄位: 建案名稱, 棟及號, 解約情形")
    print("5. 為租屋新增專用欄位: 土地面積平方公尺, 租賃年月日, 租賃筆棟數等")
    print("6. 統一所有資料表都包含 source_file 和 quarter 欄位")

if __name__ == "__main__":
    print_table_structures()
    generate_sql_scripts()
    compare_with_existing()
    
    print(f"\n🎯 下一步建議:")
    print("1. 執行 drop_tables.sql 刪除現有資料表")
    print("2. 執行 create_tables.sql 建立新的資料表結構")
    print("3. 測試新的資料表結構")
    print("4. 重新匯入資料")

