# -*- coding: utf-8 -*-
"""
資料表重建腳本
刪除現有資料表並建立新的資料表結構
"""

import pyodbc
import logging
from typing import Dict, List
from config import DB_CONFIG, DATABASES

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('rebuild_tables.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_table_structures() -> Dict[str, Dict[str, List[str]]]:
    """取得所有資料表的結構定義"""
    
    # 中古屋資料表結構
    used_house_tables = {
        'main_data': [
            '縣市代碼 NVARCHAR(10)',
            '縣市名稱 NVARCHAR(50)',
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
            '主要用途 NVARCHAR(1000)',
            '主要建材 NVARCHAR(200)',
            '建築完成年月 NVARCHAR(20)',
            '建物移轉總面積平方公尺 DECIMAL(15,2)',
            '[建物現況格局-房] INT',
            '[建物現況格局-廳] INT',
            '[建物現況格局-衛] INT',
            '[建物現況格局-隔間] NVARCHAR(50)',
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
            '主要用途 NVARCHAR(1000)',
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
    
    # 預售屋資料表結構
    presale_tables = {
        'presale_data': [
            '縣市代碼 NVARCHAR(10)',
            '縣市名稱 NVARCHAR(50)',
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
            '主要用途 NVARCHAR(1000)',
            '主要建材 NVARCHAR(200)',
            '建築完成年月 NVARCHAR(20)',
            '建物移轉總面積平方公尺 DECIMAL(15,2)',
            '[建物現況格局-房] INT',
            '[建物現況格局-廳] INT',
            '[建物現況格局-衛] INT',
            '[建物現況格局-隔間] NVARCHAR(50)',
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
            '主要用途 NVARCHAR(1000)',
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
    
    # 租屋資料表結構
    rental_tables = {
        'rental_data': [
            '縣市代碼 NVARCHAR(10)',
            '縣市名稱 NVARCHAR(50)',
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
            '主要用途 NVARCHAR(1000)',
            '主要建材 NVARCHAR(200)',
            '建築完成年月 NVARCHAR(20)',
            '建物總面積平方公尺 DECIMAL(15,2)',
            '[建物現況格局-房] INT',
            '[建物現況格局-廳] INT',
            '[建物現況格局-衛] INT',
            '[建物現況格局-隔間] NVARCHAR(50)',
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
            '主要用途 NVARCHAR(1000)',
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

def drop_table(cursor, database_name: str, table_name: str) -> bool:
    """刪除資料表"""
    try:
        # 檢查資料表是否存在
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM {database_name}.INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = '{table_name}'
        """)
        
        if cursor.fetchone()[0] > 0:
            cursor.execute(f"DROP TABLE [{database_name}].[dbo].[{table_name}]")
            logger.info(f"✅ 已刪除資料表: {database_name}.{table_name}")
            return True
        else:
            logger.info(f"ℹ️ 資料表不存在: {database_name}.{table_name}")
            return True
            
    except Exception as e:
        logger.error(f"❌ 刪除資料表失敗 {database_name}.{table_name}: {str(e)}")
        return False

def create_table(cursor, database_name: str, table_name: str, columns: List[str]) -> bool:
    """建立資料表"""
    try:
        sql = f"CREATE TABLE [{database_name}].[dbo].[{table_name}] (\n"
        sql += "    id INT IDENTITY(1,1) PRIMARY KEY,\n"
        
        for column in columns:
            sql += f"    {column},\n"
        
        sql = sql.rstrip(",\n") + "\n"
        sql += ");"
        
        cursor.execute(sql)
        logger.info(f"✅ 已建立資料表: {database_name}.{table_name}")
        return True
        
    except Exception as e:
        logger.error(f"❌ 建立資料表失敗 {database_name}.{table_name}: {str(e)}")
        return False

def rebuild_database_tables(database_name: str, tables: Dict[str, List[str]]) -> bool:
    """重建指定資料庫的所有資料表"""
    try:
        # 連接到指定資料庫
        conn_str = (
            f"DRIVER={{{DB_CONFIG['driver']}}};"
            f"SERVER={DB_CONFIG['server']};"
            f"UID={DB_CONFIG['username']};"
            f"PWD={DB_CONFIG['password']};"
            f"Trusted_Connection={DB_CONFIG['trusted_connection']};"
            f"Encrypt={DB_CONFIG['encrypt']};"
            f"Database={database_name};"
        )
        
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        logger.info(f"🔄 開始重建資料庫: {database_name}")
        
        # 刪除現有資料表
        logger.info("🗑️ 刪除現有資料表...")
        for table_name in tables.keys():
            drop_table(cursor, database_name, table_name)
        
        # 建立新資料表
        logger.info("🏗️ 建立新資料表...")
        for table_name, columns in tables.items():
            create_table(cursor, database_name, table_name, columns)
        
        conn.commit()
        conn.close()
        
        logger.info(f"✅ 資料庫重建完成: {database_name}")
        return True
        
    except Exception as e:
        logger.error(f"❌ 重建資料庫失敗 {database_name}: {str(e)}")
        return False

def rebuild_all_tables():
    """重建所有資料庫的資料表"""
    logger.info("🚀 開始重建所有資料表...")
    print("🚀 開始重建所有資料表...")
    print("=" * 80)
    
    structures = get_table_structures()
    database_mapping = {
        'used_house': DATABASES['used_house'],
        'presale': DATABASES['pre_sale'],
        'rental': DATABASES['rental']
    }
    
    success_count = 0
    total_count = len(database_mapping)
    
    for db_type, db_name in database_mapping.items():
        print(f"\n📊 處理資料庫: {db_name}")
        print("-" * 40)
        
        if rebuild_database_tables(db_name, structures[db_type]):
            success_count += 1
            print(f"✅ {db_name} 重建成功")
        else:
            print(f"❌ {db_name} 重建失敗")
    
    print(f"\n📋 重建結果:")
    print(f"   成功: {success_count}/{total_count}")
    print(f"   失敗: {total_count - success_count}/{total_count}")
    
    if success_count == total_count:
        print("🎉 所有資料表重建完成！")
        logger.info("🎉 所有資料表重建完成！")
        return True
    else:
        print("⚠️ 部分資料表重建失敗，請檢查日誌")
        logger.warning("⚠️ 部分資料表重建失敗")
        return False

if __name__ == "__main__":
    print("🏗️ LVR 資料表重建工具")
    print("=" * 80)
    print("⚠️ 警告：此操作將刪除所有現有資料！")
    print("=" * 80)
    
    # 確認操作
    confirm = input("確定要重建所有資料表嗎？(y/N): ").strip().lower()
    
    if confirm in ['y', 'yes']:
        rebuild_all_tables()
    else:
        print("❌ 操作已取消")
        logger.info("❌ 操作已取消")
