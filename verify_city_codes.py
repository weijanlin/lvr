# -*- coding: utf-8 -*-
"""
驗證縣市代碼功能
檢查資料庫中的縣市代碼和縣市名稱是否正確
"""

import pyodbc
import logging
from config import DB_CONFIG, DATABASES

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('verify_city_codes.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def verify_city_codes():
    """驗證縣市代碼功能"""
    print("🔍 驗證縣市代碼功能")
    print("=" * 80)
    
    # 連接到預售屋資料庫
    conn_str = (
        f"DRIVER={{{DB_CONFIG['driver']}}};"
        f"SERVER={DB_CONFIG['server']};"
        f"UID={DB_CONFIG['username']};"
        f"PWD={DB_CONFIG['password']};"
        f"Trusted_Connection={DB_CONFIG['trusted_connection']};"
        f"Encrypt={DB_CONFIG['encrypt']};"
        f"Database={DATABASES['pre_sale']};"
    )
    
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        # 檢查縣市代碼和縣市名稱
        print("\n📊 預售屋資料庫中的縣市代碼:")
        print("-" * 60)
        
        cursor.execute("""
            SELECT 縣市代碼, 縣市名稱, COUNT(*) as 記錄數
            FROM presale_data 
            GROUP BY 縣市代碼, 縣市名稱
            ORDER BY 縣市代碼
        """)
        
        results = cursor.fetchall()
        total_records = 0
        
        for row in results:
            city_code, city_name, count = row
            print(f"  {city_code} ({city_name}): {count} 筆記錄")
            total_records += count
        
        print(f"\n總計: {total_records} 筆記錄")
        
        # 檢查資料表結構
        print(f"\n📋 資料表結構:")
        print("-" * 60)
        
        cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'presale_data' 
            AND COLUMN_NAME IN ('縣市代碼', '縣市名稱', '鄉鎮市區')
            ORDER BY ORDINAL_POSITION
        """)
        
        columns = cursor.fetchall()
        for col in columns:
            col_name, data_type, max_length = col
            if max_length:
                print(f"  {col_name}: {data_type}({max_length})")
            else:
                print(f"  {col_name}: {data_type}")
        
        # 檢查樣本資料
        print(f"\n📄 樣本資料:")
        print("-" * 60)
        
        cursor.execute("""
            SELECT TOP 5 縣市代碼, 縣市名稱, 鄉鎮市區, 交易標的, source_file
            FROM presale_data 
            ORDER BY id
        """)
        
        samples = cursor.fetchall()
        for sample in samples:
            city_code, city_name, district, transaction_type, source_file = sample
            print(f"  {city_code} ({city_name}) - {district} - {transaction_type} - {source_file}")
        
        conn.close()
        
        print(f"\n✅ 縣市代碼驗證完成")
        logger.info("✅ 縣市代碼驗證完成")
        
    except Exception as e:
        print(f"❌ 驗證失敗: {str(e)}")
        logger.error(f"❌ 驗證失敗: {str(e)}")

def verify_all_databases():
    """驗證所有資料庫的縣市代碼"""
    print("\n🔍 驗證所有資料庫的縣市代碼")
    print("=" * 80)
    
    database_mapping = {
        '中古屋': (DATABASES['used_house'], 'main_data'),
        '預售屋': (DATABASES['pre_sale'], 'presale_data'),
        '租屋': (DATABASES['rental'], 'rental_data')
    }
    
    for db_type, (db_name, table_name) in database_mapping.items():
        print(f"\n📊 {db_type} 資料庫 ({db_name}):")
        print("-" * 60)
        
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
            
            # 檢查記錄數
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_count = cursor.fetchone()[0]
            print(f"  總記錄數: {total_count}")
            
            if total_count > 0:
                # 檢查縣市代碼分佈
                cursor.execute(f"""
                    SELECT 縣市代碼, 縣市名稱, COUNT(*) as 記錄數
                    FROM {table_name} 
                    GROUP BY 縣市代碼, 縣市名稱
                    ORDER BY 記錄數 DESC
                """)
                
                results = cursor.fetchall()
                print(f"  縣市分佈:")
                for row in results:
                    city_code, city_name, count = row
                    print(f"    {city_code} ({city_name}): {count} 筆")
            
            conn.close()
            
        except Exception as e:
            print(f"  ❌ 檢查失敗: {str(e)}")

if __name__ == "__main__":
    verify_city_codes()
    verify_all_databases()

