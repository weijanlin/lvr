# -*- coding: utf-8 -*-
"""
測試子檔案匯入
驗證_build、_land、_park檔案的匯入功能
"""

from enhanced_data_importer import EnhancedDataImporter
import logging
import os

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test_subfile_import.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def test_subfile_import():
    """測試子檔案匯入"""
    print("🧪 測試子檔案匯入功能")
    print("=" * 80)
    
    importer = EnhancedDataImporter()
    
    # 測試不同類型的子檔案
    test_files = [
        # 中古屋子檔案
        ("113Q1/a_lvr_land_a_build.csv", "中古屋建物資料", "臺北市"),
        ("113Q1/a_lvr_land_a_land.csv", "中古屋土地資料", "臺北市"),
        ("113Q1/a_lvr_land_a_park.csv", "中古屋停車場資料", "臺北市"),
        
        # 預售屋子檔案
        ("113Q1/a_lvr_land_b_land.csv", "預售屋土地資料", "臺北市"),
        ("113Q1/a_lvr_land_b_park.csv", "預售屋停車場資料", "臺北市"),
        
        # 租屋子檔案
        ("113Q1/a_lvr_land_c_build.csv", "租屋建物資料", "臺北市"),
        ("113Q1/a_lvr_land_c_land.csv", "租屋土地資料", "臺北市"),
        ("113Q1/a_lvr_land_c_park.csv", "租屋停車場資料", "臺北市"),
    ]
    
    success_count = 0
    total_count = len(test_files)
    
    print(f"\n📋 測試 {total_count} 個子檔案:")
    print("-" * 80)
    
    for file_path, description, expected_city in test_files:
        print(f"\n📄 測試檔案: {os.path.basename(file_path)}")
        print(f"   描述: {description}")
        print(f"   預期縣市: {expected_city}")
        print("-" * 60)
        
        if os.path.exists(file_path):
            try:
                success = importer.import_single_file(file_path, "113Q1")
                if success:
                    print(f"✅ {os.path.basename(file_path)} 匯入成功")
                    success_count += 1
                else:
                    print(f"❌ {os.path.basename(file_path)} 匯入失敗")
            except Exception as e:
                print(f"❌ {os.path.basename(file_path)} 匯入錯誤: {str(e)}")
                logger.error(f"❌ {os.path.basename(file_path)} 匯入錯誤: {str(e)}")
        else:
            print(f"❌ 檔案不存在: {file_path}")
    
    print(f"\n📊 測試結果:")
    print(f"   成功: {success_count}/{total_count}")
    print(f"   失敗: {total_count - success_count}/{total_count}")
    
    if success_count == total_count:
        print("🎉 所有子檔案匯入測試成功！")
        logger.info("🎉 所有子檔案匯入測試成功！")
        return True
    else:
        print("⚠️ 部分子檔案匯入測試失敗")
        logger.warning("⚠️ 部分子檔案匯入測試失敗")
        return False

def verify_subfile_data():
    """驗證子檔案匯入的資料"""
    print(f"\n🔍 驗證子檔案匯入的資料")
    print("=" * 80)
    
    try:
        from config import DB_CONFIG, DATABASES
        import pyodbc
        
        # 檢查各資料庫的子檔案資料
        databases = [
            ("中古屋", DATABASES['used_house'], ['build_data', 'land_data', 'park_data']),
            ("預售屋", DATABASES['pre_sale'], ['build_data', 'land_data', 'park_data']),
            ("租屋", DATABASES['rental'], ['build_data', 'land_data', 'park_data'])
        ]
        
        for db_type, db_name, tables in databases:
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
                
                for table in tables:
                    try:
                        # 檢查記錄數
                        cursor.execute(f"SELECT COUNT(*) FROM {table}")
                        count = cursor.fetchone()[0]
                        print(f"  {table}: {count} 筆記錄")
                        
                        if count > 0:
                            # 檢查縣市分佈
                            cursor.execute(f"""
                                SELECT 縣市代碼, 縣市名稱, COUNT(*) as 記錄數
                                FROM {table} 
                                GROUP BY 縣市代碼, 縣市名稱
                                ORDER BY 記錄數 DESC
                            """)
                            
                            results = cursor.fetchall()
                            for row in results:
                                city_code, city_name, record_count = row
                                print(f"    {city_code} ({city_name}): {record_count} 筆")
                            
                            # 顯示樣本資料
                            cursor.execute(f"""
                                SELECT TOP 3 縣市代碼, 縣市名稱, 編號, source_file
                                FROM {table} 
                                ORDER BY id
                            """)
                            
                            samples = cursor.fetchall()
                            print(f"    樣本資料:")
                            for sample in samples:
                                city_code, city_name, record_id, source_file = sample
                                print(f"      {city_code} ({city_name}) - {record_id} - {source_file}")
                        
                    except Exception as e:
                        print(f"  ❌ 檢查 {table} 失敗: {str(e)}")
                
                conn.close()
                
            except Exception as e:
                print(f"  ❌ 連接 {db_name} 失敗: {str(e)}")
        
    except Exception as e:
        print(f"❌ 驗證失敗: {str(e)}")
        logger.error(f"❌ 驗證失敗: {str(e)}")

def test_different_cities():
    """測試不同縣市的子檔案匯入"""
    print(f"\n🌍 測試不同縣市的子檔案匯入")
    print("=" * 80)
    
    importer = EnhancedDataImporter()
    
    # 測試不同縣市的檔案
    test_cities = [
        ("113Q1/b_lvr_land_a_build.csv", "臺中市", "中古屋建物"),
        ("113Q1/f_lvr_land_b_land.csv", "新北市", "預售屋土地"),
        ("113Q1/h_lvr_land_c_park.csv", "桃園市", "租屋停車場"),
    ]
    
    success_count = 0
    total_count = len(test_cities)
    
    for file_path, expected_city, file_type in test_cities:
        print(f"\n📄 測試檔案: {os.path.basename(file_path)}")
        print(f"   預期縣市: {expected_city}")
        print(f"   檔案類型: {file_type}")
        print("-" * 60)
        
        if os.path.exists(file_path):
            try:
                success = importer.import_single_file(file_path, "113Q1")
                if success:
                    print(f"✅ {os.path.basename(file_path)} 匯入成功")
                    success_count += 1
                else:
                    print(f"❌ {os.path.basename(file_path)} 匯入失敗")
            except Exception as e:
                print(f"❌ {os.path.basename(file_path)} 匯入錯誤: {str(e)}")
                logger.error(f"❌ {os.path.basename(file_path)} 匯入錯誤: {str(e)}")
        else:
            print(f"❌ 檔案不存在: {file_path}")
    
    print(f"\n📊 不同縣市測試結果:")
    print(f"   成功: {success_count}/{total_count}")
    print(f"   失敗: {total_count - success_count}/{total_count}")

if __name__ == "__main__":
    # 執行子檔案匯入測試
    success = test_subfile_import()
    
    if success:
        # 驗證匯入的資料
        verify_subfile_data()
        
        # 測試不同縣市
        test_different_cities()
    else:
        print("❌ 子檔案匯入測試失敗，跳過驗證")




