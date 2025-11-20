# -*- coding: utf-8 -*-
"""
測試租屋資料匯入
驗證_c.csv檔案匯入功能
"""

from improved_data_importer import ImprovedDataImporter
import logging

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test_rental_import.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def test_rental_import():
    """測試租屋資料匯入"""
    print("🧪 測試租屋資料匯入")
    print("=" * 80)
    
    importer = ImprovedDataImporter()
    
    # 測試租屋主要檔案
    test_files = [
        "113Q1/a_lvr_land_c.csv",  # 租屋主要檔案
        "113Q1/a_lvr_land_c_build.csv",  # 租屋建物檔案
        "113Q1/a_lvr_land_c_land.csv",   # 租屋土地檔案
        "113Q1/a_lvr_land_c_park.csv"    # 租屋停車場檔案
    ]
    
    success_count = 0
    total_count = len(test_files)
    
    for test_file in test_files:
        print(f"\n📄 測試檔案: {test_file}")
        print("-" * 60)
        
        try:
            success = importer.import_single_file(test_file, "113Q1")
            if success:
                print(f"✅ {test_file} 匯入成功")
                success_count += 1
            else:
                print(f"❌ {test_file} 匯入失敗")
        except Exception as e:
            print(f"❌ {test_file} 匯入錯誤: {str(e)}")
            logger.error(f"❌ {test_file} 匯入錯誤: {str(e)}")
    
    print(f"\n📊 測試結果:")
    print(f"   成功: {success_count}/{total_count}")
    print(f"   失敗: {total_count - success_count}/{total_count}")
    
    if success_count == total_count:
        print("🎉 所有租屋檔案匯入測試成功！")
        logger.info("🎉 所有租屋檔案匯入測試成功！")
        return True
    else:
        print("⚠️ 部分租屋檔案匯入測試失敗")
        logger.warning("⚠️ 部分租屋檔案匯入測試失敗")
        return False

def test_rental_data_verification():
    """驗證租屋資料匯入結果"""
    print(f"\n🔍 驗證租屋資料匯入結果")
    print("=" * 80)
    
    try:
        from config import DB_CONFIG
        import pyodbc
        
        # 連接到租屋資料庫
        conn_str = (
            f"DRIVER={{{DB_CONFIG['driver']}}};"
            f"SERVER={DB_CONFIG['server']};"
            f"UID={DB_CONFIG['username']};"
            f"PWD={DB_CONFIG['password']};"
            f"Trusted_Connection={DB_CONFIG['trusted_connection']};"
            f"Encrypt={DB_CONFIG['encrypt']};"
            f"Database=LVR_Rental;"
        )
        
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        # 檢查各資料表的資料筆數
        tables = ['rental_data', 'build_data', 'land_data', 'park_data']
        
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"📊 {table}: {count} 筆資料")
            except Exception as e:
                print(f"❌ 檢查 {table} 失敗: {str(e)}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ 驗證失敗: {str(e)}")
        logger.error(f"❌ 驗證失敗: {str(e)}")

if __name__ == "__main__":
    # 執行租屋資料匯入測試
    success = test_rental_import()
    
    if success:
        # 驗證匯入結果
        test_rental_data_verification()
    else:
        print("❌ 租屋資料匯入測試失敗，跳過驗證")

