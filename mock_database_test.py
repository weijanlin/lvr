# -*- coding: utf-8 -*-
"""
模擬資料庫測試腳本
用於測試程式邏輯而不需要實際的 SQL Server 連線
"""

import os
import pandas as pd
from config import DATA_FOLDERS

def test_schema_reading():
    """測試 schema 檔案讀取功能"""
    print("🔍 測試 Schema 檔案讀取...")
    print("=" * 50)
    
    # 測試第一個資料夾的 schema 檔案
    test_folder = DATA_FOLDERS[0]  # 113Q1
    
    schema_files = [
        'schema-main.csv',
        'schema-build.csv',
        'schema-land.csv',
        'schema-park.csv'
    ]
    
    for schema_file in schema_files:
        schema_path = os.path.join(test_folder, schema_file)
        if os.path.exists(schema_path):
            try:
                df = pd.read_csv(schema_path, encoding='utf-8')
                print(f"✅ {schema_file}: {len(df)} 個欄位")
                
                # 顯示前幾個欄位
                if len(df) > 0:
                    print(f"   前5個欄位: {list(df['name'].head())}")
                
            except Exception as e:
                print(f"❌ {schema_file}: 讀取失敗 - {str(e)}")
        else:
            print(f"⚠️ {schema_file}: 檔案不存在")
    
    print()

def test_manifest_reading():
    """測試 manifest 檔案讀取功能"""
    print("📋 測試 Manifest 檔案讀取...")
    print("=" * 50)
    
    test_folder = DATA_FOLDERS[0]  # 113Q1
    manifest_path = os.path.join(test_folder, 'manifest.csv')
    
    if os.path.exists(manifest_path):
        try:
            df = pd.read_csv(manifest_path, encoding='utf-8')
            print(f"✅ manifest.csv: {len(df)} 個檔案記錄")
            
            # 分析檔案類型
            file_types = {}
            for _, row in df.iterrows():
                filename = row['name']
                if '_a.csv' in filename:
                    file_type = '中古屋買賣'
                elif '_b.csv' in filename:
                    file_type = '預售屋買賣'
                elif '_c.csv' in filename:
                    file_type = '租屋'
                else:
                    file_type = '其他'
                
                if file_type not in file_types:
                    file_types[file_type] = 0
                file_types[file_type] += 1
            
            print("\n📊 檔案類型統計:")
            for file_type, count in file_types.items():
                print(f"   {file_type}: {count} 個檔案")
            
            # 顯示前幾個檔案
            print(f"\n📁 前10個檔案:")
            for i, filename in enumerate(df['name'].head(10), 1):
                print(f"   {i:2d}. {filename}")
            
        except Exception as e:
            print(f"❌ manifest.csv 讀取失敗: {str(e)}")
    else:
        print(f"⚠️ manifest.csv 檔案不存在")
    
    print()

def test_csv_file_reading():
    """測試 CSV 檔案讀取功能"""
    print("📄 測試 CSV 檔案讀取...")
    print("=" * 50)
    
    test_folder = DATA_FOLDERS[0]  # 113Q1
    
    # 尋找一個小的 CSV 檔案進行測試
    test_files = [
        'schema-time.csv',
        'schema-park.csv',
        'z_lvr_land_a_land.csv'
    ]
    
    for test_file in test_files:
        file_path = os.path.join(test_folder, test_file)
        if os.path.exists(file_path):
            try:
                # 檢查檔案大小
                file_size = os.path.getsize(file_path)
                print(f"📁 {test_file}: {file_size} bytes")
                
                # 讀取 CSV 檔案
                df = pd.read_csv(file_path, encoding='utf-8')
                print(f"   📊 資料: {len(df)} 行 x {len(df.columns)} 列")
                
                # 顯示欄位名稱
                if len(df.columns) > 0:
                    print(f"   🏷️ 欄位: {list(df.columns)}")
                
                # 顯示前幾行資料
                if len(df) > 0:
                    print(f"   📝 前3行資料:")
                    for i, row in df.head(3).iterrows():
                        print(f"      第{i+1}行: {dict(row)}")
                
            except Exception as e:
                print(f"❌ {test_file}: 讀取失敗 - {str(e)}")
        else:
            print(f"⚠️ {test_file}: 檔案不存在")
        
        print()

def test_data_analysis():
    """測試資料分析功能"""
    print("📈 測試資料分析...")
    print("=" * 50)
    
    test_folder = DATA_FOLDERS[0]  # 113Q1
    manifest_path = os.path.join(test_folder, 'manifest.csv')
    
    if os.path.exists(manifest_path):
        try:
            df = pd.read_csv(manifest_path, encoding='utf-8')
            
            # 分析城市分布
            cities = {}
            for filename in df['name']:
                if filename.startswith('a_'):
                    city = '臺北市'
                elif filename.startswith('b_'):
                    city = '臺中市'
                elif filename.startswith('c_'):
                    city = '基隆市'
                elif filename.startswith('d_'):
                    city = '臺南市'
                elif filename.startswith('e_'):
                    city = '高雄市'
                elif filename.startswith('f_'):
                    city = '新北市'
                elif filename.startswith('g_'):
                    city = '宜蘭縣'
                elif filename.startswith('h_'):
                    city = '桃園市'
                elif filename.startswith('i_'):
                    city = '嘉義市'
                else:
                    city = '其他'
                
                if city not in cities:
                    cities[city] = 0
                cities[city] += 1
            
            print("🏙️ 城市分布:")
            for city, count in cities.items():
                print(f"   {city}: {count} 個檔案")
            
            # 分析資料表類型
            table_types = {}
            for filename in df['name']:
                if '_a.csv' in filename:
                    table_type = 'main_data'
                elif '_a_build.csv' in filename:
                    table_type = 'build_data'
                elif '_a_land.csv' in filename:
                    table_type = 'land_data'
                elif '_a_park.csv' in filename:
                    table_type = 'park_data'
                else:
                    table_type = 'other'
                
                if table_type not in table_types:
                    table_types[table_type] = 0
                table_types[table_type] += 1
            
            print("\n📊 資料表類型分布:")
            for table_type, count in table_types.items():
                print(f"   {table_type}: {count} 個檔案")
            
        except Exception as e:
            print(f"❌ 資料分析失敗: {str(e)}")
    
    print()

if __name__ == "__main__":
    print("🚀 LVR 資料檔案測試工具")
    print("=" * 60)
    
    test_schema_reading()
    test_manifest_reading()
    test_csv_file_reading()
    test_data_analysis()
    
    print("=" * 60)
    print("✅ 所有測試完成！")
    print("\n💡 下一步:")
    print("1. 確認 SQL Server 服務正在執行")
    print("2. 執行 python test_connection.py 測試資料庫連線")
    print("3. 執行 python database_manager.py 建立資料庫")
