# -*- coding: utf-8 -*-
"""
LVR 資料匯入器
負責將 CSV 檔案匯入到對應的資料庫中
"""

import os
import pandas as pd
import pyodbc
import logging
from typing import Dict, List, Optional, Tuple
from tqdm import tqdm
import time

from config import DB_CONFIG, DATABASES, DATA_FOLDERS, BATCH_SIZE

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('lvr_import.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DataImporter:
    """資料匯入器類別"""
    
    def __init__(self):
        self.connection_string = self._build_connection_string()
        
    def _build_connection_string(self) -> str:
        """建立資料庫連線字串"""
        conn_str = (
            f"DRIVER={{{DB_CONFIG['driver']}}};"
            f"SERVER={DB_CONFIG['server']};"
            f"UID={DB_CONFIG['username']};"
            f"PWD={DB_CONFIG['password']};"
            f"Trusted_Connection={DB_CONFIG['trusted_connection']};"
            f"Encrypt={DB_CONFIG['encrypt']};"
        )
        return conn_str
    
    def get_database_for_file(self, filename: str) -> str:
        """根據檔案名稱判斷應該匯入到哪個資料庫"""
        if '_a.csv' in filename:
            return DATABASES['used_house']      # 中古屋買賣
        elif '_b.csv' in filename:
            return DATABASES['pre_sale']        # 預售屋買賣
        elif '_c.csv' in filename:
            return DATABASES['rental']          # 租屋
        else:
            return None
    
    def get_table_for_file(self, filename: str) -> str:
        """根據檔案名稱判斷應該匯入到哪個資料表"""
        if '_a.csv' in filename:
            return 'main_data'
        elif '_a_build.csv' in filename:
            return 'build_data'
        elif '_a_land.csv' in filename:
            return 'land_data'
        elif '_a_park.csv' in filename:
            return 'park_data'
        elif '_b.csv' in filename:
            return 'main_data'
        elif '_b_build.csv' in filename:
            return 'build_data'
        elif '_b_land.csv' in filename:
            return 'land_data'
        elif '_b_park.csv' in filename:
            return 'park_data'
        elif '_c.csv' in filename:
            return 'main_data'
        elif '_c_build.csv' in filename:
            return 'build_data'
        elif '_c_land.csv' in filename:
            return 'land_data'
        elif '_c_park.csv' in filename:
            return 'park_data'
        else:
            return 'main_data'
    
    def read_csv_file(self, file_path: str) -> Optional[pd.DataFrame]:
        """讀取 CSV 檔案"""
        try:
            # 嘗試不同的編碼
            encodings = ['utf-8', 'big5', 'cp950', 'gbk']
            
            for encoding in encodings:
                try:
                    # 跳過第二行（欄位名稱行）
                    df = pd.read_csv(file_path, encoding=encoding, skiprows=[1])
                    logger.info(f"✅ 成功讀取 {file_path} (編碼: {encoding})")
                    return df
                except UnicodeDecodeError:
                    continue
            
            logger.error(f"❌ 無法讀取 {file_path}，所有編碼都失敗")
            return None
            
        except Exception as e:
            logger.error(f"❌ 讀取 {file_path} 失敗: {str(e)}")
            return None
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """清理資料"""
        try:
            # 移除完全空白的行
            df = df.dropna(how='all')
            
            # 定義數值欄位名稱（根據資料表結構）
            numeric_columns = [
                '土地移轉總面積平方公尺', '交易筆棟數', '總樓層數', '建物移轉總面積平方公尺',
                '建物現況格局-房', '建物現況格局-廳', '建物現況格局-衛', '總價元', '單價元平方公尺',
                '車位移轉總面積平方公尺', '車位總價元', '主建物面積', '附屬建物面積', '陽台面積',
                '屋齡', '建物移轉面積平方公尺', '土地移轉面積平方公尺', '權利人持分分母', '權利人持分分子',
                '車位價格', '車位面積平方公尺', '土地面積平方公尺', '租賃筆棟數', '建物總面積平方公尺',
                '車位面積平方公尺', '車位總額元', '總額元'
            ]
            
            # 處理數值欄位
            for col in numeric_columns:
                if col in df.columns:
                    # 先轉換為字串，然後清理
                    df[col] = df[col].astype(str)
                    # 移除非數值字符（保留小數點和負號）
                    df[col] = df[col].str.replace(r'[^\d.-]', '', regex=True)
                    # 處理空字串和無效值
                    df[col] = df[col].replace(['', 'nan', 'None', 'null'], None)
                    # 轉換為數值，無法轉換的設為 None
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    # 將 NaN 轉換為 None
                    df[col] = df[col].where(pd.notnull(df[col]), None)
            
            # 處理字串欄位的空值
            string_columns = df.select_dtypes(include=['object']).columns
            for col in string_columns:
                df[col] = df[col].fillna('')
                # 清理字串中的特殊字符
                df[col] = df[col].astype(str).str.replace('\r\n', ' ').str.replace('\n', ' ').str.strip()
                # 處理 'nan' 字串
                df[col] = df[col].replace('nan', '')
            
            logger.info(f"✅ 資料清理完成，剩餘 {len(df)} 行")
            return df
            
        except Exception as e:
            logger.error(f"❌ 資料清理失敗: {str(e)}")
            return df
    
    def create_insert_sql(self, table_name: str, columns: List[str]) -> str:
        """建立 INSERT SQL 語句"""
        # 加入額外的欄位
        all_columns = columns + ['source_file', 'quarter']
        placeholders = ', '.join(['?' for _ in all_columns])
        
        sql = f"""
        INSERT INTO [{table_name}] 
        ([{'], ['.join(all_columns)}])
        VALUES ({placeholders})
        """
        return sql
    
    def insert_data_batch(self, database_name: str, table_name: str, df: pd.DataFrame, 
                         source_file: str, quarter: str) -> bool:
        """批次插入資料"""
        try:
            # 連接到指定資料庫
            conn_str = self.connection_string + f"Database={database_name};"
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            
            # 準備資料
            columns = list(df.columns)
            insert_sql = self.create_insert_sql(table_name, columns)
            
            # 批次處理
            total_rows = len(df)
            success_count = 0
            
            for i in range(0, total_rows, BATCH_SIZE):
                batch_df = df.iloc[i:i+BATCH_SIZE]
                batch_data = []
                
                for _, row in batch_df.iterrows():
                    # 準備資料行，處理資料類型
                    row_data = []
                    for value in row.values:
                        if pd.isna(value) or value is None:
                            row_data.append(None)
                        elif isinstance(value, (int, float)):
                            # 確保數值在合理範圍內
                            if isinstance(value, float) and (value > 1e15 or value < -1e15):
                                row_data.append(None)
                            else:
                                row_data.append(value)
                        else:
                            # 字串資料
                            str_value = str(value).strip()
                            if str_value in ['', 'nan', 'None', 'null']:
                                row_data.append(None)
                            else:
                                row_data.append(str_value)
                    
                    # 加入額外欄位
                    row_data.extend([source_file, quarter])
                    batch_data.append(row_data)
                
                # 執行批次插入
                cursor.executemany(insert_sql, batch_data)
                success_count += len(batch_data)
                
                # 顯示進度
                progress = min(i + BATCH_SIZE, total_rows)
                logger.info(f"📊 進度: {progress}/{total_rows} 行已處理")
            
            conn.commit()
            conn.close()
            
            logger.info(f"✅ 成功插入 {success_count} 行到 {database_name}.{table_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ 插入資料到 {database_name}.{table_name} 失敗: {str(e)}")
            return False
    
    def import_single_folder(self, folder_path: str) -> Dict[str, int]:
        """匯入單一資料夾中的所有 CSV 檔案"""
        logger.info(f"🚀 開始匯入資料夾: {folder_path}")
        
        # 讀取 manifest.csv
        manifest_path = os.path.join(folder_path, 'manifest.csv')
        if not os.path.exists(manifest_path):
            logger.error(f"❌ manifest.csv 不存在: {manifest_path}")
            return {}
        
        try:
            manifest_df = pd.read_csv(manifest_path, encoding='utf-8')
            logger.info(f"📋 找到 {len(manifest_df)} 個檔案記錄")
        except Exception as e:
            logger.error(f"❌ 讀取 manifest.csv 失敗: {str(e)}")
            return {}
        
        # 統計結果
        import_stats = {
            'total_files': 0,
            'success_files': 0,
            'failed_files': 0,
            'total_rows': 0
        }
        
        # 處理每個 CSV 檔案
        for _, row in manifest_df.iterrows():
            filename = row['name']
            if not filename.endswith('.csv'):
                continue
            
            file_path = os.path.join(folder_path, filename)
            if not os.path.exists(file_path):
                logger.warning(f"⚠️ 檔案不存在: {file_path}")
                continue
            
            import_stats['total_files'] += 1
            
            try:
                # 判斷目標資料庫和資料表
                database_name = self.get_database_for_file(filename)
                table_name = self.get_table_for_file(filename)
                
                if not database_name:
                    logger.warning(f"⚠️ 無法判斷檔案類型: {filename}")
                    import_stats['failed_files'] += 1
                    continue
                
                logger.info(f"📁 處理檔案: {filename} -> {database_name}.{table_name}")
                
                # 讀取 CSV 檔案
                df = self.read_csv_file(file_path)
                if df is None or len(df) == 0:
                    logger.warning(f"⚠️ 檔案為空或讀取失敗: {filename}")
                    import_stats['failed_files'] += 1
                    continue
                
                # 清理資料
                df = self.clean_data(df)
                
                # 插入資料
                quarter = folder_path  # 使用資料夾名稱作為季度標識
                if self.insert_data_batch(database_name, table_name, df, filename, quarter):
                    import_stats['success_files'] += 1
                    import_stats['total_rows'] += len(df)
                    logger.info(f"✅ 成功匯入 {filename}: {len(df)} 行")
                else:
                    import_stats['failed_files'] += 1
                    logger.error(f"❌ 匯入失敗: {filename}")
                
            except Exception as e:
                logger.error(f"❌ 處理檔案 {filename} 時發生錯誤: {str(e)}")
                import_stats['failed_files'] += 1
        
        # 顯示匯入統計
        logger.info(f"📊 資料夾 {folder_path} 匯入完成:")
        logger.info(f"   總檔案數: {import_stats['total_files']}")
        logger.info(f"   成功檔案數: {import_stats['success_files']}")
        logger.info(f"   失敗檔案數: {import_stats['failed_files']}")
        logger.info(f"   總資料行數: {import_stats['total_rows']}")
        
        return import_stats
    
    def import_all_folders(self) -> Dict[str, Dict[str, int]]:
        """匯入所有資料夾"""
        logger.info("🚀 開始匯入所有資料夾")
        
        all_stats = {}
        
        for folder in DATA_FOLDERS:
            if os.path.exists(folder):
                logger.info(f"📁 處理資料夾: {folder}")
                stats = self.import_single_folder(folder)
                all_stats[folder] = stats
                
                # 暫停一下，避免過度消耗資源
                time.sleep(1)
            else:
                logger.warning(f"⚠️ 資料夾不存在: {folder}")
        
        # 顯示總體統計
        total_files = sum(stats['total_files'] for stats in all_stats.values())
        total_success = sum(stats['success_files'] for stats in all_stats.values())
        total_failed = sum(stats['failed_files'] for stats in all_stats.values())
        total_rows = sum(stats['total_rows'] for stats in all_stats.values())
        
        logger.info("🎉 所有資料夾匯入完成！")
        logger.info(f"📊 總體統計:")
        logger.info(f"   總檔案數: {total_files}")
        logger.info(f"   成功檔案數: {total_success}")
        logger.info(f"   失敗檔案數: {total_failed}")
        logger.info(f"   總資料行數: {total_rows}")
        
        return all_stats


def main():
    """主函數"""
    print("🚀 LVR 資料匯入工具")
    print("=" * 60)
    
    # 建立資料匯入器
    importer = DataImporter()
    
    # 詢問使用者要匯入哪個資料夾
    print("\n📁 可用的資料夾:")
    for i, folder in enumerate(DATA_FOLDERS, 1):
        if os.path.exists(folder):
            print(f"   {i}. {folder}")
    
    print("\n請選擇:")
    print("1. 匯入單一資料夾")
    print("2. 匯入所有資料夾")
    print("3. 退出")
    
    choice = input("\n請輸入選擇 (1-3): ").strip()
    
    if choice == '1':
        # 匯入單一資料夾
        print("\n請選擇要匯入的資料夾:")
        for i, folder in enumerate(DATA_FOLDERS, 1):
            if os.path.exists(folder):
                print(f"   {i}. {folder}")
        
        folder_choice = input(f"\n請輸入選擇 (1-{len(DATA_FOLDERS)}): ").strip()
        try:
            folder_index = int(folder_choice) - 1
            if 0 <= folder_index < len(DATA_FOLDERS):
                folder = DATA_FOLDERS[folder_index]
                if os.path.exists(folder):
                    print(f"\n🚀 開始匯入資料夾: {folder}")
                    stats = importer.import_single_folder(folder)
                    print(f"\n✅ 匯入完成！")
                    print(f"📊 統計: {stats}")
                else:
                    print(f"❌ 資料夾不存在: {folder}")
            else:
                print("❌ 無效的選擇")
        except ValueError:
            print("❌ 請輸入有效的數字")
    
    elif choice == '2':
        # 匯入所有資料夾
        print("\n🚀 開始匯入所有資料夾...")
        all_stats = importer.import_all_folders()
        print(f"\n✅ 所有資料夾匯入完成！")
        print(f"📊 總體統計:")
        for folder, stats in all_stats.items():
            print(f"   {folder}: {stats['success_files']}/{stats['total_files']} 檔案成功")
    
    elif choice == '3':
        print("👋 再見！")
    
    else:
        print("❌ 無效的選擇")


if __name__ == "__main__":
    main()
