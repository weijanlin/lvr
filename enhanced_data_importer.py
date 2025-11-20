# -*- coding: utf-8 -*-
"""
增強版資料匯入器
整合縣市代碼對應功能，支援所有檔案類型的匯入
"""

import pandas as pd
import pyodbc
import logging
import os
import glob
from typing import Dict, List, Optional, Tuple
from config import DB_CONFIG, BATCH_SIZE
from file_type_mapping import FileTypeMapping, DataType, FileType
from city_code_mapping import CityCodeMapping

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('enhanced_import.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class EnhancedDataImporter:
    """增強版資料匯入器（含縣市代碼）"""
    
    def __init__(self):
        self.connection_string = self._build_connection_string()
        self.file_mapping = FileTypeMapping()
        self.city_mapping = CityCodeMapping()
        
    def _build_connection_string(self) -> str:
        """建立連線字串"""
        return (
            f"DRIVER={{{DB_CONFIG['driver']}}};"
            f"SERVER={DB_CONFIG['server']};"
            f"UID={DB_CONFIG['username']};"
            f"PWD={DB_CONFIG['password']};"
            f"Trusted_Connection={DB_CONFIG['trusted_connection']};"
            f"Encrypt={DB_CONFIG['encrypt']};"
        )
    
    def read_csv_file(self, file_path: str) -> Optional[pd.DataFrame]:
        """讀取 CSV 檔案"""
        try:
            # 嘗試不同的編碼
            encodings = ['utf-8', 'big5', 'cp950', 'gbk']
            
            for encoding in encodings:
                try:
                    # 讀取 CSV，跳過第二行 (欄位名稱)
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
    
    def clean_data(self, df: pd.DataFrame, file_type: FileType) -> pd.DataFrame:
        """清理資料"""
        try:
            # 移除完全空白的行
            df = df.dropna(how='all')
            
            # 根據檔案類型定義數值欄位
            numeric_columns = self._get_numeric_columns(file_type)
            
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
    
    def _get_numeric_columns(self, file_type: FileType) -> List[str]:
        """根據檔案類型取得數值欄位列表"""
        if file_type == FileType.MAIN:
            return [
                '土地移轉總面積平方公尺', '交易筆棟數', '總樓層數', '建物移轉總面積平方公尺',
                '[建物現況格局-房]', '[建物現況格局-廳]', '[建物現況格局-衛]', '總價元', '單價元平方公尺',
                '車位移轉總面積平方公尺', '車位總價元', '主建物面積', '附屬建物面積', '陽台面積',
                '土地面積平方公尺', '建物總面積平方公尺', '車位面積平方公尺', '車位總額元', '總額元',
                '租賃筆棟數', '屋齡', '建物移轉面積平方公尺', '權利人持分分母', '權利人持分分子',
                '車位價格', '車位面積平方公尺'
            ]
        elif file_type == FileType.BUILD:
            return [
                '屋齡', '建物移轉面積平方公尺', '總層數'
            ]
        elif file_type == FileType.LAND:
            return [
                '土地移轉面積平方公尺', '權利人持分分母', '權利人持分分子'
            ]
        elif file_type == FileType.PARK:
            return [
                '車位價格', '車位面積平方公尺'
            ]
        else:
            return []
    
    def create_insert_sql(self, table_name: str, columns: List[str]) -> str:
        """建立 INSERT SQL 語句"""
        # 加入額外欄位（縣市代碼、縣市名稱、source_file、quarter）
        all_columns = ['縣市代碼', '縣市名稱'] + columns + ['source_file', 'quarter']
        placeholders = ', '.join(['?' for _ in all_columns])
        column_names = ', '.join([f'[{col}]' if '-' in col else col for col in all_columns])
        
        return f"INSERT INTO [{table_name}] ({column_names}) VALUES ({placeholders})"
    
    def insert_data_batch(self, database_name: str, table_name: str, df: pd.DataFrame,
                         source_file: str, quarter: str, city_code: str, city_name: str) -> bool:
        """批次插入資料（含縣市代碼）"""
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
                    
                    # 加入縣市代碼和縣市名稱
                    row_data.extend([city_code, city_name])
                    
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
    
    def import_single_file(self, file_path: str, quarter: str) -> bool:
        """匯入單一檔案（含縣市代碼）"""
        try:
            filename = os.path.basename(file_path)
            logger.info(f"🔄 開始匯入檔案: {filename}")
            
            # 取得檔案類型資訊
            file_info = self.file_mapping.get_file_info(filename)
            if not file_info:
                logger.error(f"❌ 不支援的檔案類型: {filename}")
                return False
            
            # 取得縣市資訊
            city_info = self.city_mapping.get_city_info_from_filename(filename)
            if not city_info:
                logger.error(f"❌ 無法識別縣市代碼: {filename}")
                return False
            
            logger.info(f"📋 檔案資訊: {file_info['description']} → {file_info['database_name']}.{file_info['table_name']}")
            logger.info(f"🏙️ 縣市資訊: {city_info['city_code']} ({city_info['city_name']})")
            
            # 讀取CSV檔案
            df = self.read_csv_file(file_path)
            if df is None or df.empty:
                logger.error(f"❌ 檔案為空或讀取失敗: {filename}")
                return False
            
            # 清理資料
            df = self.clean_data(df, file_info['file_type'])
            if df.empty:
                logger.error(f"❌ 清理後資料為空: {filename}")
                return False
            
            # 插入資料
            success = self.insert_data_batch(
                file_info['database_name'],
                file_info['table_name'],
                df,
                filename,
                quarter,
                city_info['city_code'],
                city_info['city_name']
            )
            
            if success:
                logger.info(f"✅ 檔案匯入成功: {filename}")
                return True
            else:
                logger.error(f"❌ 檔案匯入失敗: {filename}")
                return False
                
        except Exception as e:
            logger.error(f"❌ 匯入檔案失敗 {file_path}: {str(e)}")
            return False
    
    def import_single_folder(self, folder_name: str) -> Dict[str, int]:
        """匯入單一資料夾的所有檔案（含縣市代碼）"""
        logger.info(f"🔄 開始匯入資料夾: {folder_name}")
        print(f"🔄 開始匯入資料夾: {folder_name}")
        
        if not os.path.exists(folder_name):
            logger.error(f"❌ 資料夾不存在: {folder_name}")
            return {'success': 0, 'failed': 0, 'total': 0}
        
        # 取得所有CSV檔案
        csv_files = glob.glob(os.path.join(folder_name, "*.csv"))
        
        if not csv_files:
            logger.error(f"❌ 資料夾中沒有CSV檔案: {folder_name}")
            return {'success': 0, 'failed': 0, 'total': 0}
        
        logger.info(f"📁 找到 {len(csv_files)} 個CSV檔案")
        print(f"📁 找到 {len(csv_files)} 個CSV檔案")
        
        success_count = 0
        failed_count = 0
        
        for file_path in csv_files:
            filename = os.path.basename(file_path)
            print(f"\n📄 處理檔案: {filename}")
            
            if self.import_single_file(file_path, folder_name):
                success_count += 1
                print(f"✅ {filename} 匯入成功")
            else:
                failed_count += 1
                print(f"❌ {filename} 匯入失敗")
        
        result = {
            'success': success_count,
            'failed': failed_count,
            'total': len(csv_files)
        }
        
        logger.info(f"📊 資料夾匯入完成: {folder_name}")
        logger.info(f"   成功: {success_count}/{len(csv_files)}")
        logger.info(f"   失敗: {failed_count}/{len(csv_files)}")
        
        print(f"\n📊 資料夾匯入完成: {folder_name}")
        print(f"   成功: {success_count}/{len(csv_files)}")
        print(f"   失敗: {failed_count}/{len(csv_files)}")
        
        return result
    
    def get_folder_statistics(self, folder_name: str) -> Dict:
        """取得資料夾統計資訊（含縣市代碼）"""
        if not os.path.exists(folder_name):
            return {'error': '資料夾不存在'}
        
        csv_files = glob.glob(os.path.join(folder_name, "*.csv"))
        file_stats = {}
        
        for file_path in csv_files:
            filename = os.path.basename(file_path)
            file_info = self.file_mapping.get_file_info(filename)
            city_info = self.city_mapping.get_city_info_from_filename(filename)
            
            if file_info and city_info:
                file_type = f"{file_info['data_type'].value}_{file_info['file_type'].value}"
                city_key = f"{city_info['city_code']}_{city_info['city_name']}"
                
                if file_type not in file_stats:
                    file_stats[file_type] = {}
                
                if city_key not in file_stats[file_type]:
                    file_stats[file_type][city_key] = []
                
                # 讀取檔案行數
                try:
                    df = pd.read_csv(file_path, encoding='utf-8', skiprows=[1])
                    file_stats[file_type][city_key].append({
                        'filename': filename,
                        'rows': len(df),
                        'columns': len(df.columns)
                    })
                except:
                    file_stats[file_type][city_key].append({
                        'filename': filename,
                        'rows': 0,
                        'columns': 0
                    })
        
        return file_stats

def test_enhanced_importer():
    """測試增強版匯入器"""
    print("🧪 測試增強版資料匯入器（含縣市代碼）")
    print("=" * 80)
    
    importer = EnhancedDataImporter()
    
    # 測試資料夾統計
    print("\n📊 113Q1 資料夾統計（含縣市代碼）:")
    stats = importer.get_folder_statistics('113Q1')
    
    if 'error' in stats:
        print(f"❌ {stats['error']}")
        return
    
    for file_type, cities in stats.items():
        print(f"\n🔸 {file_type}:")
        total_rows = 0
        for city_key, files in cities.items():
            city_code, city_name = city_key.split('_', 1)
            print(f"   📍 {city_code} ({city_name}):")
            for file_info in files:
                print(f"     {file_info['filename']:<30} - {file_info['rows']:>6} 行, {file_info['columns']:>2} 欄位")
                total_rows += file_info['rows']
        print(f"   總計: {total_rows} 行")
    
    # 測試單一檔案匯入
    print(f"\n🔄 測試單一檔案匯入（含縣市代碼）...")
    test_file = "113Q1/a_lvr_land_b.csv"  # 預售屋檔案（臺北市）
    
    if os.path.exists(test_file):
        success = importer.import_single_file(test_file, "113Q1")
        if success:
            print(f"✅ 預售屋檔案匯入測試成功（含縣市代碼）")
        else:
            print(f"❌ 預售屋檔案匯入測試失敗")
    else:
        print(f"❌ 測試檔案不存在: {test_file}")

if __name__ == "__main__":
    test_enhanced_importer()

