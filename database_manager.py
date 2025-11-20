# -*- coding: utf-8 -*-
"""
資料庫管理類別
負責建立資料庫連線、建立資料庫和資料表
"""

import pyodbc
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import logging
from typing import Dict, List, Optional
import os

from config import DB_CONFIG, DATABASES

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


class DatabaseManager:
    """資料庫管理類別"""
    
    def __init__(self):
        self.connection_string = self._build_connection_string()
        self.engine = None
        self.connection = None
        
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
    
    def test_connection(self) -> bool:
        """測試資料庫連線"""
        try:
            # 先連接到 master 資料庫
            conn_str = self.connection_string + "Database=master;"
            conn = pyodbc.connect(conn_str)
            conn.close()
            logger.info("✅ 資料庫連線測試成功")
            return True
        except Exception as e:
            logger.error(f"❌ 資料庫連線測試失敗: {str(e)}")
            return False
    
    def create_databases(self) -> bool:
        """建立所需的資料庫"""
        try:
            for db_name in DATABASES.values():
                try:
                    # 為每個資料庫使用單獨的連線
                    conn_str = self.connection_string + "Database=master;"
                    conn = pyodbc.connect(conn_str)
                    cursor = conn.cursor()
                    
                    # 檢查資料庫是否已存在
                    cursor.execute(f"SELECT name FROM sys.databases WHERE name = '{db_name}'")
                    if not cursor.fetchone():
                        # 建立資料庫
                        cursor.execute(f"CREATE DATABASE [{db_name}]")
                        logger.info(f"✅ 已建立資料庫: {db_name}")
                    else:
                        logger.info(f"ℹ️ 資料庫已存在: {db_name}")
                    
                    conn.close()
                    
                except Exception as e:
                    logger.error(f"❌ 建立資料庫 {db_name} 失敗: {str(e)}")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"❌ 建立資料庫失敗: {str(e)}")
            return False
    
    def get_schema_info(self, schema_file: str) -> Dict[str, str]:
        """讀取 schema 檔案，取得欄位名稱和標題對應"""
        try:
            df = pd.read_csv(schema_file, encoding='utf-8')
            schema_dict = dict(zip(df['name'], df['title']))
            return schema_dict
        except Exception as e:
            logger.error(f"❌ 讀取 schema 檔案失敗 {schema_file}: {str(e)}")
            return {}
    
    def create_table_sql(self, table_name: str, schema_dict: Dict[str, str]) -> str:
        """根據 schema 產生 CREATE TABLE SQL 語句"""
        # 簡化的資料類型對應，實際使用時可能需要更精確的對應
        type_mapping = {
            '鄉鎮市區': 'NVARCHAR(50)',
            '交易標的': 'NVARCHAR(100)',
            '土地位置建物門牌': 'NVARCHAR(200)',
            '土地移轉總面積平方公尺': 'DECIMAL(10,2)',
            '都市土地使用分區': 'NVARCHAR(100)',
            '非都市土地使用分區': 'NVARCHAR(100)',
            '非都市土地使用編定': 'NVARCHAR(100)',
            '交易年月日': 'NVARCHAR(20)',
            '交易筆棟數': 'INT',
            '移轉層次': 'NVARCHAR(50)',
            '總樓層數': 'INT',
            '建物型態': 'NVARCHAR(100)',
            '主要用途': 'NVARCHAR(100)',
            '主要建材': 'NVARCHAR(100)',
            '建築完成年月': 'NVARCHAR(20)',
            '建物移轉總面積平方公尺': 'DECIMAL(10,2)',
            '建物現況格局-房': 'INT',
            '建物現況格局-廳': 'INT',
            '建物現況格局-衛': 'INT',
            '建物現況格局-隔間': 'NVARCHAR(50)',
            '有無管理組織': 'NVARCHAR(20)',
            '總價元': 'DECIMAL(15,2)',
            '單價元平方公尺': 'DECIMAL(15,2)',
            '車位類別': 'NVARCHAR(50)',
            '車位移轉總面積平方公尺': 'DECIMAL(10,2)',
            '車位總價元': 'DECIMAL(15,2)',
            '備註': 'NVARCHAR(500)',
            '編號': 'NVARCHAR(50)',
            '主建物面積': 'DECIMAL(10,2)',
            '附屬建物面積': 'DECIMAL(10,2)',
            '陽台面積': 'DECIMAL(10,2)',
            '電梯': 'NVARCHAR(20)',
            '移轉編號': 'NVARCHAR(50)'
        }
        
        columns = []
        for field_name in schema_dict.keys():
            sql_type = type_mapping.get(field_name, 'NVARCHAR(255)')  # 預設使用 NVARCHAR(255)
            columns.append(f"[{field_name}] {sql_type}")
        
        # 加入建立時間和來源檔案欄位
        columns.extend([
            "[created_at] DATETIME2 DEFAULT GETDATE()",
            "[source_file] NVARCHAR(200)",
            "[quarter] NVARCHAR(10)"
        ])
        
        sql = f"""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{table_name}')
        CREATE TABLE [{table_name}] (
            [id] BIGINT IDENTITY(1,1) PRIMARY KEY,
            {',\n            '.join(columns)}
        )
        """
        return sql
    
    def create_tables(self, database_name: str) -> bool:
        """在指定資料庫中建立資料表"""
        try:
            # 連接到指定資料庫
            conn_str = self.connection_string + f"Database={database_name};"
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            
            # 建立主要資料表
            tables = {
                'main_data': 'schema-main.csv',
                'build_data': 'schema-build.csv', 
                'land_data': 'schema-land.csv',
                'park_data': 'schema-park.csv'
            }
            
            for table_name, schema_file in tables.items():
                schema_path = os.path.join('113Q1', schema_file)  # 使用第一個資料夾的 schema
                if os.path.exists(schema_path):
                    schema_dict = self.get_schema_info(schema_path)
                    if schema_dict:
                        create_sql = self.create_table_sql(table_name, schema_dict)
                        cursor.execute(create_sql)
                        logger.info(f"✅ 已在資料庫 {database_name} 中建立資料表: {table_name}")
                    else:
                        logger.warning(f"⚠️ 無法讀取 schema 檔案: {schema_path}")
                else:
                    logger.warning(f"⚠️ Schema 檔案不存在: {schema_path}")
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"❌ 在資料庫 {database_name} 中建立資料表失敗: {str(e)}")
            return False
    
    def setup_all_databases(self) -> bool:
        """設定所有資料庫和資料表"""
        try:
            logger.info("🚀 開始設定資料庫...")
            
            # 測試連線
            if not self.test_connection():
                return False
            
            # 建立資料庫
            if not self.create_databases():
                return False
            
            # 為每個資料庫建立資料表
            for db_name in DATABASES.values():
                if not self.create_tables(db_name):
                    logger.error(f"❌ 無法為資料庫 {db_name} 建立資料表")
                    return False
            
            logger.info("✅ 所有資料庫設定完成")
            return True
            
        except Exception as e:
            logger.error(f"❌ 設定資料庫失敗: {str(e)}")
            return False


if __name__ == "__main__":
    # 測試資料庫管理功能
    db_manager = DatabaseManager()
    success = db_manager.setup_all_databases()
    
    if success:
        print("🎉 資料庫設定成功！")
    else:
        print("�� 資料庫設定失敗，請檢查錯誤日誌")
