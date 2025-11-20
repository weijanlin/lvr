# -*- coding: utf-8 -*-
"""
LVR 資料庫備份與還原工具
提供更強大的備份和還原功能
"""

import os
import sys
import time
import shutil
from datetime import datetime
from pathlib import Path
import pyodbc
import logging

from config import DB_CONFIG, DATABASES

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('database_backup_restore.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DatabaseBackupRestore:
    """資料庫備份與還原工具"""
    
    def __init__(self):
        self.server = DB_CONFIG['server']
        self.username = DB_CONFIG['username']
        self.password = DB_CONFIG['password']
        self.backup_dir = Path('backups')
        self.backup_dir.mkdir(exist_ok=True)
        
    def get_connection_string(self, database='master'):
        """取得連線字串"""
        return (
            f"DRIVER={{{DB_CONFIG['driver']}}};"
            f"SERVER={self.server};"
            f"UID={self.username};"
            f"PWD={self.password};"
            f"Database={database};"
            f"Trusted_Connection={DB_CONFIG['trusted_connection']};"
            f"Encrypt={DB_CONFIG['encrypt']};"
        )
    
    def test_connection(self):
        """測試資料庫連線"""
        try:
            conn_str = self.get_connection_string('master')
            conn = pyodbc.connect(conn_str)
            conn.close()
            logger.info("✅ 資料庫連線測試成功")
            return True
        except Exception as e:
            logger.error(f"❌ 資料庫連線測試失敗: {str(e)}")
            return False
    
    def backup_database(self, database_name, description=""):
        """備份單一資料庫"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_file = self.backup_dir / f"{database_name}_{timestamp}.bak"
            
            logger.info(f"🔄 開始備份 {database_name} 資料庫...")
            
            conn_str = self.get_connection_string('master')
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            
            # 執行備份
            backup_sql = f"""
            BACKUP DATABASE [{database_name}] 
            TO DISK = '{backup_file}' 
            WITH FORMAT, INIT, 
            NAME = '{database_name}-Full Database Backup', 
            SKIP, NOREWIND, NOUNLOAD, STATS = 10
            """
            
            cursor.execute(backup_sql)
            conn.commit()
            conn.close()
            
            # 檢查備份檔案是否建立成功
            if backup_file.exists():
                file_size = backup_file.stat().st_size
                logger.info(f"✅ {database_name} 備份成功")
                logger.info(f"   檔案: {backup_file}")
                logger.info(f"   大小: {file_size:,} bytes")
                return str(backup_file)
            else:
                logger.error(f"❌ {database_name} 備份失敗 - 檔案未建立")
                return None
                
        except Exception as e:
            logger.error(f"❌ {database_name} 備份失敗: {str(e)}")
            return None
    
    def backup_all_databases(self):
        """備份所有資料庫"""
        logger.info("🚀 開始備份所有 LVR 資料庫")
        logger.info("=" * 60)
        
        if not self.test_connection():
            return False
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_files = {}
        success_count = 0
        
        for db_type, db_name in DATABASES.items():
            logger.info(f"\n📁 備份 {db_name} ({db_type})...")
            backup_file = self.backup_database(db_name)
            
            if backup_file:
                backup_files[db_name] = backup_file
                success_count += 1
            else:
                logger.error(f"❌ {db_name} 備份失敗")
        
        # 建立備份資訊檔案
        self.create_backup_info(timestamp, backup_files)
        
        logger.info("\n" + "=" * 60)
        logger.info("📊 備份完成統計:")
        logger.info(f"   成功: {success_count}/{len(DATABASES)}")
        logger.info(f"   失敗: {len(DATABASES) - success_count}/{len(DATABASES)}")
        
        return success_count == len(DATABASES)
    
    def create_backup_info(self, timestamp, backup_files):
        """建立備份資訊檔案"""
        info_file = self.backup_dir / f"backup_info_{timestamp}.txt"
        
        with open(info_file, 'w', encoding='utf-8') as f:
            f.write("LVR 資料庫備份資訊\n")
            f.write("=" * 50 + "\n")
            f.write(f"備份時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"備份目錄: {self.backup_dir.absolute()}\n")
            f.write(f"資料庫伺服器: {self.server}\n")
            f.write("\n備份的資料庫:\n")
            
            for db_type, db_name in DATABASES.items():
                f.write(f"- {db_name} ({db_type})\n")
            
            f.write("\n備份檔案:\n")
            for db_name, backup_file in backup_files.items():
                if backup_file:
                    file_path = Path(backup_file)
                    file_size = file_path.stat().st_size if file_path.exists() else 0
                    f.write(f"- {file_path.name}: {file_size:,} bytes\n")
        
        logger.info(f"📄 備份資訊已儲存至: {info_file}")
    
    def restore_database(self, database_name, backup_file):
        """還原單一資料庫"""
        try:
            backup_path = Path(backup_file)
            if not backup_path.exists():
                logger.error(f"❌ 備份檔案不存在: {backup_file}")
                return False
            
            logger.info(f"🔄 開始還原 {database_name} 資料庫...")
            logger.info(f"   備份檔案: {backup_path}")
            
            conn_str = self.get_connection_string('master')
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            
            # 斷開資料庫連線
            logger.info("   斷開資料庫連線...")
            cursor.execute(f"ALTER DATABASE [{database_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
            conn.commit()
            
            # 還原資料庫
            logger.info("   還原資料庫...")
            restore_sql = f"""
            RESTORE DATABASE [{database_name}] 
            FROM DISK = '{backup_path}' 
            WITH REPLACE, STATS = 10
            """
            
            cursor.execute(restore_sql)
            conn.commit()
            
            # 恢復多使用者模式
            logger.info("   恢復多使用者模式...")
            cursor.execute(f"ALTER DATABASE [{database_name}] SET MULTI_USER")
            conn.commit()
            
            conn.close()
            
            logger.info(f"✅ {database_name} 還原成功")
            return True
            
        except Exception as e:
            logger.error(f"❌ {database_name} 還原失敗: {str(e)}")
            return False
    
    def list_backup_files(self):
        """列出所有備份檔案"""
        backup_files = list(self.backup_dir.glob("*.bak"))
        
        if not backup_files:
            logger.info("📁 沒有找到備份檔案")
            return []
        
        logger.info("📁 可用的備份檔案:")
        logger.info("-" * 60)
        
        # 按時間分組
        backup_groups = {}
        for backup_file in backup_files:
            # 從檔案名稱提取時間戳記
            parts = backup_file.stem.split('_')
            if len(parts) >= 3:
                timestamp = f"{parts[-2]}_{parts[-1]}"
                if timestamp not in backup_groups:
                    backup_groups[timestamp] = []
                backup_groups[timestamp].append(backup_file)
        
        for timestamp in sorted(backup_groups.keys(), reverse=True):
            logger.info(f"\n時間戳記: {timestamp}")
            for backup_file in backup_groups[timestamp]:
                file_size = backup_file.stat().st_size
                logger.info(f"  - {backup_file.name} ({file_size:,} bytes)")
        
        return backup_files
    
    def restore_by_timestamp(self, timestamp):
        """根據時間戳記還原所有資料庫"""
        logger.info(f"🔄 開始還原時間戳記為 {timestamp} 的資料庫...")
        
        success_count = 0
        total_count = 0
        
        for db_type, db_name in DATABASES.items():
            backup_file = self.backup_dir / f"{db_name}_{timestamp}.bak"
            
            if backup_file.exists():
                total_count += 1
                if self.restore_database(db_name, backup_file):
                    success_count += 1
            else:
                logger.warning(f"⚠️  跳過 {db_name} (沒有找到備份檔案)")
        
        logger.info(f"\n📊 還原完成: {success_count}/{total_count} 成功")
        return success_count == total_count
    
    def restore_latest(self):
        """還原最新的備份檔案"""
        logger.info("🔄 開始還原最新的備份檔案...")
        
        success_count = 0
        total_count = 0
        
        for db_type, db_name in DATABASES.items():
            # 找到最新的備份檔案
            pattern = f"{db_name}_*.bak"
            backup_files = list(self.backup_dir.glob(pattern))
            
            if backup_files:
                # 按修改時間排序，取最新的
                latest_backup = max(backup_files, key=lambda x: x.stat().st_mtime)
                total_count += 1
                
                logger.info(f"📁 使用最新備份: {latest_backup.name}")
                if self.restore_database(db_name, latest_backup):
                    success_count += 1
            else:
                logger.warning(f"⚠️  跳過 {db_name} (沒有找到備份檔案)")
        
        logger.info(f"\n📊 還原完成: {success_count}/{total_count} 成功")
        return success_count == total_count

def main():
    """主函數"""
    print("🚀 LVR 資料庫備份與還原工具")
    print("=" * 60)
    
    tool = DatabaseBackupRestore()
    
    while True:
        print("\n請選擇操作:")
        print("1. 備份所有資料庫")
        print("2. 備份單一資料庫")
        print("3. 還原所有資料庫 (按時間戳記)")
        print("4. 還原所有資料庫 (最新備份)")
        print("5. 還原單一資料庫")
        print("6. 列出備份檔案")
        print("7. 測試資料庫連線")
        print("0. 結束")
        
        choice = input("\n請選擇 (0-7): ").strip()
        
        if choice == "0":
            print("👋 再見！")
            break
        elif choice == "1":
            tool.backup_all_databases()
        elif choice == "2":
            print("\n可用的資料庫:")
            for i, (db_type, db_name) in enumerate(DATABASES.items(), 1):
                print(f"{i}. {db_name} ({db_type})")
            
            try:
                db_choice = int(input("請選擇資料庫編號: ")) - 1
                db_names = list(DATABASES.values())
                if 0 <= db_choice < len(db_names):
                    db_name = db_names[db_choice]
                    tool.backup_database(db_name)
                else:
                    print("❌ 無效的選擇")
            except ValueError:
                print("❌ 請輸入有效的數字")
        elif choice == "3":
            timestamp = input("請輸入時間戳記 (例如: 20250909_084500): ").strip()
            if timestamp:
                tool.restore_by_timestamp(timestamp)
            else:
                print("❌ 時間戳記不能為空")
        elif choice == "4":
            tool.restore_latest()
        elif choice == "5":
            backup_files = tool.list_backup_files()
            if backup_files:
                backup_file = input("請輸入備份檔案名稱: ").strip()
                if backup_file:
                    # 從檔案名稱提取資料庫名稱
                    db_name = backup_file.split('_')[0] + '_' + backup_file.split('_')[1]
                    tool.restore_database(db_name, tool.backup_dir / backup_file)
        elif choice == "6":
            tool.list_backup_files()
        elif choice == "7":
            tool.test_connection()
        else:
            print("❌ 無效的選擇")

if __name__ == "__main__":
    main()


