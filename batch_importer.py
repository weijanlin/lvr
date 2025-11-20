# -*- coding: utf-8 -*-
"""
批次匯入系統
處理所有6個季度資料夾的CSV檔案匯入
"""

import os
import glob
import logging
import time
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from tqdm import tqdm
import pandas as pd

from config import DB_CONFIG, DATABASES, DATA_FOLDERS, BATCH_SIZE
from enhanced_data_importer import EnhancedDataImporter
from file_type_mapping import FileTypeMapping
from city_code_mapping import CityCodeMapping

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('batch_import.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BatchImporter:
    """批次匯入器"""
    
    def __init__(self):
        self.importer = EnhancedDataImporter()
        self.file_mapping = FileTypeMapping()
        self.city_mapping = CityCodeMapping()
        self.stats = {
            'total_folders': 0,
            'total_files': 0,
            'successful_files': 0,
            'failed_files': 0,
            'total_records': 0,
            'start_time': None,
            'end_time': None,
            'folder_stats': {}
        }
    
    def scan_all_folders(self) -> Dict[str, List[str]]:
        """掃描所有資料夾中的CSV檔案"""
        logger.info("🔍 掃描所有資料夾中的CSV檔案")
        
        all_files = {}
        
        for folder in DATA_FOLDERS:
            if os.path.exists(folder):
                csv_files = glob.glob(os.path.join(folder, "*.csv"))
                all_files[folder] = csv_files
                logger.info(f"📁 {folder}: 找到 {len(csv_files)} 個CSV檔案")
            else:
                logger.warning(f"⚠️ 資料夾不存在: {folder}")
                all_files[folder] = []
        
        return all_files
    
    def analyze_files(self, all_files: Dict[str, List[str]]) -> Dict:
        """分析檔案分布"""
        logger.info("📊 分析檔案分布")
        
        analysis = {
            'file_types': {},
            'city_distribution': {},
            'total_files': 0
        }
        
        for folder, files in all_files.items():
            for file_path in files:
                filename = os.path.basename(file_path)
                analysis['total_files'] += 1
                
                # 分析檔案類型
                file_info = self.file_mapping.get_file_info(filename)
                if file_info:
                    file_type = file_info['description']
                    if file_type not in analysis['file_types']:
                        analysis['file_types'][file_type] = 0
                    analysis['file_types'][file_type] += 1
                
                # 分析縣市分布
                city_code = self.city_mapping.extract_city_code_from_filename(filename)
                if city_code:
                    city_name = self.city_mapping.get_city_name(city_code)
                    if city_name not in analysis['city_distribution']:
                        analysis['city_distribution'][city_name] = 0
                    analysis['city_distribution'][city_name] += 1
        
        return analysis
    
    def import_folder(self, folder: str, files: List[str]) -> Dict:
        """匯入單一資料夾的所有檔案"""
        logger.info(f"📂 開始匯入資料夾: {folder}")
        
        folder_stats = {
            'folder': folder,
            'total_files': len(files),
            'successful_files': 0,
            'failed_files': 0,
            'total_records': 0,
            'file_results': {},
            'errors': []
        }
        
        # 使用進度條
        with tqdm(files, desc=f"匯入 {folder}", unit="檔案") as pbar:
            for file_path in files:
                filename = os.path.basename(file_path)
                pbar.set_postfix({
                    '成功': folder_stats['successful_files'],
                    '失敗': folder_stats['failed_files'],
                    '記錄': f"{folder_stats['total_records']:,}"
                })
                
                try:
                    # 匯入檔案
                    success = self.importer.import_single_file(file_path, folder)
                    
                    if success:
                        folder_stats['successful_files'] += 1
                        folder_stats['file_results'][filename] = {
                            'status': 'success'
                        }
                        logger.info(f"✅ {filename} 匯入成功")
                    else:
                        folder_stats['failed_files'] += 1
                        folder_stats['file_results'][filename] = {
                            'status': 'failed',
                            'error': 'Import failed'
                        }
                        folder_stats['errors'].append(f"{filename}: Import failed")
                        logger.error(f"❌ {filename} 匯入失敗")
                
                except Exception as e:
                    folder_stats['failed_files'] += 1
                    folder_stats['file_results'][filename] = {
                        'status': 'error',
                        'error': str(e)
                    }
                    folder_stats['errors'].append(f"{filename}: {str(e)}")
                    logger.error(f"❌ {filename} 處理異常: {str(e)}")
                
                pbar.update(1)
        
        logger.info(f"📂 完成匯入資料夾: {folder} - 成功: {folder_stats['successful_files']}, 失敗: {folder_stats['failed_files']}")
        return folder_stats
    
    def import_all_folders(self, dry_run: bool = False) -> Dict:
        """匯入所有資料夾"""
        logger.info("🚀 開始批次匯入所有資料夾")
        
        self.stats['start_time'] = datetime.now()
        
        # 掃描所有檔案
        all_files = self.scan_all_folders()
        
        # 分析檔案分布
        analysis = self.analyze_files(all_files)
        
        logger.info("📊 檔案分布分析:")
        logger.info(f"  總檔案數: {analysis['total_files']}")
        logger.info("  檔案類型分布:")
        for file_type, count in analysis['file_types'].items():
            logger.info(f"    {file_type}: {count} 個檔案")
        logger.info("  縣市分布:")
        for city, count in analysis['city_distribution'].items():
            logger.info(f"    {city}: {count} 個檔案")
        
        if dry_run:
            logger.info("🔍 乾跑模式 - 不執行實際匯入")
            return {
                'analysis': analysis,
                'files': all_files,
                'dry_run': True
            }
        
        # 匯入每個資料夾
        for folder, files in all_files.items():
            if files:
                folder_stats = self.import_folder(folder, files)
                self.stats['folder_stats'][folder] = folder_stats
                self.stats['total_files'] += folder_stats['total_files']
                self.stats['successful_files'] += folder_stats['successful_files']
                self.stats['failed_files'] += folder_stats['failed_files']
                self.stats['total_records'] += folder_stats['total_records']
            else:
                logger.warning(f"⚠️ 資料夾 {folder} 沒有CSV檔案")
        
        self.stats['end_time'] = datetime.now()
        self.stats['total_folders'] = len([f for f in all_files.values() if f])
        
        # 生成匯入報告
        self.generate_import_report()
        
        return self.stats
    
    def generate_import_report(self):
        """生成匯入報告"""
        logger.info("📋 生成匯入報告")
        
        duration = self.stats['end_time'] - self.stats['start_time']
        
        success_rate = (self.stats['successful_files'] / self.stats['total_files'] * 100) if self.stats['total_files'] > 0 else 0
        
        report = f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                           批次匯入報告                                        ║
╠══════════════════════════════════════════════════════════════════════════════╣
║ 匯入時間: {self.stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')} - {self.stats['end_time'].strftime('%Y-%m-%d %H:%M:%S')}                    ║
║ 總耗時: {duration}                                                           ║
║                                                                              ║
║ 資料夾統計:                                                                  ║
║   總資料夾數: {self.stats['total_folders']}                                                      ║
║   總檔案數: {self.stats['total_files']}                                                        ║
║   成功檔案數: {self.stats['successful_files']}                                                    ║
║   失敗檔案數: {self.stats['failed_files']}                                                      ║
║   成功率: {success_rate:.1f}%                                                      ║
║   總記錄數: {self.stats['total_records']:,}                                                      ║
║                                                                              ║
║ 各資料夾詳細統計:                                                            ║
"""
        
        for folder, stats in self.stats['folder_stats'].items():
            success_rate = (stats['successful_files'] / stats['total_files'] * 100) if stats['total_files'] > 0 else 0
            report += f"║   {folder}: {stats['successful_files']}/{stats['total_files']} ({success_rate:.1f}%) - {stats['total_records']:,} 筆記錄\n"
        
        report += "║                                                                              ║\n"
        
        if self.stats['failed_files'] > 0:
            report += "║ 失敗檔案列表:                                                              ║\n"
            for folder, stats in self.stats['folder_stats'].items():
                if stats['errors']:
                    report += f"║   {folder}:\n"
                    for error in stats['errors'][:5]:  # 只顯示前5個錯誤
                        report += f"║     - {error}\n"
                    if len(stats['errors']) > 5:
                        report += f"║     ... 還有 {len(stats['errors']) - 5} 個錯誤\n"
        
        report += "╚══════════════════════════════════════════════════════════════════════════════╝"
        
        logger.info(report)
        
        # 保存報告到檔案
        with open('batch_import_report.txt', 'w', encoding='utf-8') as f:
            f.write(report)
        
        logger.info("📄 匯入報告已保存到 batch_import_report.txt")

def main():
    """主函數"""
    print("🚀 批次匯入系統")
    print("=" * 80)
    
    importer = BatchImporter()
    
    # 詢問是否執行乾跑
    print("選擇執行模式:")
    print("1. 乾跑模式 (只分析檔案，不執行匯入)")
    print("2. 實際匯入模式")
    
    choice = input("請選擇 (1/2): ").strip()
    
    if choice == "1":
        result = importer.import_all_folders(dry_run=True)
        print("\n🔍 乾跑模式完成")
        print(f"總檔案數: {result['analysis']['total_files']}")
        print("檔案類型分布:")
        for file_type, count in result['analysis']['file_types'].items():
            print(f"  {file_type}: {count} 個檔案")
    else:
        print("\n⚠️ 即將開始實際匯入，這可能需要很長時間...")
        confirm = input("確定要繼續嗎? (y/N): ").strip().lower()
        
        if confirm == 'y':
            result = importer.import_all_folders(dry_run=False)
            print("\n✅ 批次匯入完成!")
            print(f"成功: {result['successful_files']}/{result['total_files']} 檔案")
            print(f"總記錄數: {result['total_records']:,}")
        else:
            print("❌ 匯入已取消")

if __name__ == "__main__":
    main()