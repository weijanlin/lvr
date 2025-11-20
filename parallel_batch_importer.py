# -*- coding: utf-8 -*-
"""
並行批次匯入系統
使用多執行緒和多程序來提升匯入速度
"""

import os
import glob
import logging
import time
import threading
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from tqdm import tqdm
import pandas as pd
import queue
import multiprocessing as mp

from config import DB_CONFIG, DATABASES, DATA_FOLDERS, BATCH_SIZE, MAX_WORKERS
from enhanced_data_importer import EnhancedDataImporter
from file_type_mapping import FileTypeMapping
from city_code_mapping import CityCodeMapping

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('parallel_batch_import.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ParallelBatchImporter:
    """並行批次匯入器"""
    
    def __init__(self, max_workers: int = None, use_processes: bool = False):
        self.max_workers = max_workers or min(MAX_WORKERS, mp.cpu_count())
        self.use_processes = use_processes
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
            'folder_stats': {},
            'parallel_stats': {
                'threads_used': 0,
                'processes_used': 0,
                'avg_processing_time': 0
            }
        }
        self.lock = threading.Lock()
    
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
            'total_files': 0,
            'files_by_size': {
                'small': [],    # < 1000 rows
                'medium': [],   # 1000-10000 rows
                'large': []     # > 10000 rows
            }
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
                
                # 估算檔案大小（快速預覽）
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        line_count = sum(1 for _ in f)
                    
                    if line_count < 1000:
                        analysis['files_by_size']['small'].append(file_path)
                    elif line_count < 10000:
                        analysis['files_by_size']['medium'].append(file_path)
                    else:
                        analysis['files_by_size']['large'].append(file_path)
                except:
                    analysis['files_by_size']['small'].append(file_path)
        
        return analysis
    
    def import_single_file_worker(self, file_path: str, folder: str) -> Dict:
        """單一檔案匯入工作函數"""
        filename = os.path.basename(file_path)
        start_time = time.time()
        
        try:
            # 建立獨立的匯入器實例
            importer = EnhancedDataImporter()
            success = importer.import_single_file(file_path, folder)
            
            processing_time = time.time() - start_time
            
            return {
                'filename': filename,
                'file_path': file_path,
                'folder': folder,
                'success': success,
                'processing_time': processing_time,
                'error': None
            }
            
        except Exception as e:
            processing_time = time.time() - start_time
            return {
                'filename': filename,
                'file_path': file_path,
                'folder': folder,
                'success': False,
                'processing_time': processing_time,
                'error': str(e)
            }
    
    def import_folder_parallel(self, folder: str, files: List[str]) -> Dict:
        """並行匯入單一資料夾的所有檔案"""
        logger.info(f"📂 開始並行匯入資料夾: {folder} (使用 {self.max_workers} 個工作執行緒)")
        
        folder_stats = {
            'folder': folder,
            'total_files': len(files),
            'successful_files': 0,
            'failed_files': 0,
            'total_records': 0,
            'file_results': {},
            'errors': [],
            'processing_times': []
        }
        
        # 使用執行緒池進行並行處理
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交所有任務
            future_to_file = {
                executor.submit(self.import_single_file_worker, file_path, folder): file_path
                for file_path in files
            }
            
            # 使用進度條追蹤進度
            with tqdm(total=len(files), desc=f"並行匯入 {folder}", unit="檔案") as pbar:
                for future in as_completed(future_to_file):
                    result = future.result()
                    filename = result['filename']
                    
                    # 更新統計
                    with self.lock:
                        if result['success']:
                            folder_stats['successful_files'] += 1
                            folder_stats['file_results'][filename] = {
                                'status': 'success',
                                'processing_time': result['processing_time']
                            }
                            logger.info(f"✅ {filename} 匯入成功 ({result['processing_time']:.2f}s)")
                        else:
                            folder_stats['failed_files'] += 1
                            folder_stats['file_results'][filename] = {
                                'status': 'failed',
                                'error': result['error'],
                                'processing_time': result['processing_time']
                            }
                            folder_stats['errors'].append(f"{filename}: {result['error']}")
                            logger.error(f"❌ {filename} 匯入失敗: {result['error']}")
                        
                        folder_stats['processing_times'].append(result['processing_time'])
                    
                    pbar.set_postfix({
                        '成功': folder_stats['successful_files'],
                        '失敗': folder_stats['failed_files'],
                        '平均時間': f"{sum(folder_stats['processing_times'])/len(folder_stats['processing_times']):.2f}s"
                    })
                    pbar.update(1)
        
        # 計算平均處理時間
        if folder_stats['processing_times']:
            folder_stats['avg_processing_time'] = sum(folder_stats['processing_times']) / len(folder_stats['processing_times'])
        
        logger.info(f"📂 完成並行匯入資料夾: {folder} - 成功: {folder_stats['successful_files']}, 失敗: {folder_stats['failed_files']}, 平均時間: {folder_stats['avg_processing_time']:.2f}s")
        return folder_stats
    
    def import_all_folders_parallel(self, dry_run: bool = False) -> Dict:
        """並行匯入所有資料夾"""
        logger.info(f"🚀 開始並行批次匯入所有資料夾 (使用 {self.max_workers} 個工作執行緒)")
        
        self.stats['start_time'] = datetime.now()
        
        # 掃描所有檔案
        all_files = self.scan_all_folders()
        
        # 分析檔案分布
        analysis = self.analyze_files(all_files)
        
        logger.info("📊 檔案分布分析:")
        logger.info(f"  總檔案數: {analysis['total_files']}")
        logger.info(f"  小檔案 (<1000行): {len(analysis['files_by_size']['small'])}")
        logger.info(f"  中檔案 (1000-10000行): {len(analysis['files_by_size']['medium'])}")
        logger.info(f"  大檔案 (>10000行): {len(analysis['files_by_size']['large'])}")
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
        
        # 並行匯入每個資料夾
        for folder, files in all_files.items():
            if files:
                folder_stats = self.import_folder_parallel(folder, files)
                self.stats['folder_stats'][folder] = folder_stats
                self.stats['total_files'] += folder_stats['total_files']
                self.stats['successful_files'] += folder_stats['successful_files']
                self.stats['failed_files'] += folder_stats['failed_files']
                self.stats['total_records'] += folder_stats['total_records']
            else:
                logger.warning(f"⚠️ 資料夾 {folder} 沒有CSV檔案")
        
        self.stats['end_time'] = datetime.now()
        self.stats['total_folders'] = len([f for f in all_files.values() if f])
        self.stats['parallel_stats']['threads_used'] = self.max_workers
        
        # 計算平均處理時間
        all_processing_times = []
        for folder_stats in self.stats['folder_stats'].values():
            all_processing_times.extend(folder_stats['processing_times'])
        
        if all_processing_times:
            self.stats['parallel_stats']['avg_processing_time'] = sum(all_processing_times) / len(all_processing_times)
        
        # 生成匯入報告
        self.generate_parallel_import_report()
        
        return self.stats
    
    def generate_parallel_import_report(self):
        """生成並行匯入報告"""
        logger.info("📋 生成並行匯入報告")
        
        duration = self.stats['end_time'] - self.stats['start_time']
        success_rate = (self.stats['successful_files'] / self.stats['total_files'] * 100) if self.stats['total_files'] > 0 else 0
        
        report = f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                           並行批次匯入報告                                    ║
╠══════════════════════════════════════════════════════════════════════════════╣
║ 匯入時間: {self.stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')} - {self.stats['end_time'].strftime('%Y-%m-%d %H:%M:%S')}                    ║
║ 總耗時: {duration}                                                           ║
║                                                                              ║
║ 並行處理統計:                                                                ║
║   使用執行緒數: {self.stats['parallel_stats']['threads_used']}                                                      ║
║   平均處理時間: {self.stats['parallel_stats']['avg_processing_time']:.2f}秒/檔案                                        ║
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
            folder_success_rate = (stats['successful_files'] / stats['total_files'] * 100) if stats['total_files'] > 0 else 0
            report += f"║   {folder}: {stats['successful_files']}/{stats['total_files']} ({folder_success_rate:.1f}%) - 平均: {stats['avg_processing_time']:.2f}s/檔案\n"
        
        report += "║                                                                              ║\n"
        
        if self.stats['failed_files'] > 0:
            report += "║ 失敗檔案列表:                                                              ║\n"
            for folder, stats in self.stats['folder_stats'].items():
                if stats['errors']:
                    report += f"║   {folder}:\n"
                    for error in stats['errors'][:3]:  # 只顯示前3個錯誤
                        report += f"║     - {error}\n"
                    if len(stats['errors']) > 3:
                        report += f"║     ... 還有 {len(stats['errors']) - 3} 個錯誤\n"
        
        report += "╚══════════════════════════════════════════════════════════════════════════════╝"
        
        logger.info(report)
        
        # 保存報告到檔案
        with open('parallel_batch_import_report.txt', 'w', encoding='utf-8') as f:
            f.write(report)
        
        logger.info("📄 並行匯入報告已保存到 parallel_batch_import_report.txt")

def main():
    """主函數"""
    print("🚀 並行批次匯入系統")
    print("=" * 80)
    
    # 詢問並行設定
    print("並行處理設定:")
    print("1. 自動設定 (建議)")
    print("2. 手動設定執行緒數")
    
    choice = input("請選擇 (1/2): ").strip()
    
    if choice == "2":
        try:
            max_workers = int(input("請輸入執行緒數 (建議 2-8): ").strip())
        except ValueError:
            max_workers = 4
            print(f"使用預設值: {max_workers}")
    else:
        max_workers = min(MAX_WORKERS, mp.cpu_count())
        print(f"自動設定執行緒數: {max_workers}")
    
    importer = ParallelBatchImporter(max_workers=max_workers)
    
    # 詢問是否執行乾跑
    print("\n選擇執行模式:")
    print("1. 乾跑模式 (只分析檔案，不執行匯入)")
    print("2. 實際匯入模式")
    
    choice = input("請選擇 (1/2): ").strip()
    
    if choice == "1":
        result = importer.import_all_folders_parallel(dry_run=True)
        print("\n🔍 乾跑模式完成")
        print(f"總檔案數: {result['analysis']['total_files']}")
        print("檔案大小分布:")
        for size, files in result['analysis']['files_by_size'].items():
            print(f"  {size}: {len(files)} 個檔案")
    else:
        print(f"\n⚠️ 即將開始並行匯入，使用 {max_workers} 個執行緒...")
        confirm = input("確定要繼續嗎? (y/N): ").strip().lower()
        
        if confirm == 'y':
            result = importer.import_all_folders_parallel(dry_run=False)
            print("\n✅ 並行批次匯入完成!")
            print(f"成功: {result['successful_files']}/{result['total_files']} 檔案")
            print(f"成功率: {(result['successful_files'] / result['total_files'] * 100):.1f}%")
            print(f"平均處理時間: {result['parallel_stats']['avg_processing_time']:.2f}秒/檔案")
            print(f"總記錄數: {result['total_records']:,}")
        else:
            print("❌ 匯入已取消")

if __name__ == "__main__":
    main()




