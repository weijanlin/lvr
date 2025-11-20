# -*- coding: utf-8 -*-
"""
快速並行匯入腳本
用於快速並行匯入所有資料夾的CSV檔案
"""

import os
import glob
import logging
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import threading

from config import DATA_FOLDERS, MAX_WORKERS
from enhanced_data_importer import EnhancedDataImporter

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('quick_parallel_import.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def import_single_file_worker(file_path: str, folder: str) -> dict:
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

def quick_parallel_import_all(max_workers: int = 4):
    """快速並行匯入所有資料夾"""
    print(f"🚀 快速並行批次匯入 (使用 {max_workers} 個執行緒)")
    print("=" * 80)
    
    # 統計資訊
    total_files = 0
    successful_files = 0
    failed_files = 0
    total_records = 0
    start_time = datetime.now()
    processing_times = []
    lock = threading.Lock()
    
    # 掃描所有檔案
    all_files = []
    for folder in DATA_FOLDERS:
        if os.path.exists(folder):
            csv_files = glob.glob(os.path.join(folder, "*.csv"))
            all_files.extend([(file_path, folder) for file_path in csv_files])
            print(f"📁 {folder}: {len(csv_files)} 個CSV檔案")
        else:
            print(f"❌ 資料夾不存在: {folder}")
    
    total_files = len(all_files)
    print(f"\n📊 總計: {total_files} 個CSV檔案")
    
    if total_files == 0:
        print("❌ 沒有找到任何CSV檔案")
        return
    
    # 開始並行匯入
    print(f"\n🚀 開始並行匯入...")
    start_time = datetime.now()
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任務
        future_to_file = {
            executor.submit(import_single_file_worker, file_path, folder): (file_path, folder)
            for file_path, folder in all_files
        }
        
        # 使用進度條追蹤進度
        with tqdm(total=total_files, desc="並行匯入進度", unit="檔案") as pbar:
            for future in as_completed(future_to_file):
                result = future.result()
                filename = result['filename']
                
                # 更新統計
                with lock:
                    if result['success']:
                        successful_files += 1
                        logger.info(f"✅ {filename} 匯入成功 ({result['processing_time']:.2f}s)")
                    else:
                        failed_files += 1
                        logger.error(f"❌ {filename} 匯入失敗: {result['error']}")
                    
                    processing_times.append(result['processing_time'])
                
                pbar.set_postfix({
                    '成功': successful_files,
                    '失敗': failed_files,
                    '平均時間': f"{sum(processing_times)/len(processing_times):.2f}s"
                })
                pbar.update(1)
    
    # 計算統計
    end_time = datetime.now()
    duration = end_time - start_time
    success_rate = (successful_files / total_files * 100) if total_files > 0 else 0
    avg_processing_time = sum(processing_times) / len(processing_times) if processing_times else 0
    
    # 顯示結果
    print(f"\n✅ 並行批次匯入完成!")
    print("=" * 80)
    print(f"⏱️  總耗時: {duration}")
    print(f"🧵 使用執行緒數: {max_workers}")
    print(f"📁 總檔案數: {total_files}")
    print(f"✅ 成功檔案數: {successful_files}")
    print(f"❌ 失敗檔案數: {failed_files}")
    print(f"📊 成功率: {success_rate:.1f}%")
    print(f"⏱️  平均處理時間: {avg_processing_time:.2f}秒/檔案")
    print(f"📝 總記錄數: {total_records:,}")
    
    # 效能分析
    if processing_times:
        min_time = min(processing_times)
        max_time = max(processing_times)
        print(f"\n📊 處理時間分析:")
        print(f"  最快檔案: {min_time:.2f}秒")
        print(f"  最慢檔案: {max_time:.2f}秒")
        print(f"  平均時間: {avg_processing_time:.2f}秒")
    
    # 保存統計到檔案
    stats_file = f"quick_parallel_import_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    with open(stats_file, 'w', encoding='utf-8') as f:
        f.write(f"快速並行匯入統計報告\n")
        f.write(f"匯入時間: {start_time.strftime('%Y-%m-%d %H:%M:%S')} - {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"總耗時: {duration}\n")
        f.write(f"使用執行緒數: {max_workers}\n")
        f.write(f"總檔案數: {total_files}\n")
        f.write(f"成功檔案數: {successful_files}\n")
        f.write(f"失敗檔案數: {failed_files}\n")
        f.write(f"成功率: {success_rate:.1f}%\n")
        f.write(f"平均處理時間: {avg_processing_time:.2f}秒/檔案\n")
        f.write(f"總記錄數: {total_records:,}\n")
    
    print(f"📄 統計報告已保存到: {stats_file}")

def main():
    """主函數"""
    print("選擇執行模式:")
    print("1. 快速並行匯入 (4執行緒)")
    print("2. 高並行匯入 (8執行緒)")
    print("3. 自訂執行緒數")
    print("4. 只匯入第一個資料夾 (測試)")
    
    choice = input("請選擇 (1/2/3/4): ").strip()
    
    if choice == "1":
        quick_parallel_import_all(max_workers=4)
    elif choice == "2":
        quick_parallel_import_all(max_workers=8)
    elif choice == "3":
        try:
            max_workers = int(input("請輸入執行緒數 (建議 2-8): ").strip())
            quick_parallel_import_all(max_workers=max_workers)
        except ValueError:
            print("❌ 無效的執行緒數，使用預設值 4")
            quick_parallel_import_all(max_workers=4)
    elif choice == "4":
        # 只匯入第一個資料夾
        if DATA_FOLDERS and os.path.exists(DATA_FOLDERS[0]):
            folder = DATA_FOLDERS[0]
            csv_files = glob.glob(os.path.join(folder, "*.csv"))
            print(f"📁 測試資料夾: {folder} ({len(csv_files)} 個檔案)")
            
            # 只處理前10個檔案
            test_files = csv_files[:10]
            print(f"📄 測試檔案數: {len(test_files)}")
            
            importer = EnhancedDataImporter()
            successful = 0
            failed = 0
            
            with ThreadPoolExecutor(max_workers=4) as executor:
                future_to_file = {
                    executor.submit(import_single_file_worker, file_path, folder): file_path
                    for file_path in test_files
                }
                
                with tqdm(total=len(test_files), desc="測試匯入", unit="檔案") as pbar:
                    for future in as_completed(future_to_file):
                        result = future.result()
                        if result['success']:
                            successful += 1
                            print(f"✅ {result['filename']}")
                        else:
                            failed += 1
                            print(f"❌ {result['filename']}: {result['error']}")
                        pbar.update(1)
            
            print(f"\n測試結果: 成功 {successful}, 失敗 {failed}")
        else:
            print("❌ 沒有可用的測試資料夾")
    else:
        print("❌ 無效的選擇")

if __name__ == "__main__":
    main()




