# -*- coding: utf-8 -*-
"""
自動掃描並匯入新資料夾
此程式會自動掃描當前目錄下的所有資料夾，找出不在 config.py 中定義的新資料夾並匯入
"""

import os
import glob
import logging
import time
import re
import sys
from datetime import datetime
from typing import List, Dict, Optional
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
        logging.FileHandler('new_folders_import.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def scan_new_folders(exclude_folders: List[str] = None) -> List[str]:
    """
    掃描當前目錄下的新資料夾
    
    Args:
        exclude_folders: 要排除的資料夾列表（預設為 config.py 中的 DATA_FOLDERS）
    
    Returns:
        新資料夾列表
    """
    if exclude_folders is None:
        exclude_folders = DATA_FOLDERS.copy()
    
    # 排除的資料夾和檔案
    exclude_items = exclude_folders + [
        '__pycache__', 'backups', '.git', '.vscode', 
        'node_modules', 'venv', 'env', '.idea'
    ]
    
    # 取得當前目錄下的所有資料夾
    current_dir = os.getcwd()
    all_items = os.listdir(current_dir)
    
    new_folders = []
    for item in all_items:
        item_path = os.path.join(current_dir, item)
        # 只處理資料夾，且不在排除列表中
        if os.path.isdir(item_path) and item not in exclude_items:
            # 檢查資料夾中是否有 CSV 檔案
            csv_files = glob.glob(os.path.join(item_path, "*.csv"))
            if csv_files:
                new_folders.append(item)
                logger.info(f"📁 發現新資料夾: {item} (包含 {len(csv_files)} 個CSV檔案)")
    
    return sorted(new_folders)

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
        logger.error(f"❌ 匯入 {filename} 失敗: {str(e)}")
        return {
            'filename': filename,
            'file_path': file_path,
            'folder': folder,
            'success': False,
            'processing_time': processing_time,
            'error': str(e)
        }

def import_new_folders(new_folders: List[str], max_workers: int = None):
    """
    匯入新資料夾中的所有CSV檔案
    
    Args:
        new_folders: 要匯入的新資料夾列表
        max_workers: 最大並行執行緒數（預設使用 config.py 中的 MAX_WORKERS）
    """
    if not new_folders:
        logger.info("✅ 沒有發現新資料夾")
        return
    
    if max_workers is None:
        max_workers = MAX_WORKERS
    
    logger.info(f"🚀 開始匯入 {len(new_folders)} 個新資料夾 (使用 {max_workers} 個執行緒)")
    logger.info(f"📂 新資料夾列表: {', '.join(new_folders)}")
    
    # 統計資訊
    total_files = 0
    successful_files = 0
    failed_files = 0
    total_records = 0
    start_time = datetime.now()
    processing_times = []
    lock = threading.Lock()
    folder_stats = {}
    
    # 掃描所有新資料夾中的CSV檔案
    all_files = []
    for folder in new_folders:
        if os.path.exists(folder):
            csv_files = glob.glob(os.path.join(folder, "*.csv"))
            all_files.extend([(file_path, folder) for file_path in csv_files])
            folder_stats[folder] = {
                'total_files': len(csv_files),
                'successful_files': 0,
                'failed_files': 0
            }
            logger.info(f"📁 {folder}: 找到 {len(csv_files)} 個CSV檔案")
        else:
            logger.warning(f"⚠️ 資料夾不存在: {folder}")
    
    total_files = len(all_files)
    logger.info(f"\n📊 總計: {total_files} 個CSV檔案")
    
    if total_files == 0:
        logger.warning("❌ 沒有找到任何CSV檔案")
        return
    
    # 開始並行匯入
    logger.info(f"\n🚀 開始並行匯入...")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任務
        future_to_file = {
            executor.submit(import_single_file_worker, file_path, folder): (file_path, folder)
            for file_path, folder in all_files
        }
        
        # 使用 tqdm 顯示進度
        with tqdm(total=total_files, desc="匯入進度", unit="檔案") as pbar:
            for future in as_completed(future_to_file):
                result = future.result()
                
                with lock:
                    if result['success']:
                        successful_files += 1
                        folder_stats[result['folder']]['successful_files'] += 1
                    else:
                        failed_files += 1
                        folder_stats[result['folder']]['failed_files'] += 1
                        logger.warning(f"❌ {result['folder']}/{result['filename']}: {result['error']}")
                    
                    processing_times.append(result['processing_time'])
                
                pbar.update(1)
    
    # 計算統計資訊
    end_time = datetime.now()
    duration = end_time - start_time
    success_rate = (successful_files / total_files * 100) if total_files > 0 else 0
    avg_processing_time = sum(processing_times) / len(processing_times) if processing_times else 0
    
    # 輸出統計資訊
    logger.info("\n" + "=" * 80)
    logger.info("📊 匯入統計報告")
    logger.info("=" * 80)
    logger.info(f"匯入時間: {start_time.strftime('%Y-%m-%d %H:%M:%S')} - {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"總耗時: {duration}")
    logger.info(f"使用執行緒數: {max_workers}")
    logger.info(f"總檔案數: {total_files}")
    logger.info(f"成功檔案數: {successful_files}")
    logger.info(f"失敗檔案數: {failed_files}")
    logger.info(f"成功率: {success_rate:.1f}%")
    logger.info(f"平均處理時間: {avg_processing_time:.2f}秒/檔案")
    logger.info(f"總記錄數: {total_records:,}")
    
    logger.info("\n各資料夾統計:")
    for folder, stats in folder_stats.items():
        folder_success_rate = (stats['successful_files'] / stats['total_files'] * 100) if stats['total_files'] > 0 else 0
        logger.info(f"  {folder}: {stats['successful_files']}/{stats['total_files']} ({folder_success_rate:.1f}%)")
    
    # 保存統計到檔案
    stats_file = f"new_folders_import_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    with open(stats_file, 'w', encoding='utf-8') as f:
        f.write("新資料夾匯入統計報告\n")
        f.write(f"匯入時間: {start_time.strftime('%Y-%m-%d %H:%M:%S')} - {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"總耗時: {duration}\n")
        f.write(f"使用執行緒數: {max_workers}\n")
        f.write(f"總檔案數: {total_files}\n")
        f.write(f"成功檔案數: {successful_files}\n")
        f.write(f"失敗檔案數: {failed_files}\n")
        f.write(f"成功率: {success_rate:.1f}%\n")
        f.write(f"平均處理時間: {avg_processing_time:.2f}秒/檔案\n")
        f.write(f"總記錄數: {total_records:,}\n\n")
        f.write("各資料夾統計:\n")
        for folder, stats in folder_stats.items():
            folder_success_rate = (stats['successful_files'] / stats['total_files'] * 100) if stats['total_files'] > 0 else 0
            f.write(f"  {folder}: {stats['successful_files']}/{stats['total_files']} ({folder_success_rate:.1f}%)\n")
    
    logger.info(f"📄 統計報告已保存到: {stats_file}")
    
    # 返回成功匯入的資料夾列表，用於更新 config.py
    successfully_imported_folders = [
        folder for folder, stats in folder_stats.items()
        if stats['successful_files'] > 0  # 至少有一個檔案成功匯入
    ]
    
    return successfully_imported_folders

def update_config_file(new_folders: List[str]) -> bool:
    """
    更新 config.py 檔案，將新資料夾加入到 DATA_FOLDERS 中
    
    Args:
        new_folders: 要加入的新資料夾列表
    
    Returns:
        是否成功更新
    """
    if not new_folders:
        return False
    
    config_file_path = 'config.py'
    
    try:
        # 讀取現有的 config.py
        with open(config_file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 讀取現有的 DATA_FOLDERS
        from config import DATA_FOLDERS as existing_folders
        
        # 合併現有和新資料夾，去除重複並排序
        all_folders = sorted(list(set(existing_folders + new_folders)))
        
        # 建立新的 DATA_FOLDERS 列表字串
        # 將資料夾分組：季度格式（如 113Q1）和日期格式（如 20250511）
        quarter_folders = sorted([f for f in all_folders if re.match(r'^\d{3}Q[1-4]$', f)])
        date_folders = sorted([f for f in all_folders if re.match(r'^\d{8}$', f)])
        other_folders = sorted([f for f in all_folders if f not in quarter_folders and f not in date_folders])
        
        # 建立新的 DATA_FOLDERS 定義
        folder_lines = []
        if quarter_folders:
            quarter_str = ", ".join([f"'{f}'" for f in quarter_folders])
            folder_lines.append(f"    {quarter_str},  # 季度資料夾")
        
        if date_folders:
            date_str = ", ".join([f"'{f}'" for f in date_folders])
            folder_lines.append(f"    {date_str},  # 日期資料夾")
        
        if other_folders:
            other_str = ", ".join([f"'{f}'" for f in other_folders])
            folder_lines.append(f"    {other_str},  # 其他資料夾")
        
        # 移除最後一行的逗號
        if folder_lines:
            folder_lines[-1] = folder_lines[-1].rstrip(',')
        
        new_data_folders_str = "[\n" + "\n".join(folder_lines) + "\n]"
        
        # 使用正則表達式替換 DATA_FOLDERS 定義
        # 匹配 DATA_FOLDERS = [...] 的整個區塊（支援多行）
        pattern = r'DATA_FOLDERS\s*=\s*\[.*?\]'
        replacement = f'DATA_FOLDERS = {new_data_folders_str}'
        
        new_content = re.sub(pattern, replacement, content, flags=re.DOTALL)
        
        # 寫回檔案
        with open(config_file_path, 'w', encoding='utf-8') as f:
            f.write(new_content)
        
        logger.info(f"✅ 已更新 config.py，新增 {len(new_folders)} 個資料夾到 DATA_FOLDERS")
        logger.info(f"📝 新增的資料夾: {', '.join(new_folders)}")
        logger.info(f"📊 目前 DATA_FOLDERS 總數: {len(all_folders)} 個資料夾")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 更新 config.py 失敗: {str(e)}")
        return False

def main(auto_mode: bool = False):
    """
    主函數
    
    Args:
        auto_mode: 是否為自動模式（帶參數 1 時為 True，跳過所有交互式輸入）
    """
    print("=" * 80)
    print("🔍 自動掃描新資料夾並匯入")
    print("=" * 80)
    
    # 掃描新資料夾
    print("\n正在掃描新資料夾...")
    new_folders = scan_new_folders()
    
    if not new_folders:
        print("✅ 沒有發現新資料夾")
        print(f"\n目前 config.py 中已定義的資料夾: {', '.join(DATA_FOLDERS)}")
        return
    
    print(f"\n📂 發現 {len(new_folders)} 個新資料夾:")
    for i, folder in enumerate(new_folders, 1):
        csv_count = len(glob.glob(os.path.join(folder, "*.csv")))
        print(f"  {i}. {folder} ({csv_count} 個CSV檔案)")
    
    # 根據模式選擇執行方式
    if auto_mode:
        # 自動模式：直接使用自動設定執行緒數
        max_workers = MAX_WORKERS
        print(f"\n🤖 自動模式：使用 {max_workers} 個執行緒")
        print(f"⚠️ 即將開始匯入 {len(new_folders)} 個新資料夾...")
    else:
        # 交互模式：詢問用戶
        print("\n選擇執行模式:")
        print("1. 自動設定執行緒數 (建議)")
        print("2. 手動設定執行緒數")
        print("3. 取消")
        
        choice = input("\n請選擇 (1/2/3): ").strip()
        
        if choice == "3":
            print("❌ 已取消")
            return
        
        if choice == "2":
            try:
                max_workers = int(input("請輸入執行緒數 (建議 2-8): ").strip())
            except ValueError:
                max_workers = MAX_WORKERS
                print(f"使用預設值: {max_workers}")
        else:
            max_workers = MAX_WORKERS
            print(f"自動設定執行緒數: {max_workers}")
        
        # 確認匯入
        print(f"\n⚠️ 即將開始匯入 {len(new_folders)} 個新資料夾，使用 {max_workers} 個執行緒...")
        confirm = input("確定要繼續嗎? (y/N): ").strip().lower()
        
        if confirm != 'y':
            print("❌ 匯入已取消")
            return
    
    # 執行匯入
    successfully_imported = import_new_folders(new_folders, max_workers=max_workers)
    print("\n✅ 新資料夾匯入完成!")
    
    # 處理 config.py 更新
    if successfully_imported:
        print(f"\n📝 已成功匯入 {len(successfully_imported)} 個資料夾")
        
        if auto_mode:
            # 自動模式：直接更新 config.py
            print("🤖 自動模式：自動更新 config.py...")
            if update_config_file(successfully_imported):
                print("✅ config.py 已更新完成！")
            else:
                print("❌ config.py 更新失敗，請手動更新")
        else:
            # 交互模式：詢問是否更新
            print("是否要更新 config.py，將這些資料夾加入到 DATA_FOLDERS 中？")
            print("(這樣下次執行時就不會重複匯入這些資料夾)")
            update_confirm = input("更新 config.py? (Y/n): ").strip().lower()
            
            if update_confirm != 'n':
                if update_config_file(successfully_imported):
                    print("✅ config.py 已更新完成！")
                else:
                    print("❌ config.py 更新失敗，請手動更新")
            else:
                print("⏭️  已跳過更新 config.py")
    else:
        print("⚠️  沒有成功匯入任何資料夾，不更新 config.py")

if __name__ == "__main__":
    # 檢查命令行參數
    auto_mode = False
    if len(sys.argv) > 1:
        if sys.argv[1] == "1":
            auto_mode = True
        else:
            print(f"❌ 未知的參數: {sys.argv[1]}")
            print("用法: python import_new_folders.py [1]")
            print("  1: 自動模式（自動設定執行緒數，無需交互）")
            sys.exit(1)
    
    main(auto_mode=auto_mode)

