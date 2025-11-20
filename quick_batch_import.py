# -*- coding: utf-8 -*-
"""
快速批次匯入腳本
用於快速匯入所有資料夾的CSV檔案
"""

import os
import glob
import logging
from datetime import datetime
from tqdm import tqdm

from config import DATA_FOLDERS
from enhanced_data_importer import EnhancedDataImporter

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('quick_batch_import.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def quick_import_all():
    """快速匯入所有資料夾"""
    print("🚀 快速批次匯入所有資料夾")
    print("=" * 80)
    
    importer = EnhancedDataImporter()
    
    # 統計資訊
    total_files = 0
    successful_files = 0
    failed_files = 0
    total_records = 0
    start_time = datetime.now()
    
    # 掃描所有檔案
    all_files = []
    for folder in DATA_FOLDERS:
        if os.path.exists(folder):
            csv_files = glob.glob(os.path.join(folder, "*.csv"))
            all_files.extend(csv_files)
            print(f"📁 {folder}: {len(csv_files)} 個CSV檔案")
        else:
            print(f"❌ 資料夾不存在: {folder}")
    
    total_files = len(all_files)
    print(f"\n📊 總計: {total_files} 個CSV檔案")
    
    if total_files == 0:
        print("❌ 沒有找到任何CSV檔案")
        return
    
    # 開始匯入
    print(f"\n🚀 開始匯入...")
    start_time = datetime.now()
    
    with tqdm(all_files, desc="匯入進度", unit="檔案") as pbar:
        for file_path in all_files:
            filename = os.path.basename(file_path)
            folder = os.path.dirname(file_path)
            
            pbar.set_postfix({
                '成功': successful_files,
                '失敗': failed_files,
                '記錄': f"{total_records:,}"
            })
            
            try:
                success = importer.import_single_file(file_path, folder)
                
                if success:
                    successful_files += 1
                    logger.info(f"✅ {filename} 匯入成功")
                else:
                    failed_files += 1
                    logger.error(f"❌ {filename} 匯入失敗")
                
            except Exception as e:
                failed_files += 1
                logger.error(f"❌ {filename} 處理異常: {str(e)}")
            
            pbar.update(1)
    
    # 計算統計
    end_time = datetime.now()
    duration = end_time - start_time
    success_rate = (successful_files / total_files * 100) if total_files > 0 else 0
    
    # 顯示結果
    print(f"\n✅ 批次匯入完成!")
    print("=" * 80)
    print(f"⏱️  總耗時: {duration}")
    print(f"📁 總檔案數: {total_files}")
    print(f"✅ 成功檔案數: {successful_files}")
    print(f"❌ 失敗檔案數: {failed_files}")
    print(f"📊 成功率: {success_rate:.1f}%")
    print(f"📝 總記錄數: {total_records:,}")
    
    # 保存統計到檔案
    stats_file = f"quick_import_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    with open(stats_file, 'w', encoding='utf-8') as f:
        f.write(f"快速批次匯入統計報告\n")
        f.write(f"匯入時間: {start_time.strftime('%Y-%m-%d %H:%M:%S')} - {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"總耗時: {duration}\n")
        f.write(f"總檔案數: {total_files}\n")
        f.write(f"成功檔案數: {successful_files}\n")
        f.write(f"失敗檔案數: {failed_files}\n")
        f.write(f"成功率: {success_rate:.1f}%\n")
        f.write(f"總記錄數: {total_records:,}\n")
    
    print(f"📄 統計報告已保存到: {stats_file}")

def main():
    """主函數"""
    print("選擇執行模式:")
    print("1. 快速匯入所有檔案")
    print("2. 只匯入第一個資料夾 (測試)")
    
    choice = input("請選擇 (1/2): ").strip()
    
    if choice == "1":
        quick_import_all()
    elif choice == "2":
        # 只匯入第一個資料夾
        if DATA_FOLDERS and os.path.exists(DATA_FOLDERS[0]):
            folder = DATA_FOLDERS[0]
            csv_files = glob.glob(os.path.join(folder, "*.csv"))
            print(f"📁 測試資料夾: {folder} ({len(csv_files)} 個檔案)")
            
            importer = EnhancedDataImporter()
            successful = 0
            failed = 0
            
            for file_path in csv_files[:5]:  # 只處理前5個檔案
                filename = os.path.basename(file_path)
                try:
                    success = importer.import_single_file(file_path, folder)
                    if success:
                        successful += 1
                        print(f"✅ {filename}")
                    else:
                        failed += 1
                        print(f"❌ {filename}")
                except Exception as e:
                    failed += 1
                    print(f"❌ {filename}: {str(e)}")
            
            print(f"\n測試結果: 成功 {successful}, 失敗 {failed}")
        else:
            print("❌ 沒有可用的測試資料夾")
    else:
        print("❌ 無效的選擇")

if __name__ == "__main__":
    main()