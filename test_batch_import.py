# -*- coding: utf-8 -*-
"""
測試批次匯入功能
驗證批次匯入系統的運作
"""

import os
import glob
from datetime import datetime

from config import DATA_FOLDERS
from batch_importer import BatchImporter

def test_batch_import():
    """測試批次匯入功能"""
    print("🧪 測試批次匯入功能")
    print("=" * 80)
    
    # 檢查資料夾
    print("📁 檢查資料夾:")
    available_folders = []
    for folder in DATA_FOLDERS:
        if os.path.exists(folder):
            csv_files = glob.glob(os.path.join(folder, "*.csv"))
            print(f"  ✅ {folder}: {len(csv_files)} 個CSV檔案")
            available_folders.append(folder)
        else:
            print(f"  ❌ {folder}: 資料夾不存在")
    
    if not available_folders:
        print("❌ 沒有可用的資料夾")
        return
    
    # 建立批次匯入器
    importer = BatchImporter()
    
    # 測試掃描功能
    print(f"\n🔍 測試檔案掃描功能:")
    all_files = importer.scan_all_folders()
    
    total_files = sum(len(files) for files in all_files.values())
    print(f"  總檔案數: {total_files}")
    
    # 測試分析功能
    print(f"\n📊 測試檔案分析功能:")
    analysis = importer.analyze_files(all_files)
    
    print(f"  檔案類型分布:")
    for file_type, count in analysis['file_types'].items():
        print(f"    {file_type}: {count} 個檔案")
    
    print(f"  縣市分布:")
    for city, count in analysis['city_distribution'].items():
        print(f"    {city}: {count} 個檔案")
    
    # 測試乾跑模式
    print(f"\n🔍 測試乾跑模式:")
    result = importer.import_all_folders(dry_run=True)
    
    print(f"  乾跑結果:")
    print(f"    總檔案數: {result['analysis']['total_files']}")
    print(f"    檔案類型: {len(result['analysis']['file_types'])} 種")
    print(f"    縣市數: {len(result['analysis']['city_distribution'])} 個")
    
    # 詢問是否執行實際匯入測試
    print(f"\n❓ 是否要執行實際匯入測試?")
    print("⚠️  注意: 這將匯入所有檔案到資料庫")
    choice = input("輸入 'yes' 確認執行: ").strip().lower()
    
    if choice == 'yes':
        print(f"\n🚀 開始實際匯入測試...")
        start_time = datetime.now()
        
        result = importer.import_all_folders(dry_run=False)
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        print(f"\n✅ 實際匯入測試完成!")
        print(f"⏱️  總耗時: {duration}")
        print(f"📁 總資料夾數: {result['total_folders']}")
        print(f"📄 總檔案數: {result['total_files']}")
        print(f"✅ 成功檔案數: {result['successful_files']}")
        print(f"❌ 失敗檔案數: {result['failed_files']}")
        print(f"📊 成功率: {(result['successful_files'] / result['total_files'] * 100):.1f}%")
        print(f"📝 總記錄數: {result['total_records']:,}")
        
        # 顯示各資料夾統計
        print(f"\n📊 各資料夾統計:")
        for folder, stats in result['folder_stats'].items():
            success_rate = (stats['successful_files'] / stats['total_files'] * 100) if stats['total_files'] > 0 else 0
            print(f"  {folder}: {stats['successful_files']}/{stats['total_files']} ({success_rate:.1f}%)")
        
        # 顯示錯誤
        if result['failed_files'] > 0:
            print(f"\n❌ 失敗檔案:")
            for folder, stats in result['folder_stats'].items():
                if stats['errors']:
                    print(f"  {folder}:")
                    for error in stats['errors'][:3]:  # 只顯示前3個錯誤
                        print(f"    - {error}")
                    if len(stats['errors']) > 3:
                        print(f"    ... 還有 {len(stats['errors']) - 3} 個錯誤")
    else:
        print("❌ 實際匯入測試已取消")

def main():
    """主函數"""
    test_batch_import()

if __name__ == "__main__":
    main()