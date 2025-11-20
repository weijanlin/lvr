# -*- coding: utf-8 -*-
"""
測試並行處理效能
比較順序處理和並行處理的效能差異
"""

import os
import glob
import time
from datetime import datetime
from typing import List

from config import DATA_FOLDERS
from batch_importer import BatchImporter
from parallel_batch_importer import ParallelBatchImporter

def test_performance_comparison():
    """測試效能比較"""
    print("🧪 並行處理效能測試")
    print("=" * 80)
    
    # 選擇測試資料夾（使用第一個可用的資料夾）
    test_folder = None
    test_files = []
    
    for folder in DATA_FOLDERS:
        if os.path.exists(folder):
            csv_files = glob.glob(os.path.join(folder, "*.csv"))
            # 只取前10個檔案進行測試
            test_files = csv_files[:10]
            test_folder = folder
            break
    
    if not test_files:
        print("❌ 沒有找到可用的測試檔案")
        return
    
    print(f"📁 測試資料夾: {test_folder}")
    print(f"📄 測試檔案數: {len(test_files)}")
    print(f"📝 測試檔案列表:")
    for i, file_path in enumerate(test_files, 1):
        filename = os.path.basename(file_path)
        print(f"  {i}. {filename}")
    
    # 測試1: 順序處理
    print(f"\n🔄 測試1: 順序處理")
    print("-" * 40)
    
    sequential_importer = BatchImporter()
    sequential_start = time.time()
    
    # 模擬順序處理（只測試檔案掃描和分析）
    sequential_files = {test_folder: test_files}
    sequential_analysis = sequential_importer.analyze_files(sequential_files)
    
    sequential_end = time.time()
    sequential_time = sequential_end - sequential_start
    
    print(f"⏱️  順序處理時間: {sequential_time:.2f}秒")
    print(f"📊 分析結果: {sequential_analysis['total_files']} 個檔案")
    
    # 測試2: 並行處理
    print(f"\n🔄 測試2: 並行處理 (4個執行緒)")
    print("-" * 40)
    
    parallel_importer = ParallelBatchImporter(max_workers=4)
    parallel_start = time.time()
    
    # 模擬並行處理（只測試檔案掃描和分析）
    parallel_files = {test_folder: test_files}
    parallel_analysis = parallel_importer.analyze_files(parallel_files)
    
    parallel_end = time.time()
    parallel_time = parallel_end - parallel_start
    
    print(f"⏱️  並行處理時間: {parallel_time:.2f}秒")
    print(f"📊 分析結果: {parallel_analysis['total_files']} 個檔案")
    
    # 測試3: 高並行處理
    print(f"\n🔄 測試3: 高並行處理 (8個執行緒)")
    print("-" * 40)
    
    high_parallel_importer = ParallelBatchImporter(max_workers=8)
    high_parallel_start = time.time()
    
    # 模擬高並行處理
    high_parallel_files = {test_folder: test_files}
    high_parallel_analysis = high_parallel_importer.analyze_files(high_parallel_files)
    
    high_parallel_end = time.time()
    high_parallel_time = high_parallel_end - high_parallel_start
    
    print(f"⏱️  高並行處理時間: {high_parallel_time:.2f}秒")
    print(f"📊 分析結果: {high_parallel_analysis['total_files']} 個檔案")
    
    # 效能比較
    print(f"\n📊 效能比較結果")
    print("=" * 80)
    
    print(f"順序處理時間:     {sequential_time:.2f}秒")
    print(f"並行處理時間:     {parallel_time:.2f}秒")
    print(f"高並行處理時間:   {high_parallel_time:.2f}秒")
    
    if sequential_time > 0:
        speedup_4 = sequential_time / parallel_time if parallel_time > 0 else 0
        speedup_8 = sequential_time / high_parallel_time if high_parallel_time > 0 else 0
        
        print(f"\n🚀 效能提升:")
        print(f"4執行緒加速比:   {speedup_4:.2f}x")
        print(f"8執行緒加速比:   {speedup_8:.2f}x")
        
        if speedup_4 > 1:
            print(f"✅ 4執行緒並行處理比順序處理快 {speedup_4:.2f} 倍")
        else:
            print(f"⚠️  4執行緒並行處理沒有明顯提升")
        
        if speedup_8 > speedup_4:
            print(f"✅ 8執行緒比4執行緒更快")
        elif speedup_8 > 1:
            print(f"✅ 8執行緒並行處理比順序處理快 {speedup_8:.2f} 倍")
        else:
            print(f"⚠️  8執行緒並行處理沒有明顯提升")
    
    # 建議
    print(f"\n💡 建議:")
    if parallel_time < sequential_time:
        print("✅ 建議使用並行處理來提升匯入速度")
        if high_parallel_time < parallel_time:
            print("✅ 建議使用8個執行緒進行高並行處理")
        else:
            print("✅ 建議使用4個執行緒進行並行處理")
    else:
        print("⚠️  對於小量檔案，順序處理可能更適合")
        print("💡 建議對大量檔案使用並行處理")

def test_small_batch_import():
    """測試小批次匯入"""
    print(f"\n🧪 小批次匯入測試")
    print("=" * 80)
    
    # 選擇測試資料夾
    test_folder = None
    test_files = []
    
    for folder in DATA_FOLDERS:
        if os.path.exists(folder):
            csv_files = glob.glob(os.path.join(folder, "*.csv"))
            # 只取前5個檔案進行實際匯入測試
            test_files = csv_files[:5]
            test_folder = folder
            break
    
    if not test_files:
        print("❌ 沒有找到可用的測試檔案")
        return
    
    print(f"📁 測試資料夾: {test_folder}")
    print(f"📄 測試檔案數: {len(test_files)}")
    
    # 詢問是否執行實際匯入測試
    print(f"\n❓ 是否要執行實際匯入測試?")
    print("⚠️  注意: 這將匯入檔案到資料庫")
    choice = input("輸入 'yes' 確認執行: ").strip().lower()
    
    if choice == 'yes':
        # 測試並行匯入
        print(f"\n🚀 開始並行匯入測試...")
        
        parallel_importer = ParallelBatchImporter(max_workers=4)
        start_time = time.time()
        
        # 只匯入測試檔案
        folder_stats = parallel_importer.import_folder_parallel(test_folder, test_files)
        
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"\n✅ 並行匯入測試完成!")
        print(f"⏱️  總耗時: {duration:.2f}秒")
        print(f"📄 成功檔案數: {folder_stats['successful_files']}")
        print(f"❌ 失敗檔案數: {folder_stats['failed_files']}")
        print(f"📊 成功率: {(folder_stats['successful_files'] / folder_stats['total_files'] * 100):.1f}%")
        print(f"⏱️  平均處理時間: {folder_stats['avg_processing_time']:.2f}秒/檔案")
        
        if folder_stats['errors']:
            print(f"\n❌ 錯誤列表:")
            for error in folder_stats['errors']:
                print(f"  - {error}")
    else:
        print("❌ 實際匯入測試已取消")

def main():
    """主函數"""
    print("選擇測試模式:")
    print("1. 效能比較測試 (只分析檔案)")
    print("2. 小批次匯入測試 (實際匯入)")
    print("3. 完整測試 (效能比較 + 小批次匯入)")
    
    choice = input("請選擇 (1/2/3): ").strip()
    
    if choice == "1":
        test_performance_comparison()
    elif choice == "2":
        test_small_batch_import()
    elif choice == "3":
        test_performance_comparison()
        test_small_batch_import()
    else:
        print("❌ 無效的選擇")

if __name__ == "__main__":
    main()




