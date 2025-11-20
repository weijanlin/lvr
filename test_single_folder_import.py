# -*- coding: utf-8 -*-
"""
測試單一資料夾匯入功能
"""

from data_importer import DataImporter
import os

def test_single_folder_import():
    """測試單一資料夾匯入功能"""
    print("🚀 測試單一資料夾匯入功能")
    print("=" * 60)
    
    # 建立資料匯入器
    importer = DataImporter()
    
    # 選擇第一個資料夾進行測試
    test_folder = "113Q1"
    
    if not os.path.exists(test_folder):
        print(f"❌ 測試資料夾不存在: {test_folder}")
        return
    
    print(f"📁 開始測試資料夾: {test_folder}")
    print("-" * 40)
    
    try:
        # 執行單一資料夾匯入
        stats = importer.import_single_folder(test_folder)
        
        print(f"\n✅ 匯入測試完成！")
        print(f"📊 統計結果:")
        print(f"   總檔案數: {stats.get('total_files', 0)}")
        print(f"   成功檔案數: {stats.get('success_files', 0)}")
        print(f"   失敗檔案數: {stats.get('failed_files', 0)}")
        print(f"   總資料行數: {stats.get('total_rows', 0)}")
        
        # 判斷測試結果
        if stats.get('success_files', 0) > 0:
            print(f"\n🎉 測試成功！成功匯入了 {stats['success_files']} 個檔案")
        else:
            print(f"\n⚠️ 測試結果：沒有成功匯入任何檔案")
            
    except Exception as e:
        print(f"❌ 測試過程中發生錯誤: {str(e)}")

if __name__ == "__main__":
    test_single_folder_import()
