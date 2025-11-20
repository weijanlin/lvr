# -*- coding: utf-8 -*-
"""
分析CSV檔案結構
檢查預售屋(_b.csv)和租屋(_c.csv)的實際欄位
"""

import pandas as pd
import os
import glob
from typing import Dict, List, Set

def analyze_csv_structure():
    """分析CSV檔案結構"""
    print("🔍 開始分析CSV檔案結構...")
    print("=" * 80)
    
    # 分析113Q1資料夾中的檔案
    folder_path = "113Q1"
    
    if not os.path.exists(folder_path):
        print(f"❌ 資料夾不存在: {folder_path}")
        return
    
    # 分析不同類型的檔案
    file_types = {
        '中古屋': '_a.csv',
        '預售屋': '_b.csv', 
        '租屋': '_c.csv',
        '中古屋建物': '_a_build.csv',
        '中古屋土地': '_a_land.csv',
        '中古屋停車場': '_a_park.csv',
        '預售屋土地': '_b_land.csv',
        '預售屋停車場': '_b_park.csv',
        '租屋建物': '_c_build.csv',
        '租屋土地': '_c_land.csv',
        '租屋停車場': '_c_park.csv'
    }
    
    results = {}
    
    for file_type, suffix in file_types.items():
        print(f"\n📊 分析 {file_type} 檔案 ({suffix})")
        print("-" * 60)
        
        # 尋找匹配的檔案
        pattern = os.path.join(folder_path, f"*{suffix}")
        files = glob.glob(pattern)
        
        if not files:
            print(f"⚠️ 未找到 {file_type} 檔案")
            continue
        
        # 分析第一個找到的檔案
        sample_file = files[0]
        print(f"📁 分析檔案: {sample_file}")
        
        try:
            # 讀取CSV檔案
            df = pd.read_csv(sample_file, encoding='utf-8')
            
            # 基本資訊
            print(f"   行數: {len(df)}")
            print(f"   欄位數: {len(df.columns)}")
            
            # 欄位列表
            columns = list(df.columns)
            print(f"   欄位列表:")
            for i, col in enumerate(columns, 1):
                print(f"     {i:2d}. {col}")
            
            # 儲存結果
            results[file_type] = {
                'file': sample_file,
                'rows': len(df),
                'columns': len(df.columns),
                'column_list': columns
            }
            
            # 顯示前幾行資料樣本
            print(f"   資料樣本 (前3行):")
            for i in range(min(3, len(df))):
                print(f"     行 {i+1}: {dict(df.iloc[i].head(5))}")
            
        except Exception as e:
            print(f"❌ 讀取檔案失敗: {str(e)}")
            results[file_type] = {
                'file': sample_file,
                'error': str(e)
            }
    
    # 分析欄位差異
    print(f"\n🔍 欄位差異分析")
    print("=" * 80)
    
    # 比較中古屋、預售屋、租屋的欄位差異
    main_types = ['中古屋', '預售屋', '租屋']
    main_columns = {}
    
    for file_type in main_types:
        if file_type in results and 'column_list' in results[file_type]:
            main_columns[file_type] = set(results[file_type]['column_list'])
    
    if len(main_columns) >= 2:
        print("📋 主要檔案類型欄位比較:")
        
        # 找出共同欄位
        common_columns = set.intersection(*main_columns.values())
        print(f"\n✅ 共同欄位 ({len(common_columns)} 個):")
        for col in sorted(common_columns):
            print(f"   - {col}")
        
        # 找出各類型獨有的欄位
        for file_type, columns in main_columns.items():
            unique_columns = columns - common_columns
            if unique_columns:
                print(f"\n🔸 {file_type} 獨有欄位 ({len(unique_columns)} 個):")
                for col in sorted(unique_columns):
                    print(f"   - {col}")
    
    # 生成資料表結構建議
    print(f"\n💡 資料表結構建議")
    print("=" * 80)
    
    for file_type, data in results.items():
        if 'column_list' in data:
            print(f"\n📊 {file_type} 資料表結構:")
            print(f"CREATE TABLE {file_type.lower().replace(' ', '_')}_data (")
            
            for i, col in enumerate(data['column_list']):
                # 根據欄位名稱推測資料類型
                if any(keyword in col for keyword in ['面積', '價格', '總價', '單價', '金額']):
                    data_type = "DECIMAL(15,2)"
                elif any(keyword in col for keyword in ['數量', '筆數', '層數', '房', '廳', '衛']):
                    data_type = "INT"
                elif any(keyword in col for keyword in ['年月日', '日期']):
                    data_type = "NVARCHAR(20)"
                else:
                    data_type = "NVARCHAR(200)"
                
                comma = "," if i < len(data['column_list']) - 1 else ""
                print(f"    {col} {data_type}{comma}")
            
            print(");")
    
    return results

def save_analysis_results(results: Dict):
    """儲存分析結果到檔案"""
    try:
        with open('csv_structure_analysis.txt', 'w', encoding='utf-8') as f:
            f.write("CSV檔案結構分析結果\n")
            f.write("=" * 50 + "\n\n")
            
            for file_type, data in results.items():
                f.write(f"{file_type}:\n")
                f.write(f"  檔案: {data.get('file', 'N/A')}\n")
                f.write(f"  行數: {data.get('rows', 'N/A')}\n")
                f.write(f"  欄位數: {data.get('columns', 'N/A')}\n")
                
                if 'column_list' in data:
                    f.write("  欄位列表:\n")
                    for col in data['column_list']:
                        f.write(f"    - {col}\n")
                
                if 'error' in data:
                    f.write(f"  錯誤: {data['error']}\n")
                
                f.write("\n")
        
        print(f"\n💾 分析結果已儲存到: csv_structure_analysis.txt")
        
    except Exception as e:
        print(f"❌ 儲存分析結果失敗: {str(e)}")

if __name__ == "__main__":
    results = analyze_csv_structure()
    save_analysis_results(results)

