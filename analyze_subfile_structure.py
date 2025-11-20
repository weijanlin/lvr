# -*- coding: utf-8 -*-
"""
分析子檔案結構
檢查_build.csv, _land.csv, _park.csv的欄位
"""

import pandas as pd
import os
import glob
from typing import Dict, List, Set

def analyze_subfile_structure():
    """分析子檔案結構"""
    print("🔍 分析子檔案結構")
    print("=" * 80)
    
    # 分析113Q1資料夾中的子檔案
    folder_path = "113Q1"
    
    if not os.path.exists(folder_path):
        print(f"❌ 資料夾不存在: {folder_path}")
        return
    
    # 分析不同類型的子檔案
    subfile_types = {
        '建物檔案': '_build.csv',
        '土地檔案': '_land.csv', 
        '停車場檔案': '_park.csv'
    }
    
    results = {}
    
    for subfile_type, suffix in subfile_types.items():
        print(f"\n📊 分析 {subfile_type} ({suffix})")
        print("-" * 60)
        
        # 尋找匹配的檔案
        pattern = os.path.join(folder_path, f"*{suffix}")
        files = glob.glob(pattern)
        
        if not files:
            print(f"⚠️ 未找到 {subfile_type} 檔案")
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
            results[subfile_type] = {
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
            results[subfile_type] = {
                'file': sample_file,
                'error': str(e)
            }
    
    # 分析不同資料類型的子檔案差異
    print(f"\n🔍 不同資料類型的子檔案分析")
    print("=" * 80)
    
    # 分析中古屋、預售屋、租屋的子檔案
    data_types = {
        '中古屋': '_a',
        '預售屋': '_b',
        '租屋': '_c'
    }
    
    for data_type, prefix in data_types.items():
        print(f"\n📊 {data_type} 子檔案分析:")
        print("-" * 60)
        
        for subfile_type, suffix in subfile_types.items():
            pattern = os.path.join(folder_path, f"*{prefix}{suffix}")
            files = glob.glob(pattern)
            
            if files:
                sample_file = files[0]
                print(f"\n🔸 {subfile_type}:")
                print(f"   檔案: {os.path.basename(sample_file)}")
                
                try:
                    df = pd.read_csv(sample_file, encoding='utf-8')
                    print(f"   行數: {len(df)}")
                    print(f"   欄位數: {len(df.columns)}")
                    print(f"   欄位: {', '.join(df.columns)}")
                except Exception as e:
                    print(f"   ❌ 讀取失敗: {str(e)}")
            else:
                print(f"\n🔸 {subfile_type}: 未找到檔案")
    
    # 比較不同資料類型的子檔案欄位差異
    print(f"\n🔍 子檔案欄位差異分析")
    print("=" * 80)
    
    # 收集所有子檔案的欄位
    all_columns = {}
    
    for data_type, prefix in data_types.items():
        all_columns[data_type] = {}
        
        for subfile_type, suffix in subfile_types.items():
            pattern = os.path.join(folder_path, f"*{prefix}{suffix}")
            files = glob.glob(pattern)
            
            if files:
                try:
                    df = pd.read_csv(files[0], encoding='utf-8')
                    all_columns[data_type][subfile_type] = set(df.columns)
                except:
                    all_columns[data_type][subfile_type] = set()
            else:
                all_columns[data_type][subfile_type] = set()
    
    # 分析欄位差異
    for subfile_type in subfile_types.keys():
        print(f"\n📋 {subfile_type} 欄位比較:")
        print("-" * 40)
        
        # 收集所有資料類型的欄位
        columns_by_type = {}
        for data_type in data_types.keys():
            if subfile_type in all_columns[data_type]:
                columns_by_type[data_type] = all_columns[data_type][subfile_type]
        
        if len(columns_by_type) >= 2:
            # 找出共同欄位
            common_columns = set.intersection(*columns_by_type.values())
            print(f"✅ 共同欄位 ({len(common_columns)} 個):")
            for col in sorted(common_columns):
                print(f"   - {col}")
            
            # 找出各類型獨有的欄位
            for data_type, columns in columns_by_type.items():
                unique_columns = columns - common_columns
                if unique_columns:
                    print(f"\n🔸 {data_type} 獨有欄位 ({len(unique_columns)} 個):")
                    for col in sorted(unique_columns):
                        print(f"   - {col}")
    
    # 生成子檔案資料表結構建議
    print(f"\n💡 子檔案資料表結構建議")
    print("=" * 80)
    
    for subfile_type, suffix in subfile_types.items():
        if subfile_type in results and 'column_list' in results[subfile_type]:
            print(f"\n📊 {subfile_type} 資料表結構:")
            print(f"CREATE TABLE {subfile_type.lower().replace('檔案', '')}_data (")
            
            columns = results[subfile_type]['column_list']
            for i, col in enumerate(columns):
                # 根據欄位名稱推測資料類型
                if any(keyword in col for keyword in ['面積', '價格', '總價', '單價', '金額', '持分']):
                    data_type = "DECIMAL(15,2)"
                elif any(keyword in col for keyword in ['數量', '筆數', '層數', '房', '廳', '衛', '屋齡']):
                    data_type = "INT"
                elif any(keyword in col for keyword in ['年月日', '日期']):
                    data_type = "NVARCHAR(20)"
                else:
                    data_type = "NVARCHAR(200)"
                
                comma = "," if i < len(columns) - 1 else ""
                print(f"    {col} {data_type}{comma}")
            
            print(");")
    
    return results

def analyze_specific_subfiles():
    """分析特定的子檔案"""
    print(f"\n🔍 分析特定子檔案")
    print("=" * 80)
    
    # 分析特定檔案
    specific_files = [
        "113Q1/a_lvr_land_a_build.csv",  # 中古屋建物
        "113Q1/a_lvr_land_a_land.csv",   # 中古屋土地
        "113Q1/a_lvr_land_a_park.csv",   # 中古屋停車場
        "113Q1/a_lvr_land_b_land.csv",   # 預售屋土地
        "113Q1/a_lvr_land_b_park.csv",   # 預售屋停車場
        "113Q1/a_lvr_land_c_build.csv",  # 租屋建物
        "113Q1/a_lvr_land_c_land.csv",   # 租屋土地
        "113Q1/a_lvr_land_c_park.csv",   # 租屋停車場
    ]
    
    for file_path in specific_files:
        if os.path.exists(file_path):
            print(f"\n📄 分析檔案: {os.path.basename(file_path)}")
            print("-" * 60)
            
            try:
                df = pd.read_csv(file_path, encoding='utf-8')
                print(f"   行數: {len(df)}")
                print(f"   欄位數: {len(df.columns)}")
                print(f"   欄位: {', '.join(df.columns)}")
                
                # 顯示資料樣本
                if len(df) > 0:
                    print(f"   樣本資料:")
                    for i in range(min(2, len(df))):
                        sample_data = dict(df.iloc[i])
                        # 只顯示前5個欄位
                        sample_keys = list(sample_data.keys())[:5]
                        sample_dict = {k: sample_data[k] for k in sample_keys}
                        print(f"     行 {i+1}: {sample_dict}")
                
            except Exception as e:
                print(f"   ❌ 讀取失敗: {str(e)}")
        else:
            print(f"\n❌ 檔案不存在: {file_path}")

def save_analysis_results(results: Dict):
    """儲存分析結果到檔案"""
    try:
        with open('subfile_structure_analysis.txt', 'w', encoding='utf-8') as f:
            f.write("子檔案結構分析結果\n")
            f.write("=" * 50 + "\n\n")
            
            for subfile_type, data in results.items():
                f.write(f"{subfile_type}:\n")
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
        
        print(f"\n💾 分析結果已儲存到: subfile_structure_analysis.txt")
        
    except Exception as e:
        print(f"❌ 儲存分析結果失敗: {str(e)}")

if __name__ == "__main__":
    results = analyze_subfile_structure()
    analyze_specific_subfiles()
    save_analysis_results(results)




