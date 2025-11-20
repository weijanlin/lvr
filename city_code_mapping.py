# -*- coding: utf-8 -*-
"""
縣市代碼對應表
將檔案第1碼轉換為縣市名稱
"""

from typing import Dict, Optional

class CityCodeMapping:
    """縣市代碼對應表"""
    
    def __init__(self):
        # 縣市代碼對應表
        self.city_code_mapping = {
            'a': '臺北市',
            'b': '臺中市', 
            'c': '基隆市',
            'd': '臺南市',
            'e': '高雄市',
            'f': '新北市',
            'g': '宜蘭縣',
            'h': '桃園市',
            'i': '嘉義市',
            'j': '新竹縣',
            'k': '苗栗縣',
            'm': '南投縣',
            'n': '彰化縣',
            'o': '新竹市',
            'p': '雲林縣',
            'q': '嘉義縣',
            't': '屏東縣',
            'u': '花蓮縣',
            'v': '臺東縣',
            'w': '金門縣',
            'x': '澎湖縣',
            'z': '連江縣'
        }
        
        # 反向對應表（縣市名稱 → 代碼）
        self.city_name_mapping = {v: k for k, v in self.city_code_mapping.items()}
    
    def get_city_name(self, city_code: str) -> Optional[str]:
        """根據縣市代碼取得縣市名稱"""
        return self.city_code_mapping.get(city_code.lower(), None)
    
    def get_city_code(self, city_name: str) -> Optional[str]:
        """根據縣市名稱取得縣市代碼"""
        return self.city_name_mapping.get(city_name, None)
    
    def extract_city_code_from_filename(self, filename: str) -> Optional[str]:
        """從檔案名稱中提取縣市代碼"""
        if not filename:
            return None
        
        # 取得檔案名稱（不含路徑）
        basename = filename.split('/')[-1].split('\\')[-1]
        
        # 檢查是否以縣市代碼開頭，且後面跟著下底線
        if len(basename) > 1 and basename[1] == '_':
            first_char = basename[0].lower()
            if first_char in self.city_code_mapping:
                return first_char
        
        return None
    
    def get_city_info_from_filename(self, filename: str) -> Optional[Dict[str, str]]:
        """從檔案名稱中取得縣市資訊"""
        city_code = self.extract_city_code_from_filename(filename)
        if city_code:
            city_name = self.get_city_name(city_code)
            return {
                'city_code': city_code,
                'city_name': city_name
            }
        return None
    
    def is_valid_city_code(self, city_code: str) -> bool:
        """檢查縣市代碼是否有效"""
        return city_code.lower() in self.city_code_mapping
    
    def get_all_city_codes(self) -> list:
        """取得所有縣市代碼"""
        return list(self.city_code_mapping.keys())
    
    def get_all_city_names(self) -> list:
        """取得所有縣市名稱"""
        return list(self.city_code_mapping.values())
    
    def print_mapping_table(self):
        """列印對應表"""
        print("🏙️ 縣市代碼對應表")
        print("=" * 50)
        print(f"{'代碼':<4} {'縣市名稱':<10}")
        print("-" * 20)
        
        for code, name in sorted(self.city_code_mapping.items()):
            print(f"{code:<4} {name:<10}")
        
        print(f"\n總計: {len(self.city_code_mapping)} 個縣市")

def test_city_code_mapping():
    """測試縣市代碼對應表"""
    print("\n🧪 測試縣市代碼對應表")
    print("=" * 50)
    
    mapping = CityCodeMapping()
    
    # 測試檔案名稱
    test_files = [
        'a_lvr_land_a.csv',
        'b_lvr_land_b.csv', 
        'c_lvr_land_c.csv',
        'f_lvr_land_a_build.csv',
        'h_lvr_land_b_land.csv',
        'unknown_file.csv'
    ]
    
    print("\n📁 檔案名稱解析測試:")
    print("-" * 50)
    
    for filename in test_files:
        city_info = mapping.get_city_info_from_filename(filename)
        if city_info:
            print(f"✅ {filename:<25} → {city_info['city_code']} ({city_info['city_name']})")
        else:
            print(f"❌ {filename:<25} → 無法識別縣市代碼")
    
    # 測試代碼轉換
    print(f"\n🔄 代碼轉換測試:")
    print("-" * 50)
    
    test_codes = ['a', 'b', 'f', 'h', 'x', 'z']
    for code in test_codes:
        city_name = mapping.get_city_name(code)
        print(f"代碼 '{code}' → {city_name}")
    
    # 測試縣市名稱轉換
    print(f"\n🔄 縣市名稱轉換測試:")
    print("-" * 50)
    
    test_names = ['臺北市', '新北市', '桃園市', '高雄市']
    for name in test_names:
        city_code = mapping.get_city_code(name)
        print(f"縣市 '{name}' → 代碼 '{city_code}'")

if __name__ == "__main__":
    mapping = CityCodeMapping()
    mapping.print_mapping_table()
    test_city_code_mapping()
