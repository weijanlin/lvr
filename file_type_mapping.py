# -*- coding: utf-8 -*-
"""
檔案類型對應表
支援所有檔案後綴的識別和處理
"""

from typing import Dict, List, Tuple, Optional
from enum import Enum

class DataType(Enum):
    """資料類型枚舉"""
    USED_HOUSE = "used_house"      # 中古屋
    PRESALE = "presale"            # 預售屋
    RENTAL = "rental"              # 租屋

class FileType(Enum):
    """檔案類型枚舉"""
    MAIN = "main"                  # 主要資料檔案
    BUILD = "build"                # 建物資料檔案
    LAND = "land"                  # 土地資料檔案
    PARK = "park"                  # 停車場資料檔案

class FileTypeMapping:
    """檔案類型對應表"""
    
    def __init__(self):
        # 檔案後綴對應表
        self.suffix_mapping = {
            # 中古屋檔案
            '_a.csv': (DataType.USED_HOUSE, FileType.MAIN),
            '_a_build.csv': (DataType.USED_HOUSE, FileType.BUILD),
            '_a_land.csv': (DataType.USED_HOUSE, FileType.LAND),
            '_a_park.csv': (DataType.USED_HOUSE, FileType.PARK),
            
            # 預售屋檔案
            '_b.csv': (DataType.PRESALE, FileType.MAIN),
            '_b_build.csv': (DataType.PRESALE, FileType.BUILD),
            '_b_land.csv': (DataType.PRESALE, FileType.LAND),
            '_b_park.csv': (DataType.PRESALE, FileType.PARK),
            
            # 租屋檔案
            '_c.csv': (DataType.RENTAL, FileType.MAIN),
            '_c_build.csv': (DataType.RENTAL, FileType.BUILD),
            '_c_land.csv': (DataType.RENTAL, FileType.LAND),
            '_c_park.csv': (DataType.RENTAL, FileType.PARK),
        }
        
        # 資料庫對應表
        self.database_mapping = {
            DataType.USED_HOUSE: 'LVR_UsedHouse',
            DataType.PRESALE: 'LVR_PreSale',
            DataType.RENTAL: 'LVR_Rental'
        }
        
        # 資料表對應表
        self.table_mapping = {
            (DataType.USED_HOUSE, FileType.MAIN): 'main_data',
            (DataType.USED_HOUSE, FileType.BUILD): 'build_data',
            (DataType.USED_HOUSE, FileType.LAND): 'land_data',
            (DataType.USED_HOUSE, FileType.PARK): 'park_data',
            
            (DataType.PRESALE, FileType.MAIN): 'presale_data',
            (DataType.PRESALE, FileType.BUILD): 'build_data',
            (DataType.PRESALE, FileType.LAND): 'land_data',
            (DataType.PRESALE, FileType.PARK): 'park_data',
            
            (DataType.RENTAL, FileType.MAIN): 'rental_data',
            (DataType.RENTAL, FileType.BUILD): 'build_data',
            (DataType.RENTAL, FileType.LAND): 'land_data',
            (DataType.RENTAL, FileType.PARK): 'park_data',
        }
        
        # 檔案描述對應表
        self.description_mapping = {
            DataType.USED_HOUSE: "中古屋",
            DataType.PRESALE: "預售屋",
            DataType.RENTAL: "租屋",
            FileType.MAIN: "主要資料",
            FileType.BUILD: "建物資料",
            FileType.LAND: "土地資料",
            FileType.PARK: "停車場資料"
        }

    def get_file_type(self, filename: str) -> Optional[Tuple[DataType, FileType]]:
        """根據檔案名稱取得檔案類型"""
        for suffix, (data_type, file_type) in self.suffix_mapping.items():
            if filename.endswith(suffix):
                return data_type, file_type
        return None

    def get_database_name(self, data_type: DataType) -> str:
        """取得資料庫名稱"""
        return self.database_mapping.get(data_type, '')

    def get_table_name(self, data_type: DataType, file_type: FileType) -> str:
        """取得資料表名稱"""
        return self.table_mapping.get((data_type, file_type), '')

    def get_description(self, data_type: DataType, file_type: FileType) -> str:
        """取得檔案描述"""
        data_desc = self.description_mapping.get(data_type, '')
        file_desc = self.description_mapping.get(file_type, '')
        return f"{data_desc}{file_desc}"

    def get_all_suffixes(self) -> List[str]:
        """取得所有支援的檔案後綴"""
        return list(self.suffix_mapping.keys())

    def get_suffixes_by_data_type(self, data_type: DataType) -> List[str]:
        """根據資料類型取得檔案後綴"""
        suffixes = []
        for suffix, (dt, ft) in self.suffix_mapping.items():
            if dt == data_type:
                suffixes.append(suffix)
        return suffixes

    def get_suffixes_by_file_type(self, file_type: FileType) -> List[str]:
        """根據檔案類型取得檔案後綴"""
        suffixes = []
        for suffix, (dt, ft) in self.suffix_mapping.items():
            if ft == file_type:
                suffixes.append(suffix)
        return suffixes

    def is_supported_file(self, filename: str) -> bool:
        """檢查檔案是否支援"""
        return self.get_file_type(filename) is not None

    def get_file_info(self, filename: str) -> Optional[Dict]:
        """取得檔案完整資訊"""
        file_type_info = self.get_file_type(filename)
        if not file_type_info:
            return None
        
        data_type, file_type = file_type_info
        
        return {
            'filename': filename,
            'data_type': data_type,
            'file_type': file_type,
            'database_name': self.get_database_name(data_type),
            'table_name': self.get_table_name(data_type, file_type),
            'description': self.get_description(data_type, file_type),
            'suffix': self._get_suffix_from_filename(filename)
        }

    def _get_suffix_from_filename(self, filename: str) -> Optional[str]:
        """從檔案名稱取得後綴"""
        for suffix in self.suffix_mapping.keys():
            if filename.endswith(suffix):
                return suffix
        return None

    def print_mapping_table(self):
        """列印對應表"""
        print("📋 檔案類型對應表")
        print("=" * 80)
        
        print("\n🔸 檔案後綴對應:")
        print("-" * 60)
        for suffix, (data_type, file_type) in self.suffix_mapping.items():
            data_desc = self.description_mapping[data_type]
            file_desc = self.description_mapping[file_type]
            db_name = self.database_mapping[data_type]
            table_name = self.table_mapping[(data_type, file_type)]
            
            print(f"  {suffix:<15} → {data_desc}{file_desc:<8} → {db_name}.{table_name}")
        
        print(f"\n🔸 資料庫對應:")
        print("-" * 60)
        for data_type, db_name in self.database_mapping.items():
            data_desc = self.description_mapping[data_type]
            print(f"  {data_desc:<8} → {db_name}")
        
        print(f"\n🔸 支援的檔案後綴:")
        print("-" * 60)
        for suffix in sorted(self.suffix_mapping.keys()):
            print(f"  {suffix}")

def test_file_type_mapping():
    """測試檔案類型對應表"""
    print("🧪 測試檔案類型對應表")
    print("=" * 80)
    
    mapping = FileTypeMapping()
    
    # 測試檔案
    test_files = [
        'a_lvr_land_a.csv',
        'a_lvr_land_a_build.csv',
        'a_lvr_land_a_land.csv',
        'a_lvr_land_a_park.csv',
        'a_lvr_land_b.csv',
        'a_lvr_land_b_build.csv',
        'a_lvr_land_b_land.csv',
        'a_lvr_land_b_park.csv',
        'a_lvr_land_c.csv',
        'a_lvr_land_c_build.csv',
        'a_lvr_land_c_land.csv',
        'a_lvr_land_c_park.csv',
        'unknown_file.csv'
    ]
    
    print("\n📊 檔案類型識別測試:")
    print("-" * 60)
    
    for filename in test_files:
        file_info = mapping.get_file_info(filename)
        if file_info:
            print(f"✅ {filename:<25} → {file_info['description']:<12} → {file_info['database_name']}.{file_info['table_name']}")
        else:
            print(f"❌ {filename:<25} → 不支援的檔案類型")

if __name__ == "__main__":
    mapping = FileTypeMapping()
    mapping.print_mapping_table()
    test_file_type_mapping()

