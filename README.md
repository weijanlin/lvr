# LVR 實價登錄資料匯入系統

## 專案概述

本專案是一個 Python 程式，用於將 LVR（實價登錄）CSV 檔案匯入到 Microsoft SQL Server Express 資料庫中。系統支援中古屋、預售屋、租屋三種不同類型的房地產交易資料，並能自動處理多個季度的資料夾。

## 功能說明
當執行 python import_new_folders.py 1 時：
自動掃描新資料夾
顯示發現的新資料夾列表
自動使用 MAX_WORKERS 執行緒數
自動開始匯入（無需確認）
匯入完成後自動更新 config.py（無需確認）

## 系統架構

### 資料庫結構
- **LVR_UsedHouse**: 中古屋交易資料庫
- **LVR_PreSale**: 預售屋交易資料庫  
- **LVR_Rental**: 租屋交易資料庫

### 檔案類型說明
根據檔案名稱後綴判斷資料類型：

| 檔案後綴 | 資料類型 | 說明 |
|---------|---------|------|
| `_a.csv` | 中古屋 | 成屋交易資料(合計) |
| `_a_build.csv` | 中古屋建物 | 成屋交易之建物案件 |
| `_a_land.csv` | 中古屋土地 | 成屋交易之土地案件 |
| `_a_park.csv` | 中古屋停車場 | 成屋交易之停車場案件 |
| `_b.csv` | 預售屋 | 預售屋交易資料(合計) |
| `_b_land.csv` | 預售屋土地 | 預售屋交易之土地案件 |
| `_b_park.csv` | 預售屋停車場 | 預售屋交易之停車場案件 |
| `_c.csv` | 租屋 | 租賃交易資料(合計) |
| `_c_build.csv` | 租屋建物 | 租賃交易之建物案件 |
| `_c_land.csv` | 租屋土地 | 租賃交易之土地案件 |
| `_c_park.csv` | 租屋停車場 | 租賃交易之停車場案件 |

### 縣市代碼對應

| 代碼 | 縣市 | 代碼 | 縣市 |
|------|------|------|------|
| a | 臺北市 | n | 彰化縣 |
| b | 臺中市 | o | 新竹市 |
| c | 基隆市 | p | 雲林縣 |
| d | 臺南市 | q | 嘉義縣 |
| e | 高雄市 | t | 屏東縣 |
| f | 新北市 | u | 花蓮縣 |
| g | 宜蘭縣 | v | 臺東縣 |
| h | 桃園市 | w | 金門縣 |
| i | 嘉義市 | x | 澎湖縣 |
| j | 新竹縣 | z | 連江縣 |
| k | 苗栗縣 | | |
| m | 南投縣 | | |

## 安裝與設定

### 系統需求
- Python 3.7+
- Microsoft SQL Server Express
- ODBC Driver 17 for SQL Server

### 安裝步驟

1. **安裝 Python 套件**
```bash
pip install -r requirements.txt
```

2. **設定資料庫連線**
複製 `config.py.example` 為 `config.py`，並編輯 `config.py` 檔案，設定資料庫連線參數：
```python
DB_CONFIG = {
    'server': 'localhost\\SQLEXPRESS',  # 請填入您的 SQL Server 位址
    'driver': 'ODBC Driver 17 for SQL Server',
    'username': 'your_username',  # 請填入您的使用者名稱
    'password': 'your_password',  # 請填入您的密碼
    'trusted_connection': 'no',
    'encrypt': 'no'
}
```

> **注意**：`config.py` 檔案包含敏感資訊，已被 `.gitignore` 排除，不會上傳到版本控制系統。

3. **測試資料庫連線**
```bash
python test_connection.py
```

## 使用方式

### 基本使用

1. **建立資料庫和資料表**
```bash
python check_database_structure.py
```

2. **匯入單一資料夾**
```bash
python test_single_folder_import.py
```

3. **匯入所有資料夾**
```bash
python data_importer.py
```

### 進階使用

#### 檢查 ODBC 驅動程式
```bash
python check_drivers.py
```

#### 檢查 SQL Server 資訊
```bash
python sql_server_info.py
```

#### 模擬資料庫測試（無需實際資料庫）
```bash
python mock_database_test.py
```

## 專案結構

```
LVR250901/
├── config.py.example           # 設定檔範例（請複製為 config.py 並填入實際資訊）
├── config.py                   # 設定檔（本地檔案，不會上傳到 Git）
├── database_manager.py          # 資料庫管理
├── data_importer.py            # 資料匯入器
├── import_new_folders.py       # 自動掃描並匯入新資料夾
├── test_connection.py          # 連線測試
├── check_database_structure.py # 資料庫結構檢查
├── test_single_folder_import.py # 單一資料夾測試
├── check_drivers.py            # ODBC 驅動程式檢查
├── sql_server_info.py          # SQL Server 資訊檢查
├── mock_database_test.py       # 模擬測試
├── requirements.txt            # Python 套件需求
├── README.md                   # 專案說明文件
└── .gitignore                  # Git 忽略檔案設定
```

## 開發進度

### ✅ 已完成功能
- [x] 基礎資料庫連線和測試
- [x] 資料庫建立（三個資料庫）
- [x] 中古屋資料匯入功能
- [x] 單一資料夾匯入功能
- [x] CSV 檔案解析（跳過第二行欄位名稱）
- [x] 資料類型轉換和清理
- [x] 批次處理和進度顯示

### 🚧 進行中
- [ ] 修復預售屋和租屋資料表結構
- [ ] 完善檔案類型識別邏輯
- [ ] 支援子檔案匯入（_build, _land, _park）

### 📋 待辦事項
- [ ] 擴展到所有季度資料夾（113Q1-114Q2）
- [ ] 並行處理優化
- [ ] 資料完整性驗證
- [ ] 錯誤處理和日誌改進
- [ ] 匯入速度優化

## 測試結果

### 最新測試結果（113Q1 資料夾）
- **總檔案數**: 225
- **成功檔案數**: 21（中古屋資料）
- **失敗檔案數**: 204（預售屋和租屋資料）
- **總資料行數**: 88,883

### 成功匯入的檔案類型
- 中古屋交易資料（_a.csv）：21 個檔案
- 包含縣市：臺北市、臺中市、基隆市、臺南市、高雄市、新北市等

## 已知問題

1. **預售屋資料表結構不匹配**
   - 錯誤：`無效的資料行名稱 '建案名稱'`、`'棟及號'`、`'解約情形'`
   - 需要建立預售屋專用的資料表結構

2. **租屋資料表結構不匹配**
   - 錯誤：`無效的資料行名稱 '土地面積平方公尺'`、`'租賃年月日'`、`'租賃筆棟數'`
   - 需要建立租屋專用的資料表結構

3. **字串截斷問題**
   - 錯誤：`字串或二進位資料將會截斷`
   - 需要增加字串欄位的長度

4. **子檔案無法識別**
   - 警告：`無法判斷檔案類型: xxx_build.csv`
   - 需要改進檔案類型識別邏輯

## 技術規格

### 資料庫連線
- **伺服器**: 請在 `config.py` 中設定（預設為 `localhost\SQLEXPRESS`）
- **驅動程式**: ODBC Driver 17 for SQL Server
- **認證**: SQL Server 認證（請在 `config.py` 中設定使用者名稱和密碼）
- **編碼**: UTF-8

### 批次處理設定
- **批次大小**: 1,000 筆記錄
- **最大並行數**: 4 個執行緒
- **進度顯示**: 每批次顯示進度

### 支援的資料夾
- 113Q1, 113Q2, 113Q3, 113Q4 (2023年各季度)
- 114Q1, 114Q2 (2024年前兩季度)

## 故障排除

### 常見問題

1. **連線失敗**
   - 檢查 SQL Server 是否啟動
   - 確認 ODBC 驅動程式已安裝
   - 驗證連線參數

2. **套件安裝失敗**
   - 使用 `pip install --upgrade pip`
   - 嘗試安裝預編譯版本

3. **資料匯入失敗**
   - 檢查 CSV 檔案格式
   - 確認資料表結構
   - 查看日誌檔案

### 日誌檔案
- **位置**: `lvr_import.log`
- **編碼**: UTF-8
- **內容**: 詳細的匯入過程和錯誤資訊

## 貢獻指南

1. Fork 專案
2. 建立功能分支
3. 提交變更
4. 推送到分支
5. 建立 Pull Request

## 授權

本專案採用 MIT 授權條款。

## 聯絡資訊

如有問題或建議，請建立 Issue 或聯絡開發團隊。

---

**最後更新**: 2025-09-04
**版本**: 1.0.0
**狀態**: 開發中