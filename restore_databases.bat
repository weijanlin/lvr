@echo off
chcp 65001 >nul
echo ========================================
echo LVR 資料庫還原工具
echo ========================================
echo.

REM 設定變數
set SERVER=localhost\SQLEXPRESS
set BACKUP_DIR=%~dp0backups

REM 檢查備份目錄是否存在
if not exist "%BACKUP_DIR%" (
    echo ❌ 備份目錄不存在: %BACKUP_DIR%
    echo 請先執行備份程式或確認備份檔案位置
    pause
    exit /b 1
)

echo 還原時間: %date% %time%
echo 備份目錄: %BACKUP_DIR%
echo.

REM 顯示可用的備份檔案
echo 可用的備份檔案:
echo ========================================
dir /b "%BACKUP_DIR%\*.bak" 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo ❌ 沒有找到備份檔案
    pause
    exit /b 1
)
echo.

REM 讓使用者選擇備份檔案
echo 請選擇要還原的備份檔案 (輸入檔案名稱，不包含路徑):
echo 例如: LVR_UsedHouse_20250909_084500.bak
echo.
set /p BACKUP_FILE="請輸入備份檔案名稱: "

REM 檢查檔案是否存在
if not exist "%BACKUP_DIR%\%BACKUP_FILE%" (
    echo ❌ 備份檔案不存在: %BACKUP_DIR%\%BACKUP_FILE%
    pause
    exit /b 1
)

echo.
echo ⚠️  警告: 還原操作將會覆蓋現有的資料庫！
echo 請確認您要還原的資料庫:
echo.

REM 根據檔案名稱判斷要還原的資料庫
if "%BACKUP_FILE%" == "LVR_UsedHouse"* (
    set DB_NAME=LVR_UsedHouse
    set DB_DESC=中古屋資料庫
) else if "%BACKUP_FILE%" == "LVR_PreSale"* (
    set DB_NAME=LVR_PreSale
    set DB_DESC=預售屋資料庫
) else if "%BACKUP_FILE%" == "LVR_Rental"* (
    set DB_NAME=LVR_Rental
    set DB_DESC=租屋資料庫
) else (
    echo ❌ 無法識別資料庫類型，請確認檔案名稱格式正確
    pause
    exit /b 1
)

echo 資料庫名稱: %DB_NAME% (%DB_DESC%)
echo 備份檔案: %BACKUP_FILE%
echo.

set /p CONFIRM="確定要還原嗎? (Y/N): "
if /i not "%CONFIRM%" == "Y" (
    echo 還原操作已取消
    pause
    exit /b 0
)

echo.
echo 開始還原 %DB_NAME% 資料庫...
echo ========================================

REM 先斷開所有連線到該資料庫
echo [1/3] 斷開資料庫連線...
sqlcmd -S %SERVER% -E -Q "ALTER DATABASE [%DB_NAME%] SET SINGLE_USER WITH ROLLBACK IMMEDIATE"
if %ERRORLEVEL% NEQ 0 (
    echo ⚠️  無法斷開資料庫連線，繼續執行還原...
)

REM 還原資料庫
echo [2/3] 還原資料庫...
sqlcmd -S %SERVER% -E -Q "RESTORE DATABASE [%DB_NAME%] FROM DISK = '%BACKUP_DIR%\%BACKUP_FILE%' WITH REPLACE, STATS = 10"
if %ERRORLEVEL% EQU 0 (
    echo ✅ %DB_NAME% 還原成功
) else (
    echo ❌ %DB_NAME% 還原失敗
    pause
    exit /b 1
)

REM 恢復多使用者模式
echo [3/3] 恢復多使用者模式...
sqlcmd -S %SERVER% -E -Q "ALTER DATABASE [%DB_NAME%] SET MULTI_USER"
if %ERRORLEVEL% EQU 0 (
    echo ✅ 資料庫已恢復多使用者模式
) else (
    echo ⚠️  無法恢復多使用者模式，請手動檢查
)

echo.
echo ========================================
echo 還原完成！
echo ========================================
echo 資料庫: %DB_NAME% (%DB_DESC%)
echo 備份檔案: %BACKUP_FILE%
echo 還原時間: %date% %time%
echo.

REM 驗證還原結果
echo 驗證還原結果...
sqlcmd -S %SERVER% -E -Q "SELECT name FROM sys.databases WHERE name = '%DB_NAME%'"
if %ERRORLEVEL% EQU 0 (
    echo ✅ 資料庫驗證成功
) else (
    echo ❌ 資料庫驗證失敗
)

echo.
echo 按任意鍵結束...
pause >nul


