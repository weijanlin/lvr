@echo off
chcp 65001 >nul
echo ========================================
echo LVR 資料庫完整還原工具
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

REM 讓使用者選擇還原模式
echo 請選擇還原模式:
echo 1. 還原單一資料庫
echo 2. 還原所有資料庫 (使用相同時間戳記的備份)
echo 3. 還原所有資料庫 (使用最新的備份檔案)
echo.
set /p RESTORE_MODE="請選擇 (1/2/3): "

if "%RESTORE_MODE%" == "1" (
    call restore_databases.bat
    goto :end
)

if "%RESTORE_MODE%" == "2" (
    goto :restore_by_timestamp
)

if "%RESTORE_MODE%" == "3" (
    goto :restore_latest
)

echo ❌ 無效的選擇
pause
exit /b 1

:restore_by_timestamp
echo.
echo 請輸入備份檔案的時間戳記 (例如: 20250909_084500):
set /p TIMESTAMP="時間戳記: "

REM 檢查是否有對應的備份檔案
set FOUND=0
if exist "%BACKUP_DIR%\LVR_UsedHouse_%TIMESTAMP%.bak" set FOUND=1
if exist "%BACKUP_DIR%\LVR_PreSale_%TIMESTAMP%.bak" set FOUND=1
if exist "%BACKUP_DIR%\LVR_Rental_%TIMESTAMP%.bak" set FOUND=1

if %FOUND% EQU 0 (
    echo ❌ 沒有找到時間戳記為 %TIMESTAMP% 的備份檔案
    pause
    exit /b 1
)

echo.
echo 找到以下備份檔案:
if exist "%BACKUP_DIR%\LVR_UsedHouse_%TIMESTAMP%.bak" echo - LVR_UsedHouse_%TIMESTAMP%.bak
if exist "%BACKUP_DIR%\LVR_PreSale_%TIMESTAMP%.bak" echo - LVR_PreSale_%TIMESTAMP%.bak
if exist "%BACKUP_DIR%\LVR_Rental_%TIMESTAMP%.bak" echo - LVR_Rental_%TIMESTAMP%.bak
echo.

set /p CONFIRM="確定要還原所有資料庫嗎? (Y/N): "
if /i not "%CONFIRM%" == "Y" (
    echo 還原操作已取消
    pause
    exit /b 0
)

goto :restore_databases

:restore_latest
echo.
echo 搜尋最新的備份檔案...

REM 找到最新的備份檔案
for /f "delims=" %%i in ('dir /b /o-d "%BACKUP_DIR%\*.bak" 2^>nul ^| findstr /r "LVR_UsedHouse_.*\.bak$" ^| head -1') do set LATEST_USEDHOUSE=%%i
for /f "delims=" %%i in ('dir /b /o-d "%BACKUP_DIR%\*.bak" 2^>nul ^| findstr /r "LVR_PreSale_.*\.bak$" ^| head -1') do set LATEST_PRESALE=%%i
for /f "delims=" %%i in ('dir /b /o-d "%BACKUP_DIR%\*.bak" 2^>nul ^| findstr /r "LVR_Rental_.*\.bak$" ^| head -1') do set LATEST_RENTAL=%%i

echo 找到最新的備份檔案:
if defined LATEST_USEDHOUSE echo - %LATEST_USEDHOUSE%
if defined LATEST_PRESALE echo - %LATEST_PRESALE%
if defined LATEST_RENTAL echo - %LATEST_RENTAL%
echo.

set /p CONFIRM="確定要還原所有資料庫嗎? (Y/N): "
if /i not "%CONFIRM%" == "Y" (
    echo 還原操作已取消
    pause
    exit /b 0
)

:restore_databases
echo.
echo 開始還原所有資料庫...
echo ========================================

REM 還原 LVR_UsedHouse
if exist "%BACKUP_DIR%\LVR_UsedHouse_%TIMESTAMP%.bak" (
    echo [1/3] 還原 LVR_UsedHouse 資料庫...
    call :restore_single_db LVR_UsedHouse "%BACKUP_DIR%\LVR_UsedHouse_%TIMESTAMP%.bak"
) else if defined LATEST_USEDHOUSE (
    echo [1/3] 還原 LVR_UsedHouse 資料庫...
    call :restore_single_db LVR_UsedHouse "%BACKUP_DIR%\%LATEST_USEDHOUSE%"
) else (
    echo ⚠️  跳過 LVR_UsedHouse (沒有找到備份檔案)
)

REM 還原 LVR_PreSale
if exist "%BACKUP_DIR%\LVR_PreSale_%TIMESTAMP%.bak" (
    echo [2/3] 還原 LVR_PreSale 資料庫...
    call :restore_single_db LVR_PreSale "%BACKUP_DIR%\LVR_PreSale_%TIMESTAMP%.bak"
) else if defined LATEST_PRESALE (
    echo [2/3] 還原 LVR_PreSale 資料庫...
    call :restore_single_db LVR_PreSale "%BACKUP_DIR%\%LATEST_PRESALE%"
) else (
    echo ⚠️  跳過 LVR_PreSale (沒有找到備份檔案)
)

REM 還原 LVR_Rental
if exist "%BACKUP_DIR%\LVR_Rental_%TIMESTAMP%.bak" (
    echo [3/3] 還原 LVR_Rental 資料庫...
    call :restore_single_db LVR_Rental "%BACKUP_DIR%\LVR_Rental_%TIMESTAMP%.bak"
) else if defined LATEST_RENTAL (
    echo [3/3] 還原 LVR_Rental 資料庫...
    call :restore_single_db LVR_Rental "%BACKUP_DIR%\%LATEST_RENTAL%"
) else (
    echo ⚠️  跳過 LVR_Rental (沒有找到備份檔案)
)

echo.
echo ========================================
echo 還原完成！
echo ========================================
echo 還原時間: %date% %time%
echo.

REM 驗證所有資料庫
echo 驗證還原結果...
sqlcmd -S %SERVER% -E -Q "SELECT name FROM sys.databases WHERE name LIKE 'LVR_%'"
if %ERRORLEVEL% EQU 0 (
    echo ✅ 所有資料庫驗證成功
) else (
    echo ❌ 資料庫驗證失敗
)

goto :end

:restore_single_db
set DB_NAME=%~1
set BACKUP_FILE=%~2

REM 斷開資料庫連線
sqlcmd -S %SERVER% -E -Q "ALTER DATABASE [%DB_NAME%] SET SINGLE_USER WITH ROLLBACK IMMEDIATE" >nul 2>&1

REM 還原資料庫
sqlcmd -S %SERVER% -E -Q "RESTORE DATABASE [%DB_NAME%] FROM DISK = '%BACKUP_FILE%' WITH REPLACE, STATS = 10"
if %ERRORLEVEL% EQU 0 (
    echo ✅ %DB_NAME% 還原成功
) else (
    echo ❌ %DB_NAME% 還原失敗
)

REM 恢復多使用者模式
sqlcmd -S %SERVER% -E -Q "ALTER DATABASE [%DB_NAME%] SET MULTI_USER" >nul 2>&1

goto :eof

:end
echo.
echo 按任意鍵結束...
pause >nul


