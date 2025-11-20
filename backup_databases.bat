@echo off
chcp 65001 >nul
echo ========================================
echo LVR 資料庫備份工具
echo ========================================
echo.

REM 設定變數
set SERVER=localhost\SQLEXPRESS
set BACKUP_DIR=%~dp0backups
set TIMESTAMP=%date:~0,4%%date:~5,2%%date:~8,2%_%time:~0,2%%time:~3,2%%time:~6,2%
set TIMESTAMP=%TIMESTAMP: =0%

REM 建立備份目錄
if not exist "%BACKUP_DIR%" (
    echo 建立備份目錄: %BACKUP_DIR%
    mkdir "%BACKUP_DIR%"
)

echo 備份時間: %date% %time%
echo 備份目錄: %BACKUP_DIR%
echo.

REM 備份 LVR_UsedHouse 資料庫
echo [1/3] 備份 LVR_UsedHouse 資料庫...
sqlcmd -S %SERVER% -E -Q "BACKUP DATABASE [LVR_UsedHouse] TO DISK = '%BACKUP_DIR%\LVR_UsedHouse_%TIMESTAMP%.bak' WITH FORMAT, INIT, NAME = 'LVR_UsedHouse-Full Database Backup', SKIP, NOREWIND, NOUNLOAD, STATS = 10"
if %ERRORLEVEL% EQU 0 (
    echo ✅ LVR_UsedHouse 備份成功
) else (
    echo ❌ LVR_UsedHouse 備份失敗
    pause
    exit /b 1
)
echo.

REM 備份 LVR_PreSale 資料庫
echo [2/3] 備份 LVR_PreSale 資料庫...
sqlcmd -S %SERVER% -E -Q "BACKUP DATABASE [LVR_PreSale] TO DISK = '%BACKUP_DIR%\LVR_PreSale_%TIMESTAMP%.bak' WITH FORMAT, INIT, NAME = 'LVR_PreSale-Full Database Backup', SKIP, NOREWIND, NOUNLOAD, STATS = 10"
if %ERRORLEVEL% EQU 0 (
    echo ✅ LVR_PreSale 備份成功
) else (
    echo ❌ LVR_PreSale 備份失敗
    pause
    exit /b 1
)
echo.

REM 備份 LVR_Rental 資料庫
echo [3/3] 備份 LVR_Rental 資料庫...
sqlcmd -S %SERVER% -E -Q "BACKUP DATABASE [LVR_Rental] TO DISK = '%BACKUP_DIR%\LVR_Rental_%TIMESTAMP%.bak' WITH FORMAT, INIT, NAME = 'LVR_Rental-Full Database Backup', SKIP, NOREWIND, NOUNLOAD, STATS = 10"
if %ERRORLEVEL% EQU 0 (
    echo ✅ LVR_Rental 備份成功
) else (
    echo ❌ LVR_Rental 備份失敗
    pause
    exit /b 1
)
echo.

REM 顯示備份檔案資訊
echo ========================================
echo 備份完成！
echo ========================================
echo 備份檔案列表:
dir /b "%BACKUP_DIR%\*%TIMESTAMP%.bak"
echo.
echo 備份檔案大小:
for %%f in ("%BACKUP_DIR%\*%TIMESTAMP%.bak") do (
    echo %%~nxf: %%~zf bytes
)
echo.

REM 建立備份資訊檔案
echo 備份資訊 > "%BACKUP_DIR%\backup_info_%TIMESTAMP%.txt"
echo ========== >> "%BACKUP_DIR%\backup_info_%TIMESTAMP%.txt"
echo 備份時間: %date% %time% >> "%BACKUP_DIR%\backup_info_%TIMESTAMP%.txt"
echo 備份目錄: %BACKUP_DIR% >> "%BACKUP_DIR%\backup_info_%TIMESTAMP%.txt"
echo 資料庫伺服器: %SERVER% >> "%BACKUP_DIR%\backup_info_%TIMESTAMP%.txt"
echo. >> "%BACKUP_DIR%\backup_info_%TIMESTAMP%.txt"
echo 備份的資料庫: >> "%BACKUP_DIR%\backup_info_%TIMESTAMP%.txt"
echo - LVR_UsedHouse (中古屋資料庫) >> "%BACKUP_DIR%\backup_info_%TIMESTAMP%.txt"
echo - LVR_PreSale (預售屋資料庫) >> "%BACKUP_DIR%\backup_info_%TIMESTAMP%.txt"
echo - LVR_Rental (租屋資料庫) >> "%BACKUP_DIR%\backup_info_%TIMESTAMP%.txt"
echo. >> "%BACKUP_DIR%\backup_info_%TIMESTAMP%.txt"
echo 備份檔案: >> "%BACKUP_DIR%\backup_info_%TIMESTAMP%.txt"
dir /b "%BACKUP_DIR%\*%TIMESTAMP%.bak" >> "%BACKUP_DIR%\backup_info_%TIMESTAMP%.txt"

echo 備份資訊已儲存至: backup_info_%TIMESTAMP%.txt
echo.
echo 按任意鍵結束...
pause >nul


