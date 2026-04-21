@echo off

REM === [#16] Auto-elevate to Administrator ===
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo [INFO] Not running as admin. Attempting auto-elevation...
    powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Process -FilePath \"%~f0\" -Verb RunAs"
    exit /b 0
)
cd /d "%~dp0"

echo [OK] Running as Administrator.
echo [OK] Working dir: %cd%
echo.

REM === Safety wrapper: always pause at end ===
call :main_logic %*
echo.
echo Press any key to close...
pause >nul
exit /b 0

:main_logic
setlocal enabledelayedexpansion

echo ================================================================
echo   Block Trade Analyzer v19 Launcher
echo   - Auto-repeats every 10 min until 15:40
echo   - Press Ctrl+C or close window to stop
echo ================================================================
echo.

REM === Find 32-bit Python (CYBOS Plus REQUIRES 32-bit) ===
echo [STEP 1] Searching for 32-bit Python...
echo.

set PYTHON_CMD=

REM --- Check common 32-bit Python install paths ---
for %%V in (313 312 311 310 39 38) do (
    if "!PYTHON_CMD!"=="" (
        set "TRY=C:\Users\%USERNAME%\AppData\Local\Programs\Python\Python%%V-32\python.exe"
        if exist "!TRY!" (
            set "PYTHON_CMD=!TRY!"
            echo [FOUND] !TRY!
        )
    )
)

for %%V in (313 312 311 310 39 38) do (
    if "!PYTHON_CMD!"=="" (
        set "TRY=C:\Program Files ^(x86^)\Python%%V-32\python.exe"
        if exist "!TRY!" (
            set "PYTHON_CMD=!TRY!"
            echo [FOUND] !TRY!
        )
    )
)

for %%V in (313 312 311 310 39 38) do (
    if "!PYTHON_CMD!"=="" (
        set "TRY=C:\Python%%V-32\python.exe"
        if exist "!TRY!" (
            set "PYTHON_CMD=!TRY!"
            echo [FOUND] !TRY!
        )
    )
)

REM --- Found via path scan? Verify it ---
if not "!PYTHON_CMD!"=="" goto :verify_bitness

REM --- Nothing found via path scan. Try auto-install. ---
goto :do_auto_install

:verify_bitness
echo.
"!PYTHON_CMD!" -c "import struct;print(struct.calcsize(chr(80))*8)" > "%TEMP%\_pybits.tmp" 2>nul
set /p BITS=<"%TEMP%\_pybits.tmp"
del "%TEMP%\_pybits.tmp" >nul 2>&1

if "!BITS!"=="32" goto :python_ready

echo [WARN] Found !PYTHON_CMD! but it is !BITS!-bit, not 32-bit.
echo [WARN] Will auto-install 32-bit Python...
set PYTHON_CMD=
echo.

:do_auto_install
echo ================================================================
echo   [AUTO] 32-bit Python not found. Installing automatically...
echo ================================================================
echo.
call :auto_install_python
if errorlevel 1 goto :install_failed

REM --- Re-scan after install ---
set "PYTHON_CMD=C:\Users\%USERNAME%\AppData\Local\Programs\Python\Python312-32\python.exe"
if exist "!PYTHON_CMD!" goto :verify_bitness

set "PYTHON_CMD=C:\Python312-32\python.exe"
if exist "!PYTHON_CMD!" goto :verify_bitness

REM --- Try py launcher again after install ---
py -3.12-32 -c "import sys;print(sys.executable)" > "%TEMP%\_pypath.tmp" 2>nul
set /p PYTHON_CMD=<"%TEMP%\_pypath.tmp"
del "%TEMP%\_pypath.tmp" >nul 2>&1
if not "!PYTHON_CMD!"=="" goto :verify_bitness

:install_failed
echo.
echo ================================================================
echo   [ERROR] Could not get 32-bit Python working.
echo ================================================================
echo.
echo   Please install manually:
echo     1. Go to https://www.python.org/ftp/python/3.12.9/
echo     2. Download "python-3.12.9.exe" ^(the 32-bit installer^)
echo     3. CHECK "Add to PATH" during install
echo.
exit /b 1

:python_ready
echo [OK] Python 32-bit confirmed: !PYTHON_CMD!

REM === Check pywin32 ===
"!PYTHON_CMD!" -c "import win32com.client" >nul 2>&1
if errorlevel 1 (
    echo [INSTALL] pywin32 not found. Installing...
    "!PYTHON_CMD!" -m pip install pywin32 --quiet
    if errorlevel 1 (
        echo [ERROR] Failed to install pywin32.
        exit /b 1
    )
    echo [OK] pywin32 installed.
) else (
    echo [OK] pywin32
)

REM === Check requests ===
"!PYTHON_CMD!" -c "import requests" >nul 2>&1
if errorlevel 1 (
    echo [INSTALL] requests not found. Installing...
    "!PYTHON_CMD!" -m pip install requests --quiet
) else (
    echo [OK] requests
)

REM === Check openpyxl ===
"!PYTHON_CMD!" -c "import openpyxl" >nul 2>&1
if errorlevel 1 (
    echo [INSTALL] openpyxl not found. Installing...
    "!PYTHON_CMD!" -m pip install openpyxl --quiet
) else (
    echo [OK] openpyxl
)

REM === Find script ===
set SCRIPT=%~dp0block_trade_analyzer_daishin_v19.py
if not exist "%SCRIPT%" (
    echo [ERROR] Script not found: %SCRIPT%
    echo [ERROR] Place this .bat in the same folder as the .py file.
    exit /b 1
)
echo [OK] Script found.
echo.

REM === First run ===
echo [RUN] First run at %TIME:~0,8%
"!PYTHON_CMD!" "%SCRIPT%" %*
if errorlevel 1 (
    echo [ERROR] Script failed. Check CYBOS Plus login status.
    exit /b 1
)

echo.
echo [INFO] First run completed. Auto-repeat every 10 min until 15:40.
echo [INFO] Press Ctrl+C to stop.
echo.

REM === Auto-repeat loop ===
:loop
for /f "tokens=1-2 delims=:" %%a in ("%TIME: =0%") do (
    set /a "HH=1%%a-100"
    set /a "MM=1%%b-100"
)

if !HH! GTR 15 goto :done
if !HH! EQU 15 if !MM! GEQ 40 goto :done

echo [WAIT] Next run in 10 min... ^(Ctrl+C to stop^)
ping -n 601 127.0.0.1 >nul 2>&1

echo [RUN] Repeat at %TIME:~0,8%
"!PYTHON_CMD!" "%SCRIPT%" %*
if errorlevel 1 (
    echo [WARN] Script error. Will retry next cycle.
)
echo.
goto :loop

:done
echo.
echo [DONE] Market closed. Final run completed.

endlocal
exit /b 0


:auto_install_python
REM === Download and install Python 3.12.9 32-bit ===
set "PY_URL=https://www.python.org/ftp/python/3.12.9/python-3.12.9.exe"
set "PY_INSTALLER=%TEMP%\python-3.12.9-32bit.exe"

echo [DOWNLOAD] Python 3.12.9 32-bit from python.org...
echo [DOWNLOAD] URL: %PY_URL%
echo [DOWNLOAD] This may take 1-2 minutes...
echo.

powershell -NoProfile -ExecutionPolicy Bypass -Command "try { [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; Invoke-WebRequest -Uri '%PY_URL%' -OutFile '%PY_INSTALLER%' -UseBasicParsing; exit 0 } catch { Write-Host $_.Exception.Message; exit 1 }"
if errorlevel 1 (
    echo [ERROR] Download failed. Check internet connection.
    exit /b 1
)

if not exist "%PY_INSTALLER%" (
    echo [ERROR] Installer file not found after download.
    exit /b 1
)
echo [OK] Download complete.
echo.

echo [INSTALL] Installing Python 3.12.9 32-bit ^(this takes 1-3 minutes^)...
echo [INSTALL] Please wait and do NOT close this window.
echo.

"%PY_INSTALLER%" /passive PrependPath=1 Include_test=0 Include_doc=0
if errorlevel 1 (
    echo [ERROR] Python installer returned error.
    del "%PY_INSTALLER%" >nul 2>&1
    exit /b 1
)

echo.
echo [OK] Python 3.12.9 32-bit installed successfully.
del "%PY_INSTALLER%" >nul 2>&1

REM --- Refresh PATH for current session ---
set "PATH=C:\Users\%USERNAME%\AppData\Local\Programs\Python\Python312-32;C:\Users\%USERNAME%\AppData\Local\Programs\Python\Python312-32\Scripts;%PATH%"

exit /b 0