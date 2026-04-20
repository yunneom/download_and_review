@echo off
cd /d "%~dp0.."
chcp 65001 >nul

echo ========================================
echo  PRDMRT Data Validation Tool
echo ========================================
echo.

REM 가상환경 Python 우선 시도
if exist ".venv\Scripts\python.exe" (
    echo 가상환경으로 실행합니다...
    .venv\Scripts\python.exe prdmrt_ui_pyqt.py
    goto end
)

REM 가상환경 없으면 시스템 Python 시도
echo 가상환경 없음. 시스템 Python으로 실행합니다...
python --version >nul 2>&1
if errorlevel 1 (
    echo.
    echo [오류] Python이 설치되어 있지 않습니다.
    echo   https://www.python.org 에서 설치 후 다시 시도하세요.
    pause
    exit /b 1
)

REM 필수 패키지 확인
python -c "import PyQt5, pandas, boto3, pyarrow" >nul 2>&1
if errorlevel 1 (
    echo.
    echo 필수 패키지를 설치합니다...
    python -m pip install PyQt5 pandas numpy boto3 pyarrow openpyxl python-dotenv
    if errorlevel 1 (
        echo [오류] 패키지 설치 실패
        pause
        exit /b 1
    )
)

python prdmrt_ui_pyqt.py

:end
echo.
echo 종료되었습니다.
pause
