@echo off
setlocal enabledelayedexpansion

echo ========================================
echo PRDMRT - 시스템 Python 패키지 설치
echo ========================================
echo.

REM 시스템 Python 확인
echo [1/3] 시스템 Python 확인...
python --version 2>&1 | find /i "python" >nul
if errorlevel 1 (
    echo 오류: Python이 설치되어 있지 않습니다
    echo 다음 경로에 Python을 설치하세요: https://www.python.org/
    pause
    exit /b 1
)
echo 완료

REM pip 업그레이드
echo.
echo [2/3] pip 업그레이드...
python -m pip install --upgrade pip setuptools wheel
if errorlevel 1 (
    echo 경고: pip 업그레이드 중 오류 발생 (계속 진행)
)
echo 완료

REM 패키지 설치
echo.
echo [3/3] 필수 패키지 설치...
python -m pip install PyQt5 pandas numpy boto3 pyarrow openpyxl
if errorlevel 1 (
    echo 오류: 패키지 설치 실패
    pause
    exit /b 1
)
echo 완료

echo.
echo ========================================
echo 설치 완료! 이제 다음 명령으로 실행하세요:
echo   python prdmrt_ui_pyqt.py
echo ========================================
pause
