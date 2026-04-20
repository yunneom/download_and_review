@echo off
REM 한글 인코딩 설정
chcp 65001 >nul

echo ========================================
echo PRDMRT 프로젝트 - 실행 메뉴
echo ========================================
echo.
echo [1] 가상환경으로 실행 (권장) 
echo [2] 시스템 Python으로 실행 (패키지 설치 필요)
echo [3] 시스템 Python에 패키지 설치
echo [4] 종료
echo.

set /p choice="선택 (1-4): "

if "%choice%"=="1" (
    echo 가상환경으로 실행하는 중...
    call run.bat
) else if "%choice%"=="2" (
    echo 시스템 Python으로 실행...
    python prdmrt_ui_pyqt.py
) else if "%choice%"=="3" (
    echo 시스템 Python에 패키지 설치...
    call install_system_python.bat
) else if "%choice%"=="4" (
    exit /b 0
) else (
    echo 잘못된 선택입니다
    timeout /t 2
    call %0
)
