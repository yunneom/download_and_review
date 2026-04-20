@echo off
setlocal enabledelayedexpansion

echo ========================================
echo PRDMRT 프로젝트 - 환경 설정
echo ========================================
echo.

REM 현재 디렉터리
cd /d "C:\Users\LGRnD\Desktop\project\download_and_review"
echo 작업 디렉터리: %CD%
echo.

REM 가상환경 활성화
echo [1/4] 가상환경 활성화...
call .venv\Scripts\activate.bat
if errorlevel 1 (
    echo 오류: 가상환경 활성화 실패
    exit /b 1
)
echo 완료

REM pip 업그레이드
echo.
echo [2/4] pip 업그레이드...
python -m pip install --upgrade pip setuptools wheel
if errorlevel 1 (
    echo 경고: pip 업그레이드 중 오류 발생 (계속 진행)
)
echo 완료

REM 필수 패키지 설치
echo.
echo [3/4] 필수 패키지 설치...
pip install -r requirements.txt
if errorlevel 1 (
    echo 오류: 패키지 설치 실패
    exit /b 1
)
echo 완료

REM 설치 확인
echo.
echo [4/4] 설치 확인...
python -c "import PyQt5, pandas, numpy, boto3, pyarrow, openpyxl; print('모든 패키지 정상 로드됨')"
if errorlevel 1 (
    echo 오류: 패키지 로드 실패
    exit /b 1
)
echo 완료

echo.
echo ========================================
echo 환경 설정 완료!
echo ========================================
pause
