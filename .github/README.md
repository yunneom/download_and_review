# prdmrt 프로젝트

BMS 데이터 검증 자동화 도구 (완전 모듈화 버전)

## 프로젝트 구조

```
download_and_review/
├── config.py              # 설정 파일 (AWS 자격증명, 이메일 설정 등)
├── logger.py              # 로깅 유틸리티
├── s3_handler.py          # S3 파일 처리 모듈
├── data_processor.py      # 데이터 변환 모듈
├── validator.py           # 데이터 검증 모듈
├── email_sender.py        # 이메일 전송 모듈
├── prdmrt_core.py         # 핵심 통합 모듈
├── prdmrt_ui.py           # Tkinter UI 애플리케이션
├── import_os.py           # (구버전, 하위호환용)
├── prdmrt.log             # 로그 파일 (자동 생성)
└── README.md              # 이 문서0
```

## 주요 개선사항

### 1. 완전한 모듈화
- 각 기능별로 독립적인 모듈로 분리
- 유지보수 및 확장성 대폭 개선
- 테스트 용이성 향상

### 2. AWS 자격증명 하드코딩
- `config.py`에서 AWS Access Key/Secret Key 직접 설정 가능
- 환경변수 우선, 없으면 config.py 값 사용
- 보안을 위해 환경변수 사용 권장

### 3. 향상된 검증 로직
- SOC, 전압, 온도, NULL 값, 중복 데이터 등 다양한 검증
- 검증 규칙을 config.py에서 설정 가능

### 4. 통합 로깅
- 파일과 콘솔에 동시 로깅
- 로그 레벨 설정 가능

## 설치

### 필수 패키지 설치

```bash
pip install boto3 pandas pyarrow
```

## 설정

### 1. config.py 수정

```python
# AWS 자격증명 설정
AWS_CONFIG = {
    'access_key': 'AKIA...',  # 실제 AWS Access Key
    'secret_key': 'your_secret_key',  # 실제 AWS Secret Key
    'region': 'ap-northeast-2',
    'bucket': 'your-bucket-name',
}

# 이메일 설정
EMAIL_CONFIG = {
    'smtp_server': 'smtp.gmail.com',
    'smtp_port': 465,
    'smtp_user': 'your@email.com',
    'smtp_pass': 'your_password',
    'to_email': 'recipient@email.com',
}
```

### 2. 환경변수 설정 (선택사항, 더 안전함)

```bash
set AWS_ACCESS_KEY=your_access_key
set AWS_SECRET_KEY=your_secret_key
```

## 사용 방법

### 1. UI 모드 (권장)

```bash
python prdmrt_ui.py
```

UI에서 각 설정값을 입력하거나 config.py의 기본값 사용

### 2. CLI 모드 (프로그래밍 방식)

```python
from prdmrt_core import PrdmrtCore

# 초기화
core = PrdmrtCore()
core.init_s3()  # config.py의 설정 사용
core.init_email()

# 전체 파이프라인 실행
core.run_full_pipeline('your-data.parquet')
```

### 3. 개별 모듈 사용

```python
# S3만 사용
from s3_handler import S3Handler
s3 = S3Handler()
s3.download_file('s3-key', 'local-path')

# 데이터 처리만 사용
from data_processor import DataProcessor
processor = DataProcessor()
df = processor.parquet_to_csv('input.parquet', 'output.csv')

# 검증만 사용
from validator import DataValidator
validator = DataValidator()
issues = validator.validate_bms_data(df)
```

## 기능

1. **S3 파일 다운로드/업로드**: AWS S3와의 완벽한 통합
2. **데이터 변환**: Parquet ↔ CSV 양방향 변환
3. **고급 데이터 검증**: 
   - SOC 범위 검증 (0-100)
   - 전압 이상치 검증
   - 온도 이상치 검증
   - NULL 값 검증
   - 중복 데이터 검증
4. **리포트 생성**: 검증 결과를 CSV 리포트로 저장
5. **이메일 전송**: SMTP를 통한 자동 리포트 전송

## 로그

- 모든 작업은 `prdmrt.log` 파일에 기록
- 콘솔에도 동시 출력
- 로그 레벨은 config.py에서 설정 가능

## 보안

- AWS 자격증명은 절대 Git에 커밋하지 마세요
- `.gitignore`에 `config.py` 추가 권장
- 가능하면 환경변수 사용
- SMTP 비밀번호는 앱 비밀번호 사용 권장 (Gmail 등)

## 문제 해결

### ImportError 발생 시
- 모든 모듈 파일이 같은 폴더에 있는지 확인
- Python 버전 확인 (3.7 이상 권장)

### 모듈 없음 오류 시
```bash
pip install boto3 pandas pyarrow
```

### S3 다운로드 실패 시
- config.py의 AWS 자격증명 확인
- S3 버킷명과 파일 경로 확인
- IAM 권한 확인
- 네트워크 연결 확인

### 이메일 전송 실패 시
- SMTP 서버 주소와 포트 확인
- Gmail의 경우 "앱 비밀번호" 사용 필요
- 방화벽 설정 확인

## 개발 가이드

### 새로운 검증 규칙 추가

`validator.py`의 `DataValidator` 클래스에 메서드 추가:

```python
def _validate_custom_rule(self, df):
    """커스텀 검증 규칙"""
    # 검증 로직
    if 조건:
        self.issues.append("이슈 메시지")
```

### 새로운 데이터 처리 기능 추가

`data_processor.py`의 `DataProcessor` 클래스에 메서드 추가

### 다른 클라우드 스토리지 지원

`s3_handler.py`를 참고하여 새로운 핸들러 생성
