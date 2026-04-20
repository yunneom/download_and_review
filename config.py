"""
프로젝트 설정 파일
- AWS 자격증명, 파일 경로, 검증/로깅 설정값 관리
- .env 파일에서 민감 정보 로드
"""

import os
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# === AWS 설정 ===
AWS_CONFIG = {
    'access_key': os.getenv('AWS_ACCESS_KEY_ID', ''),
    'secret_key': os.getenv('AWS_SECRET_ACCESS_KEY', ''),
    'region': os.getenv('AWS_REGION', 'ap-northeast-2'),
    'bucket': os.getenv('S3_BUCKET', 'eplat-validation-monitor'),
}

# === 파일 경로 설정 ===
FILE_PATHS = {
    'local_parquet': 'downloaded.parquet',
    'local_csv': 'converted.csv',
    'report_path': 'validation_report.csv',
    'log_file': 'logs/prdmrt.log',
}

# === 검증 설정 ===
VALIDATION_CONFIG = {
    'soc_min': 0,
    'soc_max': 100,
    # 추가 검증 규칙들...
}

# === 로깅 설정 ===
LOGGING_CONFIG = {
    'level': 'INFO',  # DEBUG, INFO, WARNING, ERROR, CRITICAL
    'format': '%(asctime)s %(levelname)s %(message)s',
    'date_format': '%Y-%m-%d %H:%M:%S',
}

def get_aws_credentials():
    """AWS 자격증명 반환 (.env에서 로드)"""
    return {
        'access_key': AWS_CONFIG['access_key'],
        'secret_key': AWS_CONFIG['secret_key'],
        'region': AWS_CONFIG['region'],
    }

def get_validation_config():
    """검증 설정 반환"""
    return VALIDATION_CONFIG.copy()

