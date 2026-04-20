"""
공통 로깅 모듈
- 파일(prdmrt.log) + 콘솔 동시 출력
- 전역 logger 인스턴스 제공
"""

import logging
import os
from config import LOGGING_CONFIG, FILE_PATHS

def setup_logger(name='prdmrt'):
    """
    로거 설정 및 반환
    :param name: 로거 이름
    :return: 설정된 로거 객체
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, LOGGING_CONFIG['level']))
    
    # 기존 핸들러 제거 (중복 방지)
    if logger.handlers:
        logger.handlers.clear()
    
    # 로그 디렉터리 자동 생성
    log_dir = os.path.dirname(FILE_PATHS['log_file'])
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    # 파일 핸들러
    file_handler = logging.FileHandler(FILE_PATHS['log_file'], encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter(LOGGING_CONFIG['format'])
    file_handler.setFormatter(file_formatter)
    
    # 콘솔 핸들러
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter(LOGGING_CONFIG['format'])
    console_handler.setFormatter(console_formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

# 전역 로거 인스턴스
logger = setup_logger()
