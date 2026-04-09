"""
S3 파일 처리 모듈
- AWS S3 파일 다운로드/업로드/목록 조회
- boto3 래퍼 클래스
"""

import boto3
from logger import logger
from config import get_aws_credentials, AWS_CONFIG

class S3Handler:
    """S3 파일 다운로드/업로드 핸들러"""
    
    def __init__(self, bucket=None, access_key=None, secret_key=None, region=None):
        """
        S3 핸들러 초기화
        :param bucket: S3 버킷명
        :param access_key: AWS Access Key (None이면 config에서 읽음)
        :param secret_key: AWS Secret Key (None이면 config에서 읽음)
        :param region: AWS 리전 (None이면 config에서 읽음)
        """
        self.bucket = bucket or AWS_CONFIG['bucket']
        
        # 자격증명 설정
        if access_key and secret_key:
            self.access_key = access_key
            self.secret_key = secret_key
            self.region = region or AWS_CONFIG['region']
        else:
            creds = get_aws_credentials()
            self.access_key = creds['access_key']
            self.secret_key = creds['secret_key']
            self.region = creds['region']
        
        # S3 클라이언트 초기화
        self.s3_client = None
        self._init_client()
    
    def _init_client(self):
        """S3 클라이언트 초기화"""
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name=self.region
            )
            logger.info("S3 클라이언트 초기화 완료")
        except Exception as e:
            logger.error(f"S3 클라이언트 초기화 실패: {e}")
            raise
    
    def download_file(self, s3_key, local_path):
        """
        S3에서 파일 다운로드
        :param s3_key: S3 객체 키 (파일 경로)
        :param local_path: 로컬 저장 경로
        """
        try:
            logger.info(f"S3 다운로드 시작: {s3_key} → {local_path}")
            print(f"S3에서 파일 다운로드 시작: {s3_key}")
            
            self.s3_client.download_file(self.bucket, s3_key, local_path)
            
            logger.info(f"S3 다운로드 완료: {local_path}")
            print(f"S3 파일 다운로드 완료: {local_path}")
        except Exception as e:
            logger.error(f"S3 다운로드 실패: {e}")
            print(f"S3 다운로드 실패: {e}")
            raise
    
    def upload_file(self, local_path, s3_key):
        """
        로컬 파일을 S3에 업로드
        :param local_path: 로컬 파일 경로
        :param s3_key: S3 객체 키 (저장될 경로)
        """
        try:
            logger.info(f"S3 업로드 시작: {local_path} → {s3_key}")
            print(f"S3에 파일 업로드 시작: {local_path}")
            
            self.s3_client.upload_file(local_path, self.bucket, s3_key)
            
            logger.info(f"S3 업로드 완료: {s3_key}")
            print(f"S3 파일 업로드 완료: {s3_key}")
        except Exception as e:
            logger.error(f"S3 업로드 실패: {e}")
            print(f"S3 업로드 실패: {e}")
            raise
    
    def list_files(self, prefix=''):
        """
        S3 버킷의 파일 목록 조회
        :param prefix: 검색할 접두사 (폴더 경로)
        :return: 파일 키 리스트
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=prefix
            )
            
            if 'Contents' not in response:
                return []
            
            files = [obj['Key'] for obj in response['Contents']]
            logger.info(f"S3 파일 목록 조회 완료: {len(files)}개")
            return files
        except Exception as e:
            logger.error(f"S3 파일 목록 조회 실패: {e}")
            raise
