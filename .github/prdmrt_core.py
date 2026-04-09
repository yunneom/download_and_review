"""
PRDMRT 코어 모듈
- S3 다운로드 → 파일 변환 → BMS 검증 → 리포트 생성 통합 오케스트레이터
- UI(prdmrt_ui_pyqt.py)와 검증 엔진(data_validator_bms.py)을 연결
"""

from s3_handler import S3Handler
from data_processor import DataProcessor
from data_validator_bms import BMSDataValidator
from logger import logger
from config import FILE_PATHS

class PrdmrtCore:
    """prdmrt 핵심 기능 클래스"""
    
    def __init__(self):
        self.s3_handler = None
        self.data_processor = DataProcessor()
        self.validator = BMSDataValidator()
        
        self.current_df = None
        self.current_issues = []
    
    def init_s3(self, bucket=None, access_key=None, secret_key=None, region=None):
        """S3 핸들러 초기화"""
        self.s3_handler = S3Handler(bucket, access_key, secret_key, region)
        logger.info("S3 핸들러 초기화 완료")
    
    def download_from_s3(self, s3_key, local_path=None):
        """
        S3에서 파일 다운로드
        :param s3_key: S3 파일 경로
        :param local_path: 로컬 저장 경로
        """
        if not self.s3_handler:
            raise RuntimeError("S3 핸들러가 초기화되지 않았습니다. init_s3()를 먼저 호출하세요.")
        
        local_path = local_path or FILE_PATHS['local_parquet']
        self.s3_handler.download_file(s3_key, local_path)
        return local_path
    
    def convert_parquet_to_csv(self, parquet_path=None, csv_path=None):
        """
        Parquet 파일을 CSV로 변환
        :param parquet_path: Parquet 파일 경로
        :param csv_path: CSV 파일 경로
        :return: DataFrame
        """
        parquet_path = parquet_path or FILE_PATHS['local_parquet']
        csv_path = csv_path or FILE_PATHS['local_csv']
        
        self.current_df = self.data_processor.parquet_to_csv(parquet_path, csv_path)
        return self.current_df
    
    def validate_data(self, df=None):
        """
        데이터 검증
        :param df: 검증할 DataFrame (None이면 현재 DataFrame 사용)
        :return: 검증 이슈 리스트
        """
        df = df or self.current_df
        if df is None:
            raise ValueError("검증할 데이터가 없습니다. 먼저 데이터를 로드하세요.")
        
        # BMSDataValidator는 파일 경로 기반이므로, 임시 파일로 저장 후 검증
        import tempfile
        with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as f:
            df.to_csv(f.name, index=False)
            self.current_issues = self.validator.validate_file(f.name)
        return self.current_issues
    
    def save_report(self, report_path=None):
        """
        검증 리포트 저장
        :param report_path: 리포트 파일 경로
        """
        report_path = report_path or FILE_PATHS['report_path']
        self.validator.generate_report(report_path)
        return report_path
    
    def run_full_pipeline(self, s3_key):
        """
        전체 파이프라인 실행
        S3 다운로드 → 변환 → 검증 → 리포트
        :param s3_key: S3 파일 경로
        """
        try:
            logger.info("=== 전체 파이프라인 시작 ===")
            
            # 1. S3 다운로드
            local_path = self.download_from_s3(s3_key)
            
            # 2. Parquet → CSV 변환
            self.convert_parquet_to_csv(local_path)
            
            # 3. 데이터 검증
            self.validate_data()
            
            # 4. 리포트 저장
            report_path = self.save_report()
            
            logger.info("=== 전체 파이프라인 완료 ===")
            print("전체 프로세스 완료")
            
        except Exception as e:
            logger.error(f"파이프라인 실행 실패: {e}")
            print(f"파이프라인 실행 실패: {e}")
            raise

# CLI 모드 실행
if __name__ == "__main__":
    # 사용 예시
    core = PrdmrtCore()
    
    # S3 핸들러 초기화 (config.py의 설정 사용)
    core.init_s3()
    
    # 전체 파이프라인 실행
    # [액션 필요] 실제 S3 파일 경로로 변경
    # core.run_full_pipeline('your-data.parquet')
    
    print("prdmrt 초기화 완료")
    print("core.run_full_pipeline('파일경로')로 실행하세요")
