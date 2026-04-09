"""
데이터 처리 모듈
- Parquet ↔ CSV 파일 변환
"""

import pandas as pd
from logger import logger

class DataProcessor:
    """데이터 변환 및 처리 클래스"""
    
    def __init__(self):
        self.df = None
    
    def parquet_to_csv(self, parquet_path, csv_path):
        """
        Parquet 파일을 CSV로 변환
        :param parquet_path: 입력 Parquet 파일 경로
        :param csv_path: 출력 CSV 파일 경로
        :return: 변환된 DataFrame
        """
        try:
            logger.info(f"Parquet → CSV 변환 시작: {parquet_path}")
            print(f"Parquet 파일을 DataFrame으로 변환 중: {parquet_path}")
            
            self.df = pd.read_parquet(parquet_path)
            self.df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            
            logger.info(f"CSV 변환 완료: {csv_path}, 행 수: {len(self.df)}")
            print(f"CSV 파일로 저장 완료: {csv_path} ({len(self.df)}행)")
            
            return self.df
        except Exception as e:
            logger.error(f"Parquet → CSV 변환 실패: {e}")
            print(f"변환 실패: {e}")
            raise
    
    def csv_to_parquet(self, csv_path, parquet_path):
        """
        CSV 파일을 Parquet으로 변환
        :param csv_path: 입력 CSV 파일 경로
        :param parquet_path: 출력 Parquet 파일 경로
        :return: 변환된 DataFrame
        """
        try:
            logger.info(f"CSV → Parquet 변환 시작: {csv_path}")
            print(f"CSV 파일을 DataFrame으로 변환 중: {csv_path}")
            
            self.df = pd.read_csv(csv_path)
            self.df.to_parquet(parquet_path, index=False)
            
            logger.info(f"Parquet 변환 완료: {parquet_path}")
            print(f"Parquet 파일로 저장 완료: {parquet_path}")
            
            return self.df
        except Exception as e:
            logger.error(f"CSV → Parquet 변환 실패: {e}")
            print(f"변환 실패: {e}")
            raise
    
    def get_dataframe(self):
        """현재 DataFrame 반환"""
        return self.df
    
    def load_csv(self, csv_path):
        """CSV 파일 로드"""
        try:
            self.df = pd.read_csv(csv_path)
            logger.info(f"CSV 파일 로드 완료: {csv_path}")
            return self.df
        except Exception as e:
            logger.error(f"CSV 파일 로드 실패: {e}")
            raise
    
    def get_summary(self):
        """데이터 요약 정보 반환"""
        if self.df is None:
            return "데이터가 로드되지 않았습니다."
        
        summary = {
            'rows': len(self.df),
            'columns': len(self.df.columns),
            'column_names': list(self.df.columns),
            'dtypes': self.df.dtypes.to_dict(),
            'missing_values': self.df.isnull().sum().to_dict(),
        }
        return summary
