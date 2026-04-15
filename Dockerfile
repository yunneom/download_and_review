FROM python:3.11-slim

WORKDIR /app

# 시스템 패키지
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# 의존성 설치
COPY requirements_web.txt .
RUN pip install --no-cache-dir -r requirements_web.txt

# 소스 복사 (PyQt5 등 데스크톱 전용 파일 제외)
COPY config.py logger.py s3_handler.py data_processor.py \
     data_validator_bms.py bms_report_generator.py prdmrt_core.py \
     dart_web.py vehicle_master.json* ./

# .env는 런타임에 마운트 (빌드 시 포함 금지)
ENV PORT=8080
EXPOSE 8080

CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "2", "--timeout", "300", "dart_web:app"]
