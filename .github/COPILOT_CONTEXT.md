# Copilot 개발 컨텍스트 문서

> **용도**: 다른 PC에서 Copilot과 이어서 작업할 때, 이 문서를 첨부하거나 참조시키면 기존 대화 맥락을 복원할 수 있습니다.
> **마지막 업데이트**: 2026-03-19
> **사용법**: 새 Copilot 대화에서 "이 문서를 읽고 컨텍스트를 파악해줘" 라고 요청하세요.

---

## 1. 프로젝트 개요

- **프로젝트명**: PRDMRT BMS 데이터 검증 도구
- **목적**: S3에서 BMS(Battery Management System) 데이터를 다운로드하고, 37개 항목을 자동 검증하여 XLSX 리포트를 생성
- **기술 스택**: Python 3.13.2, PyQt5 (GUI), pandas, numpy, boto3, openpyxl, pyarrow
- **실행 방법**: `run.bat` 더블클릭 또는 `.venv\Scripts\python.exe prdmrt_ui_pyqt.py`

---

## 2. 프로젝트 구조

```
download_and_review/
├── prdmrt_ui_pyqt.py       # PyQt5 GUI 메인 (1684줄) - 엔트리포인트
├── prdmrt_core.py           # S3+DataProcessor+Validator 파사드 (124줄)
├── data_validator_bms.py    # BMS 검증 엔진 핵심 (1885줄) ← 주요 개발 대상
├── s3_handler.py            # boto3 S3 래퍼
├── data_processor.py        # Parquet ↔ CSV 변환
├── report_generator.py      # 통합 리포트 Excel 생성
├── config.py                # AWS, 파일경로, 설정 상수
├── logger.py                # 파일 + 콘솔 동시 로깅
├── run.bat                  # 실행 배치파일
├── .venv/                   # Python 가상환경 (공유 불필요)
├── downloads/               # S3에서 다운로드한 데이터 (자동 생성)
└── __pycache__/             # 자동 생성
```

---

## 3. 환경 설정

### AWS 자격증명
```
# .env 파일에서 관리 (git에 포함되지 않음)
AWS_ACCESS_KEY_ID     = (see .env)
AWS_SECRET_ACCESS_KEY = (see .env)
AWS_REGION            = ap-northeast-2
S3_BUCKET             = eplat-validation-monitor
```
- `config.py`에서 `python-dotenv`로 `.env` 파일 로드
- `prdmrt_ui_pyqt.py`는 `config.AWS_CONFIG`에서 참조

### Python 환경
```
Python 3.13.2 (VirtualEnvironment: .venv)
필수 패키지: PyQt5, pandas, numpy, boto3, pyarrow, openpyxl
```

### S3 주요 리소스
- **vehicle_master.json**: `s3://eplat-validation-monitor/tools/download&review/vehicle_master.json`
  - 형식: list of dict (317개 모델)
  - 각 항목: `{model_name, model_year, model_trim, serial_conn_cnt, module_temp_cnt}`
  - model_name은 대문자 (예: 'Q4-ETRON', 'RAY', 'KONA')
  - serial_conn_cnt, module_temp_cnt는 **문자열** → int() 변환 필요

- **BMS 데이터 경로 패턴**:
  - `BCP/MACRIOT/{pid}/...` (우선 시도)
  - `BCP/LGES/{pid}/...` (MACRIOT 없으면 폴백)

---

## 4. data_validator_bms.py 핵심 아키텍처

### 클래스: `BMSDataValidator`

```
validate_file(file_path)
  ├─ CSV/Parquet 로드 (필요 컬럼만 usecols)
  ├─ _get_vehicle_info()
  │    ├─ _load_vehicle_master() ← S3에서 JSON 로드 (클래스 레벨 캐시)
  │    ├─ fleet/vehicle_model 컬럼에서 추출
  │    └─ list 순회로 model_name 매칭 → cell_count, module_count 설정
  ├─ _validate_all() ← 37개 항목 순차 실행
  │    ├─ _validate_1_unix_time()
  │    ├─ _validate_2_ignit_status()
  │    ├─ ... (3~15)
  │    ├─ _validate_16_cell_voltages()     ← VWGKALRT 1→0 필터
  │    ├─ ... (17~18)
  │    ├─ _validate_19_module_avg_temp()   ← VWGKALRT 1→0 필터
  │    ├─ _validate_20_battery_modules()   ← VWGKALRT 1→0 필터
  │    ├─ ... (21~35)
  │    └─ _validate_37_chg_condition()
  ├─ self.df = None (메모리 해제)
  └─ return self.results
```

### 검증 항목별 구조
각 `_validate_N_xxx()` 메서드는 1~4개 세부 항목을 검증:
- **N-1**: 수집 주기 (interval ≤ 20)
- **N-2**: null 체크
- **N-3**: 값 유효성 (범위 검증)
- **N-4**: 기타 (중복, 단조증가 등)

### 결과 포맷
```python
{
    'ID': '16-1',
    'Column': 'cell_voltage_1~N',
    'Check': '수집 주기',
    'Criteria': '...',
    'Status': 'PASS' | 'FAIL' | 'WARNING' | 'N/A',
    'Fail_Count': 0,
    'Details': '정상'
}
```

---

## 5. 주요 비즈니스 로직 (구현 완료)

### 5.1 Fleet 기반 예외 처리

| Fleet | 예외 사항 |
|-------|-----------|
| **VWGKALRT** | acc_dchg_ah/wh: 음수 범위 허용, em_speed_kmh ≤ 255 |
| **VWGKALRT** | 16/19/20번: ignit_status **1→0 상태변경** 시점만 검증 |
| **VWGKALRT** | 37-1 충전 검출: `pack_curr > 1` (방전이 양수) |
| **기타 Fleet** | 37-1 충전 검출: `pack_curr < -1` (충전이 음수) |

### 5.2 VWGKALRT ignit_status 1→0 상태변경 필터
**배경**: VWGKALRT Fleet은 ignit_status=1에서 0으로 바뀔 때만 cell_voltage, module_temp 등을 수집함. ignit_status=1일 때는 데이터가 없는 것이 정상.

**적용 메서드 및 패턴**:
```python
# _validate_16_cell_voltages(), _validate_19_module_avg_temp(), _validate_20_battery_modules()
if self.fleet == 'VWGKALRT' and 'ignit_status' in self.df.columns:
    ignit = self.df['ignit_status']
    state_change_mask = (ignit.shift(1) == 1) & (ignit == 0)  # 1→0
    df_target = self.df[state_change_mask]
    vwg_note = ' (IGN 1→0 변경시점만)'
```

### 5.3 차종별 N/A 처리
- **Bolt 2017** (`model_name='BOLT-EV'`, `model_year='2017'`): 10-1, 10-2, 10-3 → N/A
- **Ray** (`model_name='RAY'`): 22-1~4, 23-1~4, 24-1~4, 25-1~4 → N/A

### 5.4 충전 관련 검증
- **37-1**: Fleet별 충전 구간 검출
  - 조건: `ignit_status=0, em_speed_kmh=0, main_relay_status=1`
  - VWGKALRT: `pack_curr > 1` / 기타: `pack_curr < -1`
- **37-2**: 충전 타입 인식
  - 급속: chg_conr_status_list 변화 `0→1`, 평균전류 절대값 ≥ 40A
  - 완속: chg_conr_status_list 변화 `1→0`, 평균전류 절대값 < 40A
- **35-1**: `chg_conr_status_list` 변화(`1→0` 또는 `0→1`) & `main_relay_status=0`

### 5.5 unix_time 수집 주기
- 1-1: `3초 < interval < 60초`만 FAIL
- `interval ≥ 60초`는 다른 key cycle로 판단하여 제외

---

## 6. 성능 최적화 (구현 완료)

| 최적화 | 설명 |
|--------|------|
| **S3 캐시** | `_vehicle_master_cache` 클래스 변수 → 여러 파일 검증 시 S3 1회만 호출 |
| **CSV 필요 컬럼만 로드** | `usecols`로 436열 → ~278열 (메모리 ~36% 절감) |
| **numpy 벡터화** | `_get_interval_violations()`의 for 루프 → `np.diff` + `np.where` |
| **메모리 해제** | `validate_file()` 종료 시 `self.df = None` |
| **배치 validator 재사용** | UI 배치 실행 시 `shared_validator` 1개로 루프 |

---

## 7. S3 LGES 폴백

UI(`prdmrt_ui_pyqt.py`)에서 S3 다운로드 시:
1. 먼저 `BCP/MACRIOT/{pid}/` 경로 시도
2. 없으면 `BCP/LGES/{pid}/` 경로로 폴백

---

## 8. vehicle_master.json 로드 방식

- **형태**: list of dict (원본 그대로 유지, dict 변환 안 함)
- **매칭**: list를 순회하며 `item['model_name'].upper() == vehicle_model.upper()` 비교
- **캐시**: `BMSDataValidator._vehicle_master_cache` 클래스 변수에 저장
- **serial_conn_cnt / module_temp_cnt**: 문자열이므로 `int()` 변환 필요

---

## 9. 테스트 데이터

| 데이터 | 경로 | Fleet | 차종 | 행수 |
|--------|------|-------|------|------|
| VWGKALRT | `downloads/2026-03-12/2833/MACRIOT_2833_2026-03-11_v20.csv` | VWGKALRT | Q4-ETRON 2025 | 18,401 |
| LGES | `downloads/2026-03-12/55786/MACRIOT_55786_2026-03-10_v20.csv` | LGES | RAY 2024 | 15,941 |

---

## 10. 팀원 공유 시 필요 파일

### 반드시 필요 (8개)
1. `run.bat`
2. `prdmrt_ui_pyqt.py`
3. `prdmrt_core.py`
4. `data_validator_bms.py`
5. `data_processor.py`
6. `s3_handler.py`
7. `logger.py`
8. `config.py`

### 선택
- `report_generator.py` (통합 리포트용)
- `README.md`, `COPILOT_CONTEXT.md` (문서)

### 불필요
- `.venv/`, `__pycache__/`, `downloads/`, `prdmrt.log`

### 새 PC 설정
```bash
python -m venv .venv
.venv\Scripts\pip install PyQt5 pandas numpy boto3 pyarrow openpyxl
run.bat  # 실행
```

---

## 11. 개발 이력 요약 (시간순)

### Phase 1: 초기 구축
- BMS 37개 검증 항목 구현 (1~37번)
- validate_file → _validate_all → 개별 _validate_N_xxx() 구조
- XLSX 리포트 생성 (generate_report)

### Phase 2: 차종 매칭 + Fleet 예외
- vehicle_master.json S3 로드 (list 형태)
- fleet/vehicle_model 컬럼 기반 차종 식별
- serial_conn_cnt(문자열→int), module_temp_cnt(문자열→int) 변환
- VWGKALRT Fleet 예외 처리 (acc_dchg, em_speed, ignit_status)
- 차종별 N/A (Bolt 2017: 10번, Ray: 22~25번)

### Phase 3: S3 LGES 폴백
- UI에서 BCP/MACRIOT 경로 먼저 시도, 없으면 BCP/LGES 폴백

### Phase 4: 충전 검증 세분화
- 37-1: Fleet별 충전 구간 검출 (VWGKALRT: curr>1, 기타: curr<-1)
- 37-2: 급속/완속 타입 인식 (chg 변화 패턴 + 평균전류 기준)
- 35-1: chg 변화(1→0 or 0→1) & main_relay=0 검출
- 1-1: unix_time 주기 3s~60s만 위반 (≥60s는 key cycle)
- 기존 38번(충전타입) 삭제 → 37-2로 통합

### Phase 5: VWGKALRT ignit 상태변경 필터
- 처음 `0→1`로 구현 → 사용자 정정으로 **`1→0`**으로 수정
- 16-1~4 (cell_voltage), 19-1~2 (module_avg_temp), 20-1~4 (battery_module_temp)
- 패턴: `(ignit.shift(1) == 1) & (ignit == 0)`

### Phase 6: 성능 최적화 + JSON list 유지
- S3 vehicle_master.json 클래스 레벨 캐시
- CSV usecols 필요 컬럼만 로드 (436→278열)
- _get_interval_violations numpy 벡터화
- 검증 후 self.df = None 메모리 해제
- UI 배치 실행에서 shared_validator 재사용
- JSON dict 변환 제거 → list 원본 유지

---

## 12. 알려진 이슈 / 주의사항

1. **AWS 키 하드코딩**: `prdmrt_ui_pyqt.py`에 직접 기재됨 — 보안 주의
2. **config.py**: 템플릿 상태이며 실제로는 사용되지 않음 (모든 설정이 각 파일에 직접)
3. **README.md**: 초기 버전이라 현재 구조와 다소 다름 (validator.py → data_validator_bms.py 등)
4. **model_year 비교**: JSON의 model_year가 문자열이므로 비교 시 str 변환 필요

---

## 13. Copilot에게 새 대화 시작할 때 프롬프트 예시

```
이 프로젝트의 컨텍스트 문서(COPILOT_CONTEXT.md)를 읽고 파악해줘.
이전 대화에서 이어서 작업할 거야.
현재 작업 중인 파일은 data_validator_bms.py이고,
[여기에 새로운 요청 작성]
```
