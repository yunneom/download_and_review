# Copilot 개발 컨텍스트 문서

> **용도**: 다른 PC에서 Copilot과 이어서 작업할 때, 이 문서를 첨부하거나 참조시키면 기존 대화 맥락을 복원할 수 있습니다.
> **마지막 업데이트**: 2026-04-08
> **사용법**: 새 Copilot 대화에서 "이 문서를 읽고 컨텍스트를 파악해줘" 라고 요청하세요.

---

## 1. 프로젝트 개요

- **프로젝트명**: PRDMRT BMS 데이터 검증 도구
- **목적**: S3에서 BMS(Battery Management System) 데이터를 다운로드하고, 35개 항목(71개 세부)을 자동 검증하여 XLSX 리포트를 생성
- **기술 스택**: Python 3.13.2, PyQt5 (GUI), pandas, numpy, boto3, openpyxl, pyarrow
- **실행 방법**: `run.bat` 더블클릭 또는 `.venv\Scripts\python.exe prdmrt_ui_pyqt.py`

---

## 2. 프로젝트 구조

```
download_and_review/
├── prdmrt_ui_pyqt.py       # PyQt5 GUI 메인 (1684줄) - 엔트리포인트
├── prdmrt_core.py           # S3+DataProcessor+Validator 파사드 (124줄)
├── data_validator_bms.py    # BMS 검증 엔진 핵심 (2116줄) ← 주요 개발 대상
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

### 클래스: `BMSDataValidator` (2116줄)

```
validate_file(file_path)
  ├─ CSV/Parquet 로드 (필요 컬럼만 usecols)
  ├─ _get_vehicle_info()
  │    ├─ _load_vehicle_master() ← S3에서 JSON 로드 (클래스 레벨 캐시)
  │    ├─ fleet/vehicle_model 컬럼에서 추출
  │    ├─ list 순회: model_name 매칭 → model_year/model_trim으로 세부 매칭
  │    └─ cell_count(serial_conn_cnt), module_count(module_temp_cnt) 설정
  ├─ _validate_all() ← 35개 항목(1~35) 순차 실행
  │    ├─ _validate_1_unix_time()       # 1-1 수집주기, 1-2 null
  │    ├─ _validate_2_ignit_status()    # 2-1~2-3
  │    ├─ ... (3~15)
  │    ├─ _validate_16_cell_voltages()  # 16-1~16-4 (수집구간 마스크)
  │    ├─ _validate_17~18()
  │    ├─ _validate_19_module_avg_temp()# 19-1~19-2 (수집구간 마스크)
  │    ├─ _validate_20_battery_modules()# 20-1~20-4 (수집구간 마스크)
  │    ├─ _validate_21~27()
  │    ├─ _validate_28_charging_time()  # 28-1 충전 전류 소요시간
  │    ├─ _validate_29_start_end_status()#29-1 시작/종료 status
  │    ├─ _validate_30_sleep_latency()  # 30-1 OBD Sleep Latency
  │    ├─ _validate_31_soh_rate()       # 31-1~31-3
  │    ├─ _validate_32_ign_speed()      # 32-1 IGN*speed
  │    ├─ _validate_33_ign_relay()      # 33-1 IGN*relay
  │    ├─ _validate_34_chg_relay()      # 34-1 chg*relay
  │    └─ _validate_35_chg_condition()  # 35-1 충전 조건
  ├─ self.df = None (메모리 해제)
  └─ return self.results
```

### 핵심 헬퍼 메서드

| 메서드 | 설명 |
|--------|------|
| `_add_result()` | 검증 결과 + 첫 에러 샘플 자동 생성 → Details에 추가 |
| `_get_error_sample_cols()` | 항목별 에러 샘플에 표시할 관련 컬럼 매핑 |
| `_get_collection_window_mask()` | 16/19/20번 수집 구간 마스크 (상태머신 기반) |
| `_get_interval_violations_in_windows()` | 수집 구간 내에서만 interval 위반 검사 |
| `_get_interval_violations()` | 일반 수집 주기 위반 (numpy 벡터 연산) |
| `_split_into_events()` | unix_time 간격 ≥60s 기준 이벤트 분리 |
| `_check_column_exists()` | 컬럼 존재 여부 |
| `generate_report()` | XLSX 리포트 생성 (openpyxl, 7컬럼 A-G) |

### 검증 항목 테이블 (35개, 71개 세부)

| ID | Column | 세부항목 | 비고 |
|----|--------|---------|------|
| 1 | unix_time | 1-1 수집주기(3s~60s), 1-2 null | ≥60s는 다른 key cycle |
| 2 | ignit_status | 2-1 수집주기, 2-2 null, 2-3 값(0/1) | |
| 3 | chg_conr_status_list | 3-1~3-3 | |
| 4 | em_speed_kmh | 4-1~4-3 | VWGKALRT: ≤255 |
| 5 | pack_curr | 5-1~5-3 | |
| 6 | pack_volt | 6-1~6-3 | |
| 7 | main_relay_status | 7-1~7-3 | |
| 8 | soc_display_rate | 8-1~8-3 | 0~100 |
| 9 | soc_rate | 9-1~9-3 | 0.1~100 |
| 10 | mile_km | 10-1~10-3 | Bolt 2017 → N/A |
| 11 | cell_min_volt | 11-1~11-3 | |
| 12 | cell_max_volt | 12-1~12-3 | |
| 13 | cell_volt_dev | 13-1~13-3 | |
| 14 | cell_min_volt_no | 14-1~14-3 | cell_count(N) 기준 |
| 15 | cell_max_volt_no | 15-1~15-3 | cell_count(N) 기준 |
| 16 | cell_voltage_1~N | 16-1~16-4 | **수집구간 마스크** 사용 |
| 17 | module_min_temp | 17-1~17-3 | |
| 18 | module_max_temp | 18-1~18-3 | |
| 19 | module_avg_temp | 19-1~19-2 | **수집구간 마스크** 사용 |
| 20 | battery_module_N_temp | 20-1~20-4 | **수집구간 마스크** 사용 |
| 21 | oper_second | 21-1~21-2 | |
| 22 | acc_chg_ah | 22-1~22-4 | Ray → N/A |
| 23 | acc_dchg_ah | 23-1~23-4 | Ray → N/A, VWGKALRT 음수 허용 |
| 24 | acc_chg_wh | 24-1~24-4 | Ray → N/A |
| 25 | acc_dchg_wh | 25-1~25-4 | Ray → N/A, VWGKALRT 음수 허용 |
| 26 | pack_pwr | 26-1~26-3 | |
| 27 | ir | 27-1~27-2 | |
| 28 | 충전 전류 소요시간 | 28-1 | -5s ≤ time ≤ 25s, VWGKALRT: curr>1 |
| 29 | 시작/종료 status | 29-1 | (1,0,0),(0,0,0),(0,1,0),(1,1,1) 유효 |
| 30 | OBD Sleep Latency | 30-1 | IGN=1/충전중 종료 이벤트 건너뜀 |
| 31 | soh_rate | 31-1~31-3 | (구 32번 → 31로 재번호) |
| 32 | IGN * vehicle speed | 32-1 | (구 33번 → 32로 재번호) |
| 33 | IGN * main relay | 33-1 | (구 34번 → 33으로 재번호) |
| 34 | chg * main relay | 34-1 | (구 35번 → 34로 재번호) |
| 35 | chg 충전 조건 | 35-1 | (구 37번 → 35로 재번호) |

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
| **VWGKALRT** | 16/19/20번: **수집구간 마스크** 기반 검증 (IGN OFF, 충전, sleep 대기 구간) |
| **VWGKALRT** | 28-1 충전 시간 검출: `pack_curr > 1` (방전이 양수) |
| **VWGKALRT** | 35-1 충전 검출: `pack_curr > 1` |
| **기타 Fleet** | 28-1, 35-1 충전 검출: `pack_curr < -1` (충전이 음수) |

### 5.2 수집 구간 마스크 (Collection Window Mask)
**적용 대상**: 16-1(cell_voltage), 19-1(module_avg_temp), 20-1(battery_module_temp)

**상태머신 로직** (`_get_collection_window_mask()`):
```
for each event(key cycle):
    collecting = False
    for each row:
        if collecting and IGN==1 and not charging:
            collecting = False          # 재시동 → 수집 해제
        if not collecting:
            if IGN 1→0 transition:
                collecting = True       # IGN OFF 시작
            if charging (chg in 1|0, 0|1):
                collecting = True       # 충전 중 활성화
            if chg_end (1|0/0|1 → 0|0):
                collecting = True       # 충전 종료 → sleep 대기
        mask[idx] = collecting
```

**처리 시나리오**:
- IGN OFF → 충전 시작 → 충전 종료 → sleep (전 구간 수집)
- IGN OFF → 충전 → 재시동(IGN=1) → 충전 재개 (재시동 구간만 제외)
- 충전 없이 IGN OFF → sleep (전 구간 수집)
- 재시동(IGN=1) 후 14,000+ 행 후 다시 IGN=0 → 별개 구간으로 분리

### 5.3 차종별 N/A 처리
- **Bolt 2017** (`model_name='BOLT-EV'`, `model_year='2017'`): 10-1, 10-2, 10-3 → N/A
- **Ray** (`model_name='RAY'`): 22-1~4, 23-1~4, 24-1~4, 25-1~4 → N/A

### 5.4 차종 매칭 (vehicle_master.json)
- **3단계 매칭**: model_name → model_year → model_trim
- model_name 단일 매칭 시 바로 사용
- 복수 매칭(model_name 동일, model_year/trim 다름) 시 **WARNING** 로그 + 첫 번째 결과 사용
- **ambiguity warning**: 같은 model_name에 여러 serial_conn_cnt/module_temp_cnt가 존재할 때 UI에 경고

### 5.5 충전 관련 검증
- **28-1**: 충전 전류 소요시간 (chg 상태 변경 시점 ± unix_time 검증)
  - 범위: `-5s ≤ 소요시간 ≤ 25s`
  - VWGKALRT: `pack_curr > 1`으로 충전 검출, 기타: `pack_curr < -1`
- **29-1**: 데이터 시작/종료 status 검증
  - 유효 시작 튜플: `(1,0,0)`, `(0,0,0)`, `(0,1,0)`, `(1,1,1)` (ignit, chg, relay)
  - 유효 종료 튜플: `(0,0,0)`, `(0,1,0)`
- **30-1**: OBD Sleep Latency
  - IGN OFF 후 마지막 행까지의 시간 = sleep 진입 지연
  - IGN=1 또는 충전 활성 상태로 끝나는 이벤트는 건너뜀 (midnight cutoff)
- **35-1**: 충전 조건 = `speed=0, relay=1, |curr|>1` → chg 상태 검증
  - VWGKALRT: `curr > 1`, 기타: `curr < -1`

### 5.6 unix_time 수집 주기
- 1-1: `3초 < interval < 60초`만 FAIL
- `interval ≥ 60초`는 다른 key cycle로 판단하여 제외

### 5.7 에러 상세 샘플 (리포트 Details 열)
- FAIL 시 첫 번째 실패 행의 관련 컬럼 값을 자동 추출
- 형식: `| 첫 에러: idx=1742, ignit_status=0, cell_voltage_1=3.82`
- `_get_error_sample_cols()`가 항목별 관련 컬럼 매핑 제공

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
| VWGKALRT(Parquet) | `downloads/2026-03-30/2793/MACRIOT_2793_2026-03-24_v20.parquet` | VWGKALRT | ID5 | 29,045 |

> ID5 데이터(2793 PID)로 Phase 8 디버깅 수행 (cell_count=96, module_count=24)

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

### Phase 7: 수집 구간 마스크 + 항목 재번호 (2026-03-25~)
- 16/19/20번: VWGKALRT 1→0 필터 → **수집구간 마스크** (상태머신) 교체
- `_get_collection_window_mask()`: IGN OFF, 충전, sleep 대기 구간 추적
- `_get_interval_violations_in_windows()`: 같은 수집 세그먼트 내에서만 interval 검사
- 항목 재번호: 32→31(soh_rate), 33→32(IGN*speed), 34→33(IGN*relay), 35→34(chg*relay), 37→35(chg condition)
- 37-2(충전 타입 인식) 삭제 → 총 35개 항목으로 정리
- vehicle_master.json 3단계 매칭 + ambiguity warning 추가

### Phase 8: 수집 구간 디버깅 (2026-03-27~)
- 재시동(IGN=1) 시 `collecting = False`로 수집 구간 분리
- 30-1 sleep latency: IGN=1/충전중으로 끝나는 이벤트 건너뜀 (midnight cutoff)
- 2026-03-24 데이터(VWGKALRT, ID 2793): row 1742→1743 재시동 후 14,238행 수집 구간 분리 확인

### Phase 9: 28-1/29-1 수정 + 에러 상세 (2026-04-07~)
- 29-1: `(1,1,1)` 유효 시작 튜플 추가
- 28-1: 충전 시간 범위 `-5s ≤ time ≤ 25s`, VWGKALRT `pack_curr > 1` 지원
- `_add_result()`: FAIL 시 첫 번째 에러 행의 관련 컬럼 값 자동 추출 → Details에 추가
- `_get_error_sample_cols()`: 항목별 관련 컬럼 매핑 (복합 항목 포함)
- UI 로그 영역: `setMaximumHeight(200)` → `setMinimumHeight(100)` (리사이즈 가능)

---

## 12. 알려진 이슈 / 주의사항

1. **AWS 키 하드코딩**: `prdmrt_ui_pyqt.py`에 직접 기재됨 — 보안 주의
2. **config.py**: 템플릿 상태이며 실제로는 사용되지 않음 (모든 설정이 각 파일에 직접)
3. **README.md**: 초기 버전이라 현재 구조와 다소 다름 (validator.py → data_validator_bms.py 등)
4. **model_year 비교**: JSON의 model_year가 문자열이므로 비교 시 str 변환 필요
5. **.github/ 폴더**: Phase 6 이전 백업본 (구 번호 체계: 32/33/34/35/37). 현재 코드와 다름
6. **항목 번호**: 구 36/37번 삭제/재번호 됨 → 현재 31~35가 최종

---

## 13. Copilot에게 새 대화 시작할 때 프롬프트 예시

```
이 프로젝트의 컨텍스트 문서(COPILOT_CONTEXT.md)를 읽고 파악해줘.
이전 대화에서 이어서 작업할 거야.
현재 작업 중인 파일은 data_validator_bms.py이고,
[여기에 새로운 요청 작성]
```
