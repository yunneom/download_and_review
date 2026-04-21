"""
batch_validator.py
S3에서 수십~수백 대 차량 parquet 파일을 병렬로 검증하고
컬럼별 에러 통계 + 분포 정보(percentile)를 집계합니다.

기존 BMSDataValidator 와 독립적으로 동작합니다.
"""

import os
import io
import json
import fnmatch
import boto3
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

# ── 기본 검증 규칙 ────────────────────────────────────────────────
#
# check 종류:
#   "null"        — null 건수를 fail_count 로 집계 (BMSDataValidator null 체크 항목)
#   "range"       — min ≤ 값 ≤ max 범위 검증
#   "valid_values"— 허용 값 목록 검증
#
# column_pattern — 와일드카드(*) 패턴으로 동적 컬럼 확장 (fnmatch 사용)
#   battery_module_*_temperature, cell_voltage_* 등 차량별 개수가 다른 컬럼에 사용
#
# ── 제외 항목 (EXCLUDED_CHECKS 참조) ──
#   pack_volt range   — cell_count 의존 동적 범위 (차종별 상이)
#   pack_pwr          — pack_curr × pack_volt × cell_count 동적 범위 (W 단위 ±수십만)
#   ir                — BMSDataValidator는 수집주기만 체크, 값 범위 없음
#   cell_min/max_volt_no — cell_count 의존 동적 상한
#   수집주기 검증      — 행 간 시간 차이 순차 분석 필요, 파일 단위 통계 불가
#   복합 이벤트 (28~35)— 다중 컬럼 + 이벤트 분할 로직 필요

DEFAULT_RULES = [
    # ── null 체크 (BMSDataValidator 1-2 ~ 6-1 명시적 null FAIL 항목) ──
    {"column": "unix_time",            "check": "null"},
    {"column": "ignit_status",         "check": "null"},
    {"column": "chg_conr_status_list", "check": "null"},
    {"column": "em_speed_kmh",         "check": "null"},
    {"column": "pack_curr",            "check": "null"},
    {"column": "pack_volt",            "check": "null"},

    # ── 범위 검증 ──────────────────────────────────────────────────
    {"column": "soc_display_rate",  "check": "range", "min": 0,        "max": 100},
    {"column": "soc_rate",          "check": "range", "min": 0,        "max": 100},
    {"column": "soh_rate",          "check": "range", "min": 0,        "max": 110},
    # pack_volt range는 동적(cell_count 의존)이라 EXCLUDED_CHECKS에 기재
    {"column": "pack_curr",         "check": "range", "min": -500,     "max": 500},
    # BMS 기준 -20~60; 개별 모듈(battery_module_N_temperature)도 동일 범위
    {"column": "module_min_temp",   "check": "range", "min": -20,      "max": 60},
    {"column": "module_max_temp",   "check": "range", "min": -20,      "max": 60},
    {"column": "module_avg_temp",   "check": "range", "min": -20,      "max": 60},
    # BMS 기준 2.6~4.5
    {"column": "cell_min_volt",     "check": "range", "min": 2.6,      "max": 4.5},
    {"column": "cell_max_volt",     "check": "range", "min": 2.6,      "max": 4.5},
    {"column": "cell_volt_dev",     "check": "range", "min": 0,        "max": 0.5},
    {"column": "em_speed_kmh",      "check": "range", "min": 0,        "max": 300},
    {"column": "mile_km",           "check": "range", "min": 0,        "max": 500000},
    {"column": "acc_chg_ah",        "check": "range", "min": 0,        "max": 9999999},
    {"column": "acc_chg_wh",        "check": "range", "min": 0,        "max": 99999999},
    {"column": "acc_dchg_ah",       "check": "range", "min": -9999999, "max": 9999999},
    {"column": "acc_dchg_wh",       "check": "range", "min": -9999999, "max": 99999999},

    # ── 유효값 검증 ────────────────────────────────────────────────
    {"column": "ignit_status",           "check": "valid_values", "values": [0, 1]},
    {"column": "main_relay_status",      "check": "valid_values", "values": [0, 1]},
    # 실제 데이터 형식: 파이프 구분 문자열 ("0|0", "0|1", "1|0")
    {"column": "chg_conr_status_list",   "check": "valid_values", "values": ["0|0", "0|1", "1|0"]},

    # ── 동적 컬럼 패턴 (fnmatch 와일드카드) ────────────────────────
    # battery_module_1_temperature ~ battery_module_N_temperature (BMS 검증 항목 20-3/4)
    {"column_pattern": "battery_module_*_temperature", "check": "range", "min": -20.0, "max": 60.0},
    # cell_voltage_1 ~ cell_voltage_N (BMS 검증 항목 16-3/4)
    {"column_pattern": "cell_voltage_*", "check": "range", "min": 2.6, "max": 4.3},
]

# ── 배치 검증에서 제외된 BMSDataValidator 항목 ───────────────────
# trend_analyzer HTML 리포트에 이유와 함께 표시됨
EXCLUDED_CHECKS = [
    {
        "bms_id": "1-1",
        "item": "unix_time 수집주기",
        "reason": "행 간 시간 차이 계산이 필요한 순차 분석. 파일 단위 컬럼 통계로 표현 불가.",
    },
    {
        "bms_id": "6-2",
        "item": "pack_volt 범위",
        "reason": "셀 개수(cell_count) 의존 동적 범위 (셀전압 2.6~4.5V × N셀). 차종별 기준이 달라 고정 range 적용 불가.",
    },
    {
        "bms_id": "10-3 / 22-3 / 23-3 / 24-3 / 25-3",
        "item": "mile_km / acc_chg·dchg 선형증가",
        "reason": "행 간 diff 계산이 필요한 순차 분석. 파일 단위 통계로 표현 불가.",
    },
    {
        "bms_id": "13-2",
        "item": "cell_volt_dev 교차검증",
        "reason": "cell_volt_dev = cell_max_volt − cell_min_volt 교차 컬럼 비교 필요. 범위 검증(0~0.5)으로 부분 대체.",
    },
    {
        "bms_id": "14-2 / 15-2",
        "item": "cell_min/max_volt_no 범위",
        "reason": "cell_count 의존 동적 상한 (≤ N셀). 차종별 기준이 달라 고정 range 적용 불가.",
    },
    {
        "bms_id": "16-1 / 19-1 / 20-1",
        "item": "수집구간 마스크 기반 수집주기",
        "reason": "ignit_status + chg_conr_status_list 기반 수집구간 마스크 구성이 필요한 복합 순차 분석.",
    },
    {
        "bms_id": "21-1",
        "item": "oper_second 수집주기",
        "reason": "수집주기만 체크하며 값 범위 없음. 순차 분석 필요.",
    },
    {
        "bms_id": "26",
        "item": "pack_pwr 범위",
        "reason": "pack_curr × pack_volt × cell_count 기반 동적 범위 (W 단위 ±수십만). 차종별 기준 상이.",
    },
    {
        "bms_id": "27",
        "item": "ir 수집주기",
        "reason": "BMSDataValidator는 수집주기만 체크 (값 범위 없음). 순차 분석 필요.",
    },
    {
        "bms_id": "28~35",
        "item": "복합 이벤트 검증 (충전시간·시작종료·Sleep·SOH·IGN속도·릴레이·충전조건)",
        "reason": "여러 컬럼 조합 + 이벤트 분할 로직이 필요한 복합 순차 분석. 단일 컬럼 통계로 표현 불가.",
    },
]

# ── S3 키 패턴 ───────────────────────────────────────────────────
#   BCP: obd_co_id=MACRIOT/pid={pid}/signal_kst_date={date}/
#   D2 : DVAL/d2/{env}/pid_{pid}/{date}/
def parse_s3_key(key):
    """S3 키에서 pid, server_type, date, obd_co_id 추출"""
    meta = {"pid": "unknown", "server_type": "unknown",
            "obd_co_id": "", "date": "", "s3_key": key}
    parts = key.replace("\\", "/").split("/")
    for p in parts:
        if p.startswith("pid="):
            meta["pid"] = p[4:]
            meta["server_type"] = "bcp"
        elif p.startswith("pid_"):
            meta["pid"] = p[4:]
            meta["server_type"] = "d2"
        if p.startswith("obd_co_id="):
            meta["obd_co_id"] = p[10:]
        if p.startswith("signal_kst_date="):
            meta["date"] = p[16:]
        # D2 날짜: DVAL/d2/stag/pid_XXX/2024-01-01/
        if len(p) == 10 and p.count("-") == 2:
            meta["date"] = p
    meta["vehicle_id"] = f"{meta['server_type']}_{meta['pid']}"
    return meta


class BatchValidator:
    """
    S3 버킷의 parquet 파일들을 병렬 검증합니다.

    사용 예:
        v = BatchValidator(bucket='eplat-validation-monitor', rules=DEFAULT_RULES)
        df = v.run(output_path='results/batch_20240101.parquet')
    """

    def __init__(self, bucket, prefix="", rules=None,
                 n_workers=8, aws_config=None):
        self.bucket    = bucket
        self.prefix    = prefix
        self.rules     = rules or DEFAULT_RULES
        self.n_workers = n_workers

        cfg = aws_config or {}
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id     = cfg.get("access_key") or os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key = cfg.get("secret_key") or os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name           = cfg.get("region")     or os.getenv("AWS_REGION", "ap-northeast-2"),
        )

    # ── 파일 목록 수집 ──────────────────────────────────────────

    def list_files(self, prefix=None, extensions=(".parquet",)):
        """S3에서 파일 목록 수집 (페이지네이션 처리)"""
        prefix = prefix if prefix is not None else self.prefix
        files  = []
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if any(key.endswith(ext) for ext in extensions):
                    files.append({"key": key, "size": obj["Size"],
                                  "last_modified": obj["LastModified"]})
        return files

    def discover_pids(self, server_type="bcp", env="stag"):
        """버킷 내 모든 PID 자동 탐색"""
        if server_type == "bcp":
            prefixes = ["obd_co_id=MACRIOT/", "obd_co_id=LGES/"]
        else:
            prefixes = [f"DVAL/d2/{env}/"]

        pids = set()
        for pfx in prefixes:
            resp = self.s3.list_objects_v2(
                Bucket=self.bucket, Prefix=pfx, Delimiter="/"
            )
            for cp in resp.get("CommonPrefixes", []):
                part = cp["Prefix"].rstrip("/").split("/")[-1]
                if part.startswith("pid="):
                    pids.add(part[4:])
                elif part.startswith("pid_"):
                    pids.add(part[4:])
        return sorted(pids)

    def build_prefixes_for_pids(self, pids, start_date, end_date,
                                server_type="bcp", env="stag"):
        """PID 목록 + 날짜 범위 → 검색할 S3 prefix 목록"""
        prefixes = []
        cur = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date,   "%Y-%m-%d")
        dates = []
        while cur <= end:
            dates.append(cur.strftime("%Y-%m-%d"))
            cur += timedelta(days=1)

        for pid in pids:
            for d in dates:
                if server_type == "bcp":
                    prefixes.append(f"obd_co_id=MACRIOT/pid={pid}/signal_kst_date={d}/")
                    prefixes.append(f"obd_co_id=LGES/pid={pid}/signal_kst_date={d}/")
                else:
                    prefixes.append(f"DVAL/d2/{env}/pid_{pid}/{d}/")
        return prefixes

    # ── 단일 파일 검증 ──────────────────────────────────────────

    def _load_df(self, key):
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        buf = io.BytesIO(obj["Body"].read())
        if key.endswith(".parquet"):
            schema_cols = set(pq.read_schema(buf).names)
            buf.seek(0)
            # 고정 컬럼 + 패턴 컬럼 모두 포함
            needed = {r["column"] for r in self.rules if "column" in r}
            for rule in self.rules:
                if "column_pattern" in rule:
                    pat = rule["column_pattern"]
                    needed |= {c for c in schema_cols if fnmatch.fnmatch(c, pat)}
            return pd.read_parquet(buf, columns=list(needed & schema_cols))
        return pd.read_csv(buf)

    def _expand_rules(self, df):
        """column_pattern 규칙을 DataFrame 실제 컬럼으로 확장"""
        expanded = []
        for rule in self.rules:
            if "column_pattern" in rule:
                pat = rule["column_pattern"]
                for col in sorted(c for c in df.columns if fnmatch.fnmatch(c, pat)):
                    expanded.append({**rule, "column": col})
            else:
                expanded.append(rule)
        return expanded

    def _extract_model_info(self, df):
        """DataFrame에서 차종 / fleet 추출"""
        model = "unknown"
        fleet = ""
        for col in ("vehicle_model", "model", "car_model", "vehicle_type"):
            if col in df.columns:
                v = df[col].dropna().iloc[0] if len(df[col].dropna()) > 0 else None
                if v:
                    model = str(v)
                    break
        for col in ("fleet", "fleet_name", "obd_co_id"):
            if col in df.columns:
                v = df[col].dropna().iloc[0] if len(df[col].dropna()) > 0 else None
                if v:
                    fleet = str(v)
                    break
        return model, fleet

    def _validate_df(self, df, meta):
        """DataFrame에 규칙 적용 → 결과 레코드 목록 반환"""
        model, fleet = self._extract_model_info(df)
        total_rows   = len(df)
        records      = []

        for rule in self._expand_rules(df):
            col   = rule["column"]
            check = rule["check"]
            if col not in df.columns:
                continue

            series     = df[col]
            null_count = int(series.isna().sum())
            base = {
                "pid":         meta["pid"],
                "vehicle_id":  meta["vehicle_id"],
                "server_type": meta["server_type"],
                "model":       model,
                "fleet":       fleet,
                "date":        meta["date"],
                "s3_key":      meta["s3_key"],
                "column":      col,
                "check":       check,
                "total_rows":  total_rows,
                "null_count":  null_count,
            }

            if check == "null":
                records.append({
                    **base,
                    "fail_count": null_count,
                    "fail_rate":  round(null_count / total_rows * 100, 4) if total_rows else 0,
                })

            elif check == "range":
                mn, mx    = rule["min"], rule["max"]
                numeric   = pd.to_numeric(series, errors="coerce")
                fail_mask = series.notna() & ((numeric < mn) | (numeric > mx))
                fail_count = int(fail_mask.sum())

                stats = {}
                non_null = numeric.dropna()
                if len(non_null) > 10:
                    qs = non_null.quantile([.01, .05, .25, .50, .75, .95, .99])
                    stats = {
                        "p01": round(float(qs[.01]), 4),
                        "p05": round(float(qs[.05]), 4),
                        "p25": round(float(qs[.25]), 4),
                        "p50": round(float(qs[.50]), 4),
                        "p75": round(float(qs[.75]), 4),
                        "p95": round(float(qs[.95]), 4),
                        "p99": round(float(qs[.99]), 4),
                        "mean":       round(float(non_null.mean()), 4),
                        "std":        round(float(non_null.std()),  4),
                        "actual_min": round(float(non_null.min()),  4),
                        "actual_max": round(float(non_null.max()),  4),
                    }

                records.append({
                    **base,
                    "rule_min":   mn,
                    "rule_max":   mx,
                    "fail_count": fail_count,
                    "fail_rate":  round(fail_count / total_rows * 100, 4) if total_rows else 0,
                    **stats,
                })

            elif check == "valid_values":
                valid = rule.get("values", [])
                try:
                    valid_num = [float(v) for v in valid]
                    numeric   = pd.to_numeric(series, errors="coerce")
                    fail_mask = series.notna() & ~numeric.isin(valid_num)
                except Exception:
                    fail_mask = series.notna() & ~series.isin(valid)
                fail_count = int(fail_mask.sum())

                top_vals = (
                    series.value_counts().head(5).to_dict()
                    if fail_count > 0 else {}
                )

                records.append({
                    **base,
                    "rule_values": str(valid),
                    "fail_count":  fail_count,
                    "fail_rate":   round(fail_count / total_rows * 100, 4) if total_rows else 0,
                    "top_values":  str(top_vals),
                })

        return records

    def validate_one(self, file_info):
        """단일 파일 다운로드 + 검증"""
        key  = file_info["key"]
        meta = parse_s3_key(key)
        try:
            df      = self._load_df(key)
            records = self._validate_df(df, meta)
            return records, None
        except Exception as e:
            return [], f"{type(e).__name__}: {e}"

    # ── 배치 실행 ────────────────────────────────────────────────

    def run(self, pids=None, start_date=None, end_date=None,
            server_type="bcp", env="stag",
            output_path=None, progress_cb=None):
        """
        배치 검증 실행

        pids / start_date / end_date 를 지정하면 해당 범위만 처리.
        지정하지 않으면 self.prefix 로 파일 목록 수집.
        """
        if pids is not None and start_date and end_date:
            print(f"PID {len(pids)}개 · {start_date} ~ {end_date} 대상 파일 목록 수집...")
            prefixes = self.build_prefixes_for_pids(
                pids, start_date, end_date, server_type, env
            )
            files = []
            seen  = set()
            for pfx in prefixes:
                for f in self.list_files(prefix=pfx):
                    if f["key"] not in seen:
                        files.append(f)
                        seen.add(f["key"])
        else:
            print("전체 버킷 스캔...")
            files = self.list_files()

        total = len(files)
        print(f"총 {total}개 파일 발견")
        if total == 0:
            print("처리할 파일 없음.")
            return pd.DataFrame()

        all_records = []
        err_log     = []

        with ThreadPoolExecutor(max_workers=self.n_workers) as executor:
            futures = {executor.submit(self.validate_one, f): f for f in files}
            done = 0
            for future in as_completed(futures):
                records, err = future.result()
                done += 1
                fkey = futures[future]["key"]
                if err:
                    err_log.append({"key": fkey, "error": err})
                    print(f"  [{done:>4}/{total}] ✗ {fkey[-60:]}")
                    print(f"           {err}")
                else:
                    all_records.extend(records)
                    pid_str = parse_s3_key(fkey)["pid"]
                    print(f"  [{done:>4}/{total}] ✓ pid={pid_str}  ({len(records)} rules)")
                if progress_cb:
                    progress_cb(done, total)

        df = pd.DataFrame(all_records)
        print(f"\n집계 완료: {len(df)}행 (차량 {df['vehicle_id'].nunique() if len(df) else 0}대)")

        if output_path and len(df):
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(output_path, index=False)
            print(f"결과 저장: {output_path}")

        if err_log:
            err_path = (output_path or "results/batch").replace(".parquet", "_errors.json")
            Path(err_path).parent.mkdir(parents=True, exist_ok=True)
            with open(err_path, "w", encoding="utf-8") as f:
                json.dump(err_log, f, ensure_ascii=False, indent=2)
            print(f"오류 {len(err_log)}건 기록: {err_path}")

        return df
