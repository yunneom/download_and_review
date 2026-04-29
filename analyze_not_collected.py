"""
analyze_not_collected.py  (pre-development 분석 도구)

prod S3에서 PID 목록의 parquet 파일을 샘플링하여
**차종별 미수집 컬럼**을 자동 탐지합니다.

목적:
  현재 BMSDataValidator는 BOLT 2017 / RAY 두 차종만 하드코딩으로 미수집 처리.
  다른 차종에서 어떤 컬럼이 미수집되는지 정보가 없어 수동 검토 불가능.
  → 실제 prod 데이터 분포로 자동 추론.

분류 기준:
  not_collected_by_design : 모든 샘플 파일에서 ≥99% null   (차종 특성 — 정상)
  mostly_null             : 95~99% null                     (의심 — 간헐적 수집?)
  partially_collected     : 50~95% null                     (수집 비율 낮음)
  collected               : <50% null                        (정상 수집)

사용:
  python analyze_not_collected.py \\
    --bucket {prod_bucket_name} \\
    --start 2026-04-20 --end 2026-04-20 \\
    --output reports/not_collected.json

PID 목록은 PIDS 상수에 하드코딩 (이미지 첨부분).
"""

import os
import io
import json
import argparse
import boto3
import random
import pandas as pd
import pyarrow.parquet as pq
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

# ── 분석 대상 PID (이미지 첨부 기준 43개) ──────────────────────
PIDS = [
    50936, 50968, 51086, 51148, 51297, 52526, 53016, 53023, 53207, 53267,
    53762, 54129, 54188, 54223, 54296, 54438, 54492, 54629, 54687, 54692,
    54735, 54741, 54802, 54830, 54891, 54901, 55050, 55166, 55256, 55273,
    55567, 55592, 55593, 55716, 55879, 55911, 56005, 56935, 57001, 57004,
    57034, 58020, 58149,
]

# ── 분석 대상 컬럼 (BMSDataValidator 검증 컬럼 기준) ─────────────
BASE_COLUMNS = [
    "unix_time", "ignit_status", "chg_conr_status_list", "em_speed_kmh",
    "pack_curr", "pack_volt", "main_relay_status", "soc_display_rate",
    "soc_rate", "mile_km", "cell_min_volt", "cell_max_volt", "cell_volt_dev",
    "cell_min_volt_no", "cell_max_volt_no", "module_min_temp", "module_max_temp",
    "module_avg_temp", "oper_second", "acc_chg_ah", "acc_dchg_ah",
    "acc_chg_wh", "acc_dchg_wh", "pack_pwr", "ir", "soh_rate",
]
# 동적 컬럼은 패턴 매칭으로 별도 집계
DYNAMIC_PATTERNS = ("cell_voltage_", "battery_module_")

META_COLUMNS = ("vehicle_model", "model_year", "model_trim", "fleet", "obd_co_id")


def parse_s3_key(key):
    meta = {"pid": "?", "server_type": "?", "obd_co_id": "", "date": ""}
    parts = key.replace("\\", "/").split("/")
    for p in parts:
        if p.startswith("pid="):
            meta["pid"] = p[4:]; meta["server_type"] = "bcp"
        elif p.startswith("pid_"):
            meta["pid"] = p[4:]; meta["server_type"] = "d2"
        if p.startswith("obd_co_id="):
            meta["obd_co_id"] = p[10:]
        if p.startswith("signal_kst_date="):
            meta["date"] = p[16:]
        if len(p) == 10 and p.count("-") == 2:
            meta["date"] = p
    return meta


def make_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id     = os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name           = os.getenv("AWS_REGION", "ap-northeast-2"),
    )


def list_files_for_pid(s3, bucket, pid, dates, server_type="bcp", env="prod"):
    """PID + 날짜 범위에 해당하는 parquet 파일 목록"""
    keys = []
    for d in dates:
        if server_type == "bcp":
            prefixes = [
                f"obd_co_id=MACRIOT/pid={pid}/signal_kst_date={d}/",
                f"obd_co_id=LGES/pid={pid}/signal_kst_date={d}/",
            ]
        else:
            prefixes = [f"DVAL/d2/{env}/pid_{pid}/{d}/"]

        for pfx in prefixes:
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=pfx):
                for obj in page.get("Contents", []):
                    if obj["Key"].endswith(".parquet"):
                        keys.append(obj["Key"])
    return keys


def analyze_file(s3, bucket, key):
    """단일 parquet 파일 분석 → 컬럼별 null_rate + meta"""
    obj = s3.get_object(Bucket=bucket, Key=key)
    buf = io.BytesIO(obj["Body"].read())

    # 스키마에서 모든 컬럼 추출
    schema_cols = list(pq.read_schema(buf).names)
    buf.seek(0)

    # 분석 대상 컬럼만 로드 (메모리 절약)
    target_cols = [c for c in schema_cols
                   if c in BASE_COLUMNS
                   or c in META_COLUMNS
                   or any(c.startswith(p) for p in DYNAMIC_PATTERNS)]

    df = pd.read_parquet(buf, columns=target_cols)
    if len(df) == 0:
        return None

    # 메타 정보 추출
    meta = {}
    for c in META_COLUMNS:
        if c in df.columns:
            v = df[c].dropna()
            meta[c] = str(v.iloc[0]) if len(v) > 0 else None
        else:
            meta[c] = None

    # 컬럼별 null_rate
    total = len(df)
    null_rates = {}
    for col in df.columns:
        if col in META_COLUMNS:
            continue
        nc = int(df[col].isna().sum())
        null_rates[col] = round(nc / total * 100, 2)

    return {
        "key":         key,
        "total_rows":  total,
        "schema_cols": schema_cols,
        "meta":        meta,
        "null_rates":  null_rates,
    }


def model_key(meta):
    """차종 식별자 (model_name + year)"""
    name = (meta.get("vehicle_model") or "UNKNOWN").upper()
    year = meta.get("model_year") or ""
    return f"{name}_{year}".rstrip("_")


def aggregate(samples):
    """
    파일별 분석 결과를 차종 단위로 집계
    Returns: {model_key: {column: [null_rate per file], "files": [...], "fleet": ...}}
    """
    by_model = defaultdict(lambda: {
        "files": [], "fleet": set(), "null_lists": defaultdict(list),
        "schema_seen": defaultdict(int), "total_files": 0,
    })

    for s in samples:
        if s is None: continue
        mk = model_key(s["meta"])
        rec = by_model[mk]
        rec["files"].append(s["key"])
        rec["total_files"] += 1
        if s["meta"].get("fleet"):
            rec["fleet"].add(s["meta"]["fleet"])

        # 스키마에 컬럼이 있는지 카운트
        for c in s["schema_cols"]:
            rec["schema_seen"][c] += 1

        # null_rate 누적 (스키마에 있어야만 수집 가능 → 없으면 미집계)
        for col, nr in s["null_rates"].items():
            rec["null_lists"][col].append(nr)

    return by_model


def classify(by_model):
    """
    차종별로 컬럼을 분류
    Returns: {model_key: {"not_collected": [...], "mostly_null": [...],
                          "partially": [...], "collected": [...]}}
    """
    result = {}
    for mk, rec in by_model.items():
        n_files = rec["total_files"]
        # 동적 컬럼 그룹화 (cell_voltage_*, battery_module_*_temperature)
        cell_volt_cnt    = sum(1 for c in rec["schema_seen"]
                               if c.startswith("cell_voltage_"))
        battery_mod_cnt  = sum(1 for c in rec["schema_seen"]
                               if c.startswith("battery_module_") and c.endswith("_temperature"))

        not_collected = []          # ≥99% null in ≥80% of files
        mostly_null   = []          # 95~99% null
        partially     = []          # 50~95% null
        collected     = []          # <50% null
        schema_missing = []         # 스키마에도 없음

        all_cols = set(BASE_COLUMNS) | set(rec["null_lists"].keys())
        for col in sorted(all_cols):
            seen_cnt = rec["schema_seen"].get(col, 0)
            null_list = rec["null_lists"].get(col, [])

            if seen_cnt == 0:
                schema_missing.append(col)
                continue

            if not null_list:
                continue

            # 99% 이상 null인 파일 비율
            high_null_ratio = sum(1 for nr in null_list if nr >= 99.0) / len(null_list)
            avg_null = sum(null_list) / len(null_list)

            if high_null_ratio >= 0.8:
                not_collected.append({
                    "column": col, "avg_null": round(avg_null, 2),
                    "files": len(null_list),
                })
            elif avg_null >= 95.0:
                mostly_null.append({
                    "column": col, "avg_null": round(avg_null, 2),
                    "files": len(null_list),
                })
            elif avg_null >= 50.0:
                partially.append({
                    "column": col, "avg_null": round(avg_null, 2),
                    "files": len(null_list),
                })
            else:
                collected.append({
                    "column": col, "avg_null": round(avg_null, 2),
                    "files": len(null_list),
                })

        result[mk] = {
            "fleet":           sorted(rec["fleet"]),
            "files_analyzed":  n_files,
            "cell_voltage_count":  cell_volt_cnt,
            "battery_module_count": battery_mod_cnt,
            "not_collected_by_design": not_collected,
            "mostly_null":             mostly_null,
            "partially_collected":     partially,
            "collected":               collected,
            "schema_missing":          schema_missing,
        }
    return result


def to_simple_mapping(classification):
    """
    BMSDataValidator/batch_validator 에서 바로 쓸 수 있는 단순 매핑
    {model_key: [not_collected_columns]}
    """
    out = {}
    for mk, c in classification.items():
        cols = [x["column"] for x in c["not_collected_by_design"]]
        if cols:
            out[mk] = cols
    return out


def render_html(classification, output_path, args):
    rows_html = ""
    for mk, c in sorted(classification.items()):
        nc_html = "".join(
            f'<li><code>{x["column"]}</code> '
            f'<span class="muted">({x["avg_null"]}% null, {x["files"]}파일)</span></li>'
            for x in c["not_collected_by_design"]
        ) or '<li class="muted">(없음)</li>'

        mn_html = "".join(
            f'<li><code>{x["column"]}</code> '
            f'<span class="muted">({x["avg_null"]}% null)</span></li>'
            for x in c["mostly_null"]
        ) or '<li class="muted">(없음)</li>'

        pt_html = "".join(
            f'<li><code>{x["column"]}</code> '
            f'<span class="muted">({x["avg_null"]}% null)</span></li>'
            for x in c["partially_collected"]
        ) or '<li class="muted">(없음)</li>'

        sm_html = "".join(
            f'<li><code>{c}</code></li>' for c in c["schema_missing"]
        ) or '<li class="muted">(없음)</li>'

        rows_html += f"""
<div class="card">
  <h2>{mk} <span class="badge">{c['files_analyzed']}파일</span></h2>
  <div class="meta">
    fleet: {', '.join(c['fleet']) or '-'} ·
    cell_voltage: {c['cell_voltage_count']}개 ·
    battery_module: {c['battery_module_count']}개
  </div>
  <div class="grid">
    <div>
      <h3 class="t-red">🚫 차종 미수집 추정 (≥99% null)</h3>
      <ul>{nc_html}</ul>
    </div>
    <div>
      <h3 class="t-orange">⚠️ 의심 (95~99% null)</h3>
      <ul>{mn_html}</ul>
    </div>
    <div>
      <h3 class="t-yellow">📉 부분 수집 (50~95% null)</h3>
      <ul>{pt_html}</ul>
    </div>
    <div>
      <h3 class="t-gray">📭 스키마 자체 없음</h3>
      <ul>{sm_html}</ul>
    </div>
  </div>
</div>
"""

    html = f"""<!DOCTYPE html>
<html lang="ko"><head><meta charset="UTF-8">
<title>차종별 미수집 컬럼 자동 탐지 (pre-dev)</title>
<style>
body {{ font-family:'Segoe UI','Malgun Gothic',sans-serif; background:#0d1b2a; color:#e8f0fe;
       margin:0; padding:24px; }}
h1 {{ font-size:20px; margin-bottom:6px; }}
.subtitle {{ color:#8a9abb; font-size:12px; margin-bottom:24px; }}
.card {{ background:#132236; border-radius:10px; padding:20px 24px; margin-bottom:18px; }}
.card h2 {{ font-size:16px; color:#7ab3f5; margin-bottom:6px; }}
.badge {{ background:rgba(0,196,160,0.18); color:#00c4a0;
          padding:2px 9px; border-radius:9px; font-size:11px; margin-left:6px; font-weight:600; }}
.meta {{ color:#8a9abb; font-size:11px; margin-bottom:14px; }}
.grid {{ display:grid; grid-template-columns:1fr 1fr; gap:18px; }}
h3 {{ font-size:12px; margin-bottom:8px; }}
.t-red {{ color:#ff5a5a; }}
.t-orange {{ color:#f5a623; }}
.t-yellow {{ color:#c9a030; }}
.t-gray {{ color:#8a9abb; }}
ul {{ margin:0; padding-left:18px; font-size:12px; }}
li {{ margin-bottom:3px; }}
code {{ font-family:Consolas,monospace; color:#e8f0fe; background:rgba(255,255,255,0.04);
        padding:1px 5px; border-radius:3px; }}
.muted {{ color:#8a9abb; font-size:11px; }}
</style>
</head><body>
<h1>📊 차종별 미수집 컬럼 자동 탐지 (pre-dev)</h1>
<div class="subtitle">
  bucket={args.bucket} · {args.start} ~ {args.end} · PID {len(PIDS)}개 ·
  생성 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
</div>
{rows_html}
</body></html>"""
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", default=os.getenv("S3_BUCKET", "eplat-validation-monitor"),
                        help="prod S3 버킷명 (env:S3_BUCKET)")
    parser.add_argument("--start", required=True, help="시작 날짜 YYYY-MM-DD")
    parser.add_argument("--end",   required=True, help="종료 날짜 YYYY-MM-DD")
    parser.add_argument("--server", choices=["bcp", "d2"], default="bcp")
    parser.add_argument("--env",    default="prod")
    parser.add_argument("--samples-per-pid", type=int, default=2,
                        help="PID당 분석할 파일 수 (기본 2)")
    parser.add_argument("--output", default=None,
                        help="JSON 출력 경로 (미지정 시 reports/not_collected_{ts}.json)")
    args = parser.parse_args()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_json = args.output or f"reports/not_collected_{ts}.json"
    output_html = output_json.replace(".json", ".html")

    # 날짜 목록
    cur = datetime.strptime(args.start, "%Y-%m-%d")
    end = datetime.strptime(args.end,   "%Y-%m-%d")
    dates = []
    while cur <= end:
        dates.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)

    print(f"버킷: {args.bucket}  ·  날짜: {args.start} ~ {args.end}  ·  PID {len(PIDS)}개")
    print(f"PID당 샘플: {args.samples_per_pid}개")
    print()

    s3 = make_s3_client()
    samples = []
    for i, pid in enumerate(PIDS, 1):
        keys = list_files_for_pid(s3, args.bucket, pid, dates,
                                  server_type=args.server, env=args.env)
        if not keys:
            print(f"  [{i:>3}/{len(PIDS)}] pid={pid:<6} ⚠ 파일 없음")
            continue

        # 샘플링 (random)
        chosen = random.sample(keys, min(args.samples_per_pid, len(keys)))
        for k in chosen:
            try:
                s = analyze_file(s3, args.bucket, k)
                if s:
                    samples.append(s)
            except Exception as e:
                print(f"           ✗ {k[-50:]}  {type(e).__name__}: {e}")
        print(f"  [{i:>3}/{len(PIDS)}] pid={pid:<6} ✓ {len(chosen)}/{len(keys)}파일 분석")

    if not samples:
        print("\n분석 가능한 파일이 없습니다.")
        return

    print(f"\n총 {len(samples)}개 파일 분석 완료")

    by_model        = aggregate(samples)
    classification  = classify(by_model)
    simple_mapping  = to_simple_mapping(classification)

    output = {
        "generated_at":     datetime.now().isoformat(),
        "bucket":           args.bucket,
        "date_range":       [args.start, args.end],
        "pid_count":        len(PIDS),
        "total_samples":    len(samples),
        "models_found":     sorted(classification.keys()),
        "simple_mapping":   simple_mapping,   # batch_validator에 바로 쓰기 좋은 형식
        "classification":   classification,   # 상세 분석
    }

    Path(output_json).parent.mkdir(parents=True, exist_ok=True)
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2, default=str)
    print(f"JSON 결과: {output_json}")

    render_html(classification, output_html, args)
    print(f"HTML 리포트: {output_html}")

    # 콘솔 요약
    print("\n" + "=" * 64)
    print("차종별 미수집 추정 요약")
    print("=" * 64)
    for mk, cols in simple_mapping.items():
        print(f"  {mk:<22} → {cols}")
    if not simple_mapping:
        print("  (감지된 미수집 컬럼 없음)")
    print()


if __name__ == "__main__":
    main()
