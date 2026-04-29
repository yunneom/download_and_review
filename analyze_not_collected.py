"""
analyze_not_collected.py  (pre-development 1회성 종합 분석 도구)

prod S3에서 PID 목록의 parquet 파일을 샘플링하여
**차종별 데이터 특성과 검토 필요 항목**을 자동 탐지합니다.

본 도구는 메인 파이프라인(batch_validator/trend_analyzer)과 독립적인 1회성 분석용입니다.
탐지 결과를 검토한 뒤, 필요 시 메인 파이프라인에 매핑/규칙으로 반영하면 됩니다.

분석 항목:
  1. 컬럼 가용성       — 스키마 누락 / 100% null / 95~99% null / 부분 수집
  2. 고정값 (stuck)    — 단일 값이 99% 이상 차지 (예: ignit_status가 항상 0 → IGN 미수신 의심)
  3. 범위 위반         — DEFAULT_RULES 기준 5% 이상 위반
  4. 유효값 위반       — valid_values 5% 이상 위반
  5. 분포 통계         — 차종 간 비교용 (mean, p01, p50, p99)

차종별 검토 필요도 분류:
  CRITICAL : 즉시 검토 필요 (스키마 누락, stuck 값, ≥5% 범위/유효값 위반)
  WARNING  : 의심 (95~99% null, 부분 수집)
  INFO     : 통계 참고 (분포)

사용:
  python analyze_not_collected.py \\
    --bucket {prod_bucket_name} \\
    --start 2026-04-20 --end 2026-04-20 \\
    --samples-per-pid 2

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

# 메인 코드의 DEFAULT_RULES 재사용 (검증 기준 일관성)
from batch_validator import DEFAULT_RULES

# ── 분석 대상 PID (이미지 첨부 기준) ───────────────────────────────
PIDS = [
    50936, 50968, 51086, 51148, 51297, 52526, 53016, 53023, 53207, 53267,
    53762, 54129, 54188, 54223, 54296, 54438, 54492, 54629, 54687, 54692,
    54735, 54741, 54802, 54830, 54891, 54901, 55050, 55166, 55256, 55273,
    55567, 55592, 55593, 55716, 55879, 55911, 56005, 56935, 57001, 57004,
    57034, 58020, 58149,
]

# ── 분석 대상 컬럼 (사용자 첨부 이미지 기준 전체 59개 모니터링 컬럼) ──
# DEFAULT_RULES에 없는 컬럼도 null / stuck / 분포 분석은 수행
ALL_MONITORED_COLUMNS = [
    # 식별/시간
    "pid", "signal_kst_ts", "unix_time",
    # 핵심 BMS 신호
    "pack_curr", "pack_volt", "soc_display_rate", "soc_rate", "soh_rate", "pack_capa",
    "module_min_temp", "module_max_temp", "module_avg_temp",
    # 셀 / 모듈 / 리스트
    "cell_volt_list", "module_temp_list", "temp_list",
    "cell_max_volt", "cell_max_volt_no", "cell_min_volt", "cell_min_volt_no",
    "cell_volt_dev",
    "cell_max_soh_rate", "cell_max_soh_no", "cell_min_soh_rate", "cell_min_soh_no",
    # 상태/릴레이
    "main_relay_status", "ignit_status", "chg_conr_status_list",
    "fast_chg_status", "fast_chg_relay_status",
    "fan_status", "airbag_status",
    "break_status", "gaspedal_status", "mission_status",
    # 누적 / 운행
    "oper_second", "mile_km", "em_speed_kmh",
    "acc_chg_ah", "acc_dchg_ah", "acc_chg_wh", "acc_dchg_wh", "acc_use_wh",
    # 절연/전력
    "ir", "pack_pwr",
    "allow_chg_pwr", "allow_dchg_pwr",
    "allow_obc_pwr", "allow_obc_curr", "allow_obc_volt",
    "obc_curr", "obc_volt",
    # 보조/기타
    "aux_battery_volt", "pack_cap_volt", "fan_hz", "chg_est_second",
    # 위치
    "lat", "lng",
    # 리스트형 기타
    "hvac_list", "motor_rpm_list",
]

# 하위호환 — 다른 코드가 BASE_COLUMNS를 참조하는 경우 대비
BASE_COLUMNS = ALL_MONITORED_COLUMNS

DYNAMIC_PATTERNS = ("cell_voltage_", "battery_module_")
META_COLUMNS = ("vehicle_model", "model_year", "model_trim", "fleet", "obd_co_id")

# stuck 검사가 무의미한 컬럼:
#   pid                  — 파일당 단일값 (식별자)
#   signal_kst_ts/unix_time — 매 행 증가하는 시간
#   lat/lng              — 위치 (정차 중이면 100% stuck — 정상)
#   counter류            — 누적값/운행시간 (단조 증가 → stuck=정상)
SKIP_STUCK_CHECK = {
    "pid", "signal_kst_ts", "unix_time",
    "lat", "lng",
    "oper_second", "mile_km", "chg_est_second",
    "acc_chg_ah", "acc_dchg_ah", "acc_chg_wh", "acc_dchg_wh", "acc_use_wh",
}

# DEFAULT_RULES 인덱스화 (col -> rule)
RANGE_RULES = {r["column"]: r for r in DEFAULT_RULES if r.get("check") == "range" and "column" in r}
VALID_RULES = {r["column"]: r for r in DEFAULT_RULES if r.get("check") == "valid_values" and "column" in r}

# 임계값
STUCK_THRESHOLD     = 0.98   # 단일 값 비율 ≥ 98% (부동소수점 마진 포함)
RANGE_VIOL_WARN     = 5.0    # 범위 위반 ≥ 5% → critical
VALID_VIOL_WARN     = 5.0    # 유효값 위반 ≥ 5% → critical
NOT_COLLECTED_NULL  = 99.0   # null ≥ 99% in ≥ 80% files
NOT_COLLECTED_RATIO = 0.8
SUSPICIOUS_NULL_LO  = 95.0   # 95~99% null → warning
SUSPICIOUS_NULL_HI  = 99.0


# ── S3 ────────────────────────────────────────────────────────────

def make_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id     = os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name           = os.getenv("AWS_REGION", "ap-northeast-2"),
    )


def list_files_for_pid(s3, bucket, pid, dates, server_type="bcp", env="prod"):
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


# ── 단일 파일 분석 ────────────────────────────────────────────────

def analyze_file(s3, bucket, key):
    """파일 → 컬럼별 통계 (null/stuck/range/valid/distribution)"""
    obj = s3.get_object(Bucket=bucket, Key=key)
    buf = io.BytesIO(obj["Body"].read())
    schema_cols = list(pq.read_schema(buf).names)
    buf.seek(0)

    target_cols = [c for c in schema_cols
                   if c in ALL_MONITORED_COLUMNS or c in META_COLUMNS
                   or any(c.startswith(p) for p in DYNAMIC_PATTERNS)]
    df = pd.read_parquet(buf, columns=target_cols)
    if len(df) == 0:
        return None

    meta = {c: (str(df[c].dropna().iloc[0]) if c in df.columns and df[c].notna().any() else None)
            for c in META_COLUMNS}

    total = len(df)
    stats = {}   # col -> dict

    for col in df.columns:
        if col in META_COLUMNS:
            continue
        s = df[col]
        nc = int(s.isna().sum())
        nn = s.dropna()
        col_stats = {
            "total":       total,
            "null_count":  nc,
            "null_rate":   round(nc / total * 100, 2),
            "stuck_value": None,
            "stuck_ratio": 0.0,
            "range_violation_rate": None,
            "valid_violation_rate": None,
            "distribution": None,
        }

        # stuck (단일 값 점유율)
        if len(nn) > 0:
            vc = nn.value_counts(normalize=True)
            top_v = vc.index[0]
            top_r = float(vc.iloc[0])
            col_stats["stuck_value"] = (str(top_v) if not isinstance(top_v, (int, float))
                                        else top_v)
            col_stats["stuck_ratio"] = round(top_r, 4)

        # range 위반
        if col in RANGE_RULES and len(nn) > 0:
            r = RANGE_RULES[col]
            num = pd.to_numeric(s, errors="coerce")
            mask = s.notna() & ((num < r["min"]) | (num > r["max"]))
            col_stats["range_violation_rate"] = round(int(mask.sum()) / total * 100, 2)
            col_stats["range_rule"] = [r["min"], r["max"]]

        # valid_values 위반
        if col in VALID_RULES and len(nn) > 0:
            r = VALID_RULES[col]
            valid = r.get("values", [])
            try:
                vn = [float(v) for v in valid]
                num = pd.to_numeric(s, errors="coerce")
                mask = s.notna() & ~num.isin(vn)
            except Exception:
                mask = s.notna() & ~s.astype(str).isin([str(v) for v in valid])
            col_stats["valid_violation_rate"] = round(int(mask.sum()) / total * 100, 2)
            col_stats["valid_rule"] = list(valid)

        # 수치 분포
        if len(nn) > 10:
            try:
                num = pd.to_numeric(s, errors="coerce").dropna()
                if len(num) > 10:
                    qs = num.quantile([.01, .50, .99])
                    col_stats["distribution"] = {
                        "p01":  round(float(qs[.01]), 4),
                        "p50":  round(float(qs[.50]), 4),
                        "p99":  round(float(qs[.99]), 4),
                        "min":  round(float(num.min()), 4),
                        "max":  round(float(num.max()), 4),
                        "mean": round(float(num.mean()), 4),
                    }
            except Exception:
                pass

        stats[col] = col_stats

    return {
        "key":         key,
        "total_rows":  total,
        "schema_cols": schema_cols,
        "meta":        meta,
        "stats":       stats,
    }


# ── 차종 식별 + 집계 ──────────────────────────────────────────────

def model_key(meta):
    name = (meta.get("vehicle_model") or "UNKNOWN").upper()
    year = meta.get("model_year") or ""
    return f"{name}_{year}".rstrip("_")


def aggregate(samples):
    """
    파일별 결과를 차종 단위로 집계
    by_model[mk] = {
        "files": [...],
        "fleet": set,
        "schema_seen": {col: count},
        "col_stats":   {col: list of file_stats},
    }
    """
    by_model = defaultdict(lambda: {
        "files": [], "fleet": set(),
        "schema_seen": defaultdict(int),
        "col_stats":   defaultdict(list),
        "total_rows":  0,
    })
    for s in samples:
        if s is None: continue
        mk = model_key(s["meta"])
        rec = by_model[mk]
        rec["files"].append(s["key"])
        rec["total_rows"] += s["total_rows"]
        if s["meta"].get("fleet"):
            rec["fleet"].add(s["meta"]["fleet"])
        for c in s["schema_cols"]:
            rec["schema_seen"][c] += 1
        for c, cs in s["stats"].items():
            rec["col_stats"][c].append(cs)
    return by_model


def _avg(lst):
    return sum(lst) / len(lst) if lst else 0.0


def classify(by_model):
    """차종별 검토 항목 분류"""
    result = {}
    for mk, rec in by_model.items():
        n_files = len(rec["files"])

        # 동적 컬럼 카운트
        cv_cnt = sum(1 for c in rec["schema_seen"] if c.startswith("cell_voltage_"))
        bm_cnt = sum(1 for c in rec["schema_seen"]
                     if c.startswith("battery_module_") and c.endswith("_temperature"))

        critical, warning, distribution = [], [], {}
        not_collected, mostly_null, partial = [], [], []
        schema_missing = []

        all_cols = set(ALL_MONITORED_COLUMNS) | set(rec["col_stats"].keys())
        for col in sorted(all_cols):
            seen = rec["schema_seen"].get(col, 0)
            stats_list = rec["col_stats"].get(col, [])

            # (a) 스키마 누락 — critical 아님 (차종별로 다를 수 있음)
            # 별도 섹션에 단순 나열만 함
            if seen == 0:
                schema_missing.append(col)
                continue
            if not stats_list:
                continue

            # (b) null 패턴
            null_rates = [x["null_rate"] for x in stats_list]
            high_null_files = sum(1 for nr in null_rates if nr >= NOT_COLLECTED_NULL)
            avg_null = _avg(null_rates)

            if high_null_files / len(null_rates) >= NOT_COLLECTED_RATIO:
                not_collected.append({"column": col, "avg_null": round(avg_null, 2),
                                      "files": len(null_rates)})
                continue
            elif SUSPICIOUS_NULL_LO <= avg_null < SUSPICIOUS_NULL_HI:
                mostly_null.append({"column": col, "avg_null": round(avg_null, 2)})
                warning.append({
                    "type":   "mostly_null",
                    "column": col,
                    "detail": f"평균 null {avg_null:.1f}% ({len(null_rates)}파일) — 간헐적 수집 의심",
                })
            elif 50 <= avg_null < SUSPICIOUS_NULL_LO:
                partial.append({"column": col, "avg_null": round(avg_null, 2)})

            # (c) stuck 값 — 식별자/시간/위치/카운터는 의미 없으므로 스킵
            if col not in SKIP_STUCK_CHECK:
                stuck_ratios = [x["stuck_ratio"] for x in stats_list if x["null_rate"] < 50]
                if stuck_ratios:
                    avg_stuck = _avg(stuck_ratios)
                    if avg_stuck >= STUCK_THRESHOLD:
                        sample_value = next(
                            (x["stuck_value"] for x in stats_list
                             if x["null_rate"] < 50 and x["stuck_value"] is not None),
                            None,
                        )
                        interp = ""
                        if col == "ignit_status" and str(sample_value) == "0":
                            interp = " (IGN 신호 미수신 의심)"
                        elif col == "main_relay_status" and str(sample_value) == "0":
                            interp = " (메인 릴레이 항상 OFF)"
                        elif col == "fan_status" and str(sample_value) == "0":
                            interp = " (쿨링팬 항상 OFF)"
                        elif col == "airbag_status" and str(sample_value) != "0":
                            interp = " (에어백 알람 지속)"
                        critical.append({
                            "type":   "stuck_value",
                            "column": col,
                            "detail": f"단일 값 '{sample_value}'이 {avg_stuck*100:.1f}% 차지{interp}",
                        })

            # (d) 범위 위반
            rv = [x["range_violation_rate"] for x in stats_list
                  if x["range_violation_rate"] is not None]
            if rv:
                avg_rv = _avg(rv)
                if avg_rv >= RANGE_VIOL_WARN:
                    rng = stats_list[0].get("range_rule", "?")
                    critical.append({
                        "type":   "range_violation",
                        "column": col,
                        "detail": f"평균 {avg_rv:.1f}% 범위 위반 (rule: {rng})",
                    })

            # (e) 유효값 위반
            vv = [x["valid_violation_rate"] for x in stats_list
                  if x["valid_violation_rate"] is not None]
            if vv:
                avg_vv = _avg(vv)
                if avg_vv >= VALID_VIOL_WARN:
                    vr = stats_list[0].get("valid_rule", "?")
                    critical.append({
                        "type":   "valid_violation",
                        "column": col,
                        "detail": f"평균 {avg_vv:.1f}% 유효값 위반 (rule: {vr})",
                    })

            # (f) 분포
            dists = [x["distribution"] for x in stats_list if x["distribution"]]
            if dists:
                distribution[col] = {
                    "p01":  round(_avg([d["p01"]  for d in dists]), 4),
                    "p50":  round(_avg([d["p50"]  for d in dists]), 4),
                    "p99":  round(_avg([d["p99"]  for d in dists]), 4),
                    "mean": round(_avg([d["mean"] for d in dists]), 4),
                    "min":  round(min(d["min"] for d in dists), 4),
                    "max":  round(max(d["max"] for d in dists), 4),
                }

        result[mk] = {
            "fleet":               sorted(rec["fleet"]),
            "files_analyzed":      n_files,
            "total_rows":          rec["total_rows"],
            "cell_voltage_count":  cv_cnt,
            "battery_module_count": bm_cnt,
            "critical":            critical,
            "warning":             warning,
            "not_collected_by_design": not_collected,
            "mostly_null":         mostly_null,
            "partially_collected": partial,
            "schema_missing":      schema_missing,
            "distribution":        distribution,
        }
    return result


def to_simple_mapping(classification):
    """차종 → 미수집 컬럼 매핑 (메인 코드에 적용 가능한 형식)"""
    out = {}
    for mk, c in classification.items():
        cols = [x["column"] for x in c["not_collected_by_design"]]
        if cols:
            out[mk] = cols
    return out


# ── HTML 렌더링 ───────────────────────────────────────────────────

def _li(items, fmt):
    return "".join(f"<li>{fmt(x)}</li>" for x in items) or '<li class="muted">(없음)</li>'


def render_html(classification, output_path, args):
    cards = ""
    for mk, c in sorted(classification.items()):
        crit_html = _li(c["critical"], lambda x:
            f'<span class="tag tag-red">{x["type"]}</span> '
            f'<code>{x["column"]}</code> '
            f'<span class="muted">— {x["detail"]}</span>')
        warn_html = _li(c["warning"], lambda x:
            f'<span class="tag tag-orange">{x["type"]}</span> '
            f'<code>{x["column"]}</code> '
            f'<span class="muted">— {x["detail"]}</span>')
        nc_html = _li(c["not_collected_by_design"], lambda x:
            f'<code>{x["column"]}</code> '
            f'<span class="muted">({x["avg_null"]}% null, {x["files"]}파일)</span>')
        partial_html = _li(c["partially_collected"], lambda x:
            f'<code>{x["column"]}</code> '
            f'<span class="muted">({x["avg_null"]}% null)</span>')
        sm_html = _li(c["schema_missing"], lambda x: f'<code>{x}</code>')

        # 분포 테이블
        dist_rows = ""
        for col, d in sorted(c["distribution"].items()):
            dist_rows += (
                f'<tr><td><code>{col}</code></td>'
                f'<td>{d["p01"]}</td><td>{d["p50"]}</td><td>{d["p99"]}</td>'
                f'<td>{d["min"]}</td><td>{d["max"]}</td></tr>'
            )
        if not dist_rows:
            dist_rows = '<tr><td colspan="6" class="muted" style="text-align:center;">분포 데이터 없음</td></tr>'

        cards += f"""
<div class="card">
  <h2>🚙 {mk} <span class="badge">{c['files_analyzed']}파일 · {c['total_rows']:,}행</span></h2>
  <div class="meta">
    fleet: {', '.join(c['fleet']) or '-'} ·
    cell_voltage: {c['cell_voltage_count']}개 ·
    battery_module: {c['battery_module_count']}개
  </div>

  <h3 class="t-red">🔴 CRITICAL — 즉시 검토 필요 ({len(c['critical'])}건)</h3>
  <ul>{crit_html}</ul>

  <h3 class="t-orange">🟡 WARNING — 의심 ({len(c['warning'])}건)</h3>
  <ul>{warn_html}</ul>

  <div class="grid">
    <div>
      <h3 class="t-purple">🚫 차종 미수집 추정 ({len(c['not_collected_by_design'])}개)</h3>
      <ul>{nc_html}</ul>
    </div>
    <div>
      <h3 class="t-yellow">📉 부분 수집 ({len(c['partially_collected'])}개)</h3>
      <ul>{partial_html}</ul>
    </div>
    <div>
      <h3 class="t-gray">📭 스키마 자체 없음 ({len(c['schema_missing'])}개)</h3>
      <ul>{sm_html}</ul>
    </div>
  </div>

  <h3 class="t-blue">📊 수치 분포 (p01 / p50 / p99 / min / max)</h3>
  <table class="dist">
    <thead><tr><th>컬럼</th><th>p01</th><th>p50</th><th>p99</th><th>min</th><th>max</th></tr></thead>
    <tbody>{dist_rows}</tbody>
  </table>
</div>
"""

    html = f"""<!DOCTYPE html>
<html lang="ko"><head><meta charset="UTF-8">
<title>차종별 데이터 특성 자동 탐지 (pre-dev)</title>
<style>
body {{ font-family:'Segoe UI','Malgun Gothic',sans-serif; background:#0d1b2a; color:#e8f0fe;
       margin:0; padding:24px; }}
h1 {{ font-size:20px; margin-bottom:6px; }}
.subtitle {{ color:#8a9abb; font-size:12px; margin-bottom:24px; }}
.card {{ background:#132236; border-radius:10px; padding:22px 26px; margin-bottom:18px; }}
.card h2 {{ font-size:17px; color:#7ab3f5; margin-bottom:6px; }}
.badge {{ background:rgba(0,196,160,0.18); color:#00c4a0; padding:2px 9px;
          border-radius:9px; font-size:11px; margin-left:6px; font-weight:600; }}
.meta {{ color:#8a9abb; font-size:11px; margin-bottom:18px; }}
.grid {{ display:grid; grid-template-columns:1fr 1fr 1fr; gap:16px; margin-top:8px; }}
h3 {{ font-size:12px; margin:14px 0 6px; }}
.t-red {{ color:#ff5a5a; }}  .t-orange {{ color:#f5a623; }}
.t-yellow {{ color:#c9a030; }}  .t-purple {{ color:#c07aee; }}
.t-gray {{ color:#8a9abb; }}  .t-blue {{ color:#7ab3f5; }}
ul {{ margin:0 0 8px; padding-left:18px; font-size:12px; }}
li {{ margin-bottom:4px; }}
code {{ font-family:Consolas,monospace; color:#e8f0fe;
        background:rgba(255,255,255,0.04); padding:1px 5px; border-radius:3px; }}
.muted {{ color:#8a9abb; font-size:11px; }}
.tag {{ font-size:10px; padding:1px 6px; border-radius:3px; font-weight:600;
        font-family:Consolas,monospace; margin-right:4px; }}
.tag-red {{ background:rgba(255,90,90,0.18); color:#ff7a7a; }}
.tag-orange {{ background:rgba(245,166,35,0.18); color:#f5a623; }}
table.dist {{ width:100%; border-collapse:collapse; font-size:11px; margin-top:6px; }}
table.dist th {{ background:rgba(26,107,204,0.15); color:#8a9abb;
                  text-align:left; padding:5px 9px; font-weight:600; }}
table.dist td {{ padding:4px 9px; border-bottom:1px solid rgba(255,255,255,0.03); }}
</style>
</head><body>
<h1>📊 차종별 데이터 특성 자동 탐지 (pre-dev)</h1>
<div class="subtitle">
  bucket={args.bucket} · {args.start} ~ {args.end} · PID {len(PIDS)}개 ·
  생성 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
  <br>분류: 🔴 Critical(즉시 검토) · 🟡 Warning(의심) · 🚫 미수집 추정 · 📉 부분수집 · 📭 스키마없음
</div>
{cards}
</body></html>"""
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)


# ── main ──────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", default=os.getenv("S3_BUCKET", "eplat-validation-monitor"),
                        help="prod S3 버킷명")
    parser.add_argument("--start", required=True, help="시작 날짜 YYYY-MM-DD")
    parser.add_argument("--end",   required=True, help="종료 날짜 YYYY-MM-DD")
    parser.add_argument("--server", choices=["bcp", "d2"], default="bcp")
    parser.add_argument("--env",    default="prod")
    parser.add_argument("--samples-per-pid", type=int, default=2,
                        help="PID당 분석할 파일 수 (기본 2)")
    parser.add_argument("--output", default=None,
                        help="출력 경로 (미지정 시 reports/model_profile_{ts}.json)")
    args = parser.parse_args()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_json = args.output or f"reports/model_profile_{ts}.json"
    out_html = out_json.replace(".json", ".html")

    cur = datetime.strptime(args.start, "%Y-%m-%d")
    end = datetime.strptime(args.end,   "%Y-%m-%d")
    dates = []
    while cur <= end:
        dates.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)

    rule_cols = (set(RANGE_RULES.keys()) | set(VALID_RULES.keys())) & set(ALL_MONITORED_COLUMNS)
    monitor_only = set(ALL_MONITORED_COLUMNS) - rule_cols

    print(f"버킷: {args.bucket}  ·  날짜: {args.start} ~ {args.end}  ·  PID {len(PIDS)}개")
    print(f"PID당 샘플: {args.samples_per_pid}개")
    print(f"분석 대상 컬럼: {len(ALL_MONITORED_COLUMNS)}개 "
          f"(검증 규칙 적용 {len(rule_cols)} · 모니터링만 {len(monitor_only)})")
    print(f"stuck 검사 제외: {len(SKIP_STUCK_CHECK)}개 (식별자/시간/위치/카운터)")
    print()

    s3 = make_s3_client()
    samples = []
    for i, pid in enumerate(PIDS, 1):
        keys = list_files_for_pid(s3, args.bucket, pid, dates,
                                  server_type=args.server, env=args.env)
        if not keys:
            print(f"  [{i:>3}/{len(PIDS)}] pid={pid:<6} ⚠ 파일 없음")
            continue
        chosen = random.sample(keys, min(args.samples_per_pid, len(keys)))
        for k in chosen:
            try:
                s = analyze_file(s3, args.bucket, k)
                if s: samples.append(s)
            except Exception as e:
                print(f"           ✗ {k[-50:]}  {type(e).__name__}: {e}")
        print(f"  [{i:>3}/{len(PIDS)}] pid={pid:<6} ✓ {len(chosen)}/{len(keys)}파일 분석")

    if not samples:
        print("\n분석 가능한 파일이 없습니다."); return

    print(f"\n총 {len(samples)}개 파일 분석 완료")

    by_model       = aggregate(samples)
    classification = classify(by_model)
    simple_mapping = to_simple_mapping(classification)

    output = {
        "generated_at":  datetime.now().isoformat(),
        "bucket":        args.bucket,
        "date_range":    [args.start, args.end],
        "pid_count":     len(PIDS),
        "total_samples": len(samples),
        "models_found":  sorted(classification.keys()),
        "simple_mapping": simple_mapping,
        "classification": classification,
    }

    Path(out_json).parent.mkdir(parents=True, exist_ok=True)
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2, default=str)
    print(f"JSON 결과: {out_json}")

    render_html(classification, out_html, args)
    print(f"HTML 리포트: {out_html}")

    # 콘솔 요약
    print("\n" + "=" * 64)
    print("차종별 검토 항목 요약")
    print("=" * 64)
    for mk, c in sorted(classification.items()):
        crit_n = len(c["critical"]);  warn_n = len(c["warning"])
        nc_n   = len(c["not_collected_by_design"])
        print(f"  {mk:<22} 🔴{crit_n:>2}  🟡{warn_n:>2}  🚫{nc_n:>2}  ({c['files_analyzed']}파일)")
        for x in c["critical"][:5]:
            print(f"    🔴 [{x['type']}] {x['column']} — {x['detail']}")
    print()
    if simple_mapping:
        print("미수집 매핑 (메인 코드 적용 가능):")
        print(json.dumps(simple_mapping, ensure_ascii=False, indent=2))
    print()


if __name__ == "__main__":
    main()
