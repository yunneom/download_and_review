"""
trend_analyzer.py
batch_validator 결과를 분석하여:
  - 차종별 항목 에러율 히트맵
  - 에러율 상위 항목
  - 실제 분포 기반 range 개선 제안
  - HTML 트렌드 리포트
"""

import re
import json
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path

try:
    from batch_validator import EXCLUDED_CHECKS as _DEFAULT_EXCLUDED
except ImportError:
    _DEFAULT_EXCLUDED = []


class TrendAnalyzer:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    @classmethod
    def from_parquet(cls, path):
        return cls(pd.read_parquet(path))

    @classmethod
    def from_csv(cls, path):
        return cls(pd.read_csv(path))

    # ── 분석 메서드 ────────────────────────────────────────────

    def error_rate_pivot(self):
        """차종 × 컬럼 에러율 피벗 (%)"""
        return (
            self.df.groupby(["model", "column"])["fail_rate"]
            .mean()
            .unstack("column")
            .fillna(0)
            .round(2)
        )

    def top_errors(self, n=20):
        """에러율 상위 항목 DataFrame"""
        err = self.df[self.df["fail_rate"] > 0]
        if err.empty:
            return pd.DataFrame()

        def _first_kst(s):
            non_null = s.dropna()
            return non_null.iloc[0] if len(non_null) else None

        agg_kwargs = dict(
            vehicles     =("vehicle_id", "nunique"),
            avg_fail_rate=("fail_rate",   "mean"),
            max_fail_rate=("fail_rate",   "max"),
            total_fails  =("fail_count",  "sum"),
        )
        if "first_fail_kst" in err.columns:
            agg_kwargs["sample_kst"] = ("first_fail_kst", _first_kst)

        return (
            err.groupby(["model", "column", "check"])
            .agg(**agg_kwargs)
            .reset_index()
            .sort_values("avg_fail_rate", ascending=False)
            .head(n)
        )

    # ── 이슈 드릴다운 ──────────────────────────────────────────

    @staticmethod
    def issue_id(model, column, check):
        """(model, column, check) → URL-안전한 anchor ID"""
        s = f"{model}-{column}-{check}"
        return "issue-" + re.sub(r"[^a-zA-Z0-9_-]", "_", str(s))

    def get_top_issues(self, n=15, min_fail_rate=0.5):
        """드릴다운 대상 (model, column, check) 키 목록 (avg_fail_rate 내림차순)"""
        err = self.df[self.df["fail_rate"] >= min_fail_rate]
        if err.empty:
            return []
        avg = (err.groupby(["model", "column", "check"])["fail_rate"]
               .mean().sort_values(ascending=False).head(n))
        return list(avg.index)

    def issue_detail(self, model, column, check, max_pids=10, max_samples=5):
        """특정 (model, column, check) 이슈의 PID/날짜/샘플 드릴다운"""
        df = self.df[
            (self.df["model"]  == model)  &
            (self.df["column"] == column) &
            (self.df["check"]  == check)  &
            (self.df["fail_rate"] > 0)
        ]
        if df.empty:
            return None

        def _first_kst(s):
            nn = s.dropna()
            return nn.iloc[0] if len(nn) else None

        # PID 단위 집계
        pid_kwargs = dict(
            files      =("s3_key",     "nunique"),
            avg_fail   =("fail_rate",  "mean"),
            max_fail   =("fail_rate",  "max"),
            total_fail =("fail_count", "sum"),
            worst_key  =("s3_key",     "first"),
        )
        if "first_fail_kst" in df.columns:
            pid_kwargs["sample_kst"] = ("first_fail_kst", _first_kst)

        pid_agg = (df.groupby(["pid", "vehicle_id"])
                   .agg(**pid_kwargs)
                   .reset_index()
                   .sort_values("avg_fail", ascending=False)
                   .head(max_pids))

        # 날짜별 추이
        date_agg = (df.groupby("date")
                    .agg(avg_fail =("fail_rate",  "mean"),
                         pids     =("pid",        "nunique"),
                         total    =("fail_count", "sum"))
                    .reset_index()
                    .sort_values("date"))

        # Worst 샘플 (조사용 S3 키)
        worst = (df.nlargest(max_samples, "fail_rate")
                 [["pid", "date", "fail_rate", "s3_key"]]
                 .reset_index(drop=True))

        # 통합 통계
        summary = {
            "total_files":     int(df["s3_key"].nunique()),
            "total_vehicles":  int(df["vehicle_id"].nunique()),
            "total_dates":     int(df["date"].nunique()),
            "avg_fail_rate":   round(float(df["fail_rate"].mean()), 2),
            "max_fail_rate":   round(float(df["fail_rate"].max()),  2),
            "total_fails":     int(df["fail_count"].sum()),
        }

        return {
            "summary":       summary,
            "pid_breakdown": pid_agg,
            "date_timeline": date_agg,
            "worst_samples": worst,
        }

    def suggest_ranges(self, min_fail_rate=1.0):
        """
        range 타입 항목 중 에러율 ≥ min_fail_rate% 인 항목에 대해
        실제 데이터 분포(p01~p99)를 기반으로 range 개선안 제안
        """
        need_cols = {"p01", "p99", "actual_min", "actual_max"}
        if not need_cols.issubset(self.df.columns):
            return pd.DataFrame()

        mask = (
            (self.df["check"] == "range") &
            (self.df["fail_rate"] >= min_fail_rate) &
            self.df["p01"].notna()
        )
        rdf = self.df[mask]
        if rdf.empty:
            return pd.DataFrame()

        agg = (
            rdf.groupby(["model", "column"])
            .agg(
                current_min  =("rule_min",    "first"),
                current_max  =("rule_max",    "first"),
                actual_min   =("actual_min",  "min"),
                actual_max   =("actual_max",  "max"),
                p01          =("p01",         "mean"),
                p05          =("p05",         "mean"),
                p50          =("p50",         "mean"),
                p95          =("p95",         "mean"),
                p99          =("p99",         "mean"),
                avg_fail_rate=("fail_rate",   "mean"),
                max_fail_rate=("fail_rate",   "max"),
                vehicles     =("vehicle_id",  "nunique"),
                files        =("s3_key",      "nunique"),
            )
            .reset_index()
        )

        agg["suggested_min"] = agg["p01"].round(3)
        agg["suggested_max"] = agg["p99"].round(3)
        agg["min_delta"]     = (agg["suggested_min"] - agg["current_min"]).round(3)
        agg["max_delta"]     = (agg["suggested_max"] - agg["current_max"]).round(3)

        def _action(row):
            notes = []
            # 현재 하한보다 실데이터가 더 낮음 → 하한 낮춰야 함
            if row["actual_min"] < row["current_min"]:
                notes.append(f"하한 완화 필요 ({row['current_min']} → {row['suggested_min']})")
            # 현재 상한보다 실데이터가 더 높음 → 상한 올려야 함
            if row["actual_max"] > row["current_max"]:
                notes.append(f"상한 완화 필요 ({row['current_max']} → {row['suggested_max']})")
            # 실데이터가 범위보다 훨씬 안쪽에 있음 → 강화 가능
            # current_min/max가 0인 경우 절대값 기준 대신 range 크기 기준 사용
            if not notes:
                range_size = abs(row["current_max"] - row["current_min"]) or 1.0
                threshold  = range_size * 0.1
                if row["min_delta"] < -threshold:
                    notes.append(f"하한 강화 가능 ({row['current_min']} → {row['suggested_min']})")
                if row["max_delta"] < -threshold:
                    notes.append(f"상한 강화 가능 ({row['current_max']} → {row['suggested_max']})")
            return " | ".join(notes) if notes else "미세 조정"

        agg["action"] = agg.apply(_action, axis=1)
        return agg.sort_values("avg_fail_rate", ascending=False)

    # ── HTML 리포트 ────────────────────────────────────────────

    def generate_html_report(self, output_path="reports/trend_report.html",
                             title="BMS 에러 트렌드 분석",
                             excluded_checks=None):
        pivot    = self.error_rate_pivot()
        top_err  = self.top_errors(20)
        sugg     = self.suggest_ranges()
        gen_at   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        excl     = excluded_checks if excluded_checks is not None else _DEFAULT_EXCLUDED

        total_vehicles = int(self.df["vehicle_id"].nunique()) if "vehicle_id" in self.df.columns else 0
        total_models   = int(self.df["model"].nunique())      if "model"      in self.df.columns else 0
        total_files    = int(self.df["s3_key"].nunique())     if "s3_key"     in self.df.columns else 0
        avg_fail_rate  = float(self.df["fail_rate"].mean())   if "fail_rate"  in self.df.columns else 0

        # ── 이슈 드릴다운 데이터 준비 ──
        top_issue_keys = self.get_top_issues(n=15)
        issue_details  = []
        timelines      = {}   # canvas_id → {dates, values}
        for (mdl, col, chk) in top_issue_keys:
            d = self.issue_detail(mdl, col, chk)
            if d is None:
                continue
            iid = self.issue_id(mdl, col, chk)
            issue_details.append({"id": iid, "model": mdl, "column": col, "check": chk,
                                  "detail": d})
            timelines[iid] = {
                "dates":  d["date_timeline"]["date"].astype(str).tolist(),
                "values": d["date_timeline"]["avg_fail"].round(2).tolist(),
                "pids":   d["date_timeline"]["pids"].tolist(),
            }

        # ── Chart.js 데이터 ──
        chart_data = {
            "heatmap": {
                "models": pivot.index.tolist(),
                "cols":   pivot.columns.tolist(),
                "data":   [[round(v, 2) for v in row] for row in pivot.values.tolist()],
            },
            "topErrors": {
                "labels":   (top_err["model"] + " / " + top_err["column"]).tolist() if not top_err.empty else [],
                "values":   top_err["avg_fail_rate"].round(2).tolist()               if not top_err.empty else [],
                "vehicles": top_err["vehicles"].tolist()                              if not top_err.empty else [],
                "sampleKst": (top_err["sample_kst"].fillna("-").tolist()
                              if not top_err.empty and "sample_kst" in top_err.columns else []),
            },
            "timelines": timelines,
        }

        # ── 이슈 카드 렌더링 ──
        def issue_cards():
            if not issue_details:
                return '<div class="muted" style="padding:20px;text-align:center;">에러율 0.5% 이상 이슈 없음</div>'
            html_parts = []
            for it in issue_details:
                d = it["detail"]; sm = d["summary"]
                pid_rows = ""
                for _, r in d["pid_breakdown"].iterrows():
                    kst = r.get("sample_kst") if "sample_kst" in d["pid_breakdown"].columns else None
                    kst_disp = kst if kst and pd.notna(kst) else "-"
                    short_key = str(r["worst_key"])
                    short_key = "..." + short_key[-65:] if len(short_key) > 65 else short_key
                    pid_rows += f"""<tr>
  <td style="color:#7ab3f5;font-weight:600;">{r['pid']}</td>
  <td style="font-size:11px;color:#8a9abb;">{r['vehicle_id']}</td>
  <td style="text-align:center;">{int(r['files'])}</td>
  <td style="text-align:center;color:#ff5a5a;font-weight:700;">{r['avg_fail']:.2f}%</td>
  <td style="text-align:center;color:#f5a623;">{r['max_fail']:.2f}%</td>
  <td style="text-align:center;">{int(r['total_fail']):,}</td>
  <td style="text-align:center;color:#00c4a0;font-family:Consolas,monospace;">{kst_disp}</td>
  <td style="font-size:10px;color:#8a9abb;font-family:Consolas,monospace;" title="{r['worst_key']}">{short_key}</td>
</tr>"""

                worst_rows = ""
                for _, r in d["worst_samples"].iterrows():
                    short_key = str(r["s3_key"])
                    short_key = "..." + short_key[-80:] if len(short_key) > 80 else short_key
                    worst_rows += f"""<tr>
  <td style="color:#7ab3f5;font-weight:600;">{r['pid']}</td>
  <td style="text-align:center;">{r['date']}</td>
  <td style="text-align:center;color:#ff5a5a;font-weight:700;">{r['fail_rate']:.2f}%</td>
  <td style="font-size:10px;color:#8a9abb;font-family:Consolas,monospace;" title="{r['s3_key']}">{short_key}</td>
</tr>"""

                badge_clr = "#ff5a5a" if sm["avg_fail_rate"] >= 5 else "#f5a623" if sm["avg_fail_rate"] >= 1 else "#00c4a0"
                html_parts.append(f"""
<details class="issue-card" id="{it['id']}">
  <summary>
    <span class="issue-badge" style="background:{badge_clr}22;color:{badge_clr};">{sm['avg_fail_rate']:.2f}%</span>
    <span class="issue-title"><b>{it['model']}</b> · <code>{it['column']}</code> · <span class="muted">{it['check']}</span></span>
    <span class="issue-meta">차량 {sm['total_vehicles']}대 · 파일 {sm['total_files']} · 날짜 {sm['total_dates']}일 · 총 {sm['total_fails']:,}건</span>
  </summary>
  <div class="issue-body">
    <div class="issue-grid">
      <div class="issue-pids">
        <div class="card-title">📋 영향 PID Top {len(d['pid_breakdown'])}</div>
        <div class="tbl-wrap" style="max-height:280px;">
          <table>
            <thead><tr>
              <th>PID</th><th>vehicle_id</th>
              <th style="text-align:center;">파일</th>
              <th style="text-align:center;">평균</th>
              <th style="text-align:center;">최대</th>
              <th style="text-align:center;">총 fail</th>
              <th style="text-align:center;">첫 KST</th>
              <th>샘플 키 (worst)</th>
            </tr></thead>
            <tbody>{pid_rows}</tbody>
          </table>
        </div>
      </div>
      <div class="issue-timeline">
        <div class="card-title">📈 날짜별 평균 에러율</div>
        <canvas id="tl-{it['id']}" height="160"></canvas>
      </div>
    </div>
    <div class="issue-worst">
      <div class="card-title" style="margin-top:14px;">🔗 조사용 Worst 샘플 (top {len(d['worst_samples'])})</div>
      <div class="tbl-wrap">
        <table>
          <thead><tr>
            <th>PID</th><th style="text-align:center;">날짜</th>
            <th style="text-align:center;">fail_rate</th><th>S3 키</th>
          </tr></thead>
          <tbody>{worst_rows}</tbody>
        </table>
      </div>
    </div>
  </div>
</details>
""")
            return "\n".join(html_parts)

        # ── range 제안 테이블 ──
        def sugg_table_rows():
            if sugg.empty:
                return '<tr><td colspan="8" style="text-align:center;color:#8a9abb;padding:20px;">에러율 1% 이상인 range 항목 없음</td></tr>'
            rows = ""
            for _, r in sugg.iterrows():
                need_expand = "완화" in str(r["action"])
                action_clr  = "#ff5a5a" if need_expand else "#f5a623"
                rows += f"""<tr>
  <td><span style="color:#7ab3f5;font-weight:600;">{r['model']}</span></td>
  <td style="font-family:Consolas,monospace;font-size:12px;">{r['column']}</td>
  <td style="text-align:center;color:#ff5a5a;font-weight:700;">{r['avg_fail_rate']:.2f}%</td>
  <td style="text-align:center;color:#f5a623;">{r['max_fail_rate']:.2f}%</td>
  <td style="text-align:center;">{int(r['vehicles'])}대 / {int(r['files'])}파일</td>
  <td style="color:#8a9abb;">[{r['current_min']}, {r['current_max']}]</td>
  <td style="color:#00c4a0;font-weight:700;">[{r['suggested_min']}, {r['suggested_max']}]</td>
  <td style="font-size:11px; color:{action_clr};">{r['action']}</td>
</tr>"""
            return rows

        # ── 제외 항목 테이블 ──
        def excluded_rows():
            if not excl:
                return '<tr><td colspan="3" style="text-align:center;color:#8a9abb;padding:20px;">제외 항목 없음</td></tr>'
            rows = ""
            for e in excl:
                rows += f"""<tr>
  <td style="text-align:center;color:#c07aee;font-weight:700;font-family:Consolas,monospace;">{e.get('bms_id','')}</td>
  <td style="color:#e8f0fe;font-weight:600;">{e.get('item','')}</td>
  <td style="color:#8a9abb;font-size:11px;">{e.get('reason','')}</td>
</tr>"""
            return rows

        # ── valid_values 에러 상세 ──
        def valid_val_rows():
            if "check" not in self.df.columns:
                return ""
            vdf = self.df[
                (self.df["check"] == "valid_values") &
                (self.df["fail_rate"] > 0)
            ]
            if vdf.empty:
                return '<tr><td colspan="7" style="text-align:center;color:#8a9abb;padding:20px;">valid_values 에러 없음</td></tr>'
            def _first_kst(s):
                nn = s.dropna()
                return nn.iloc[0] if len(nn) else None

            agg_kwargs = dict(
                vehicles     =("vehicle_id",  "nunique"),
                avg_fail_rate=("fail_rate",   "mean"),
                top_values   =("top_values",  "first"),
            )
            if "first_fail_kst" in vdf.columns:
                agg_kwargs["sample_kst"] = ("first_fail_kst", _first_kst)

            agg = (
                vdf.groupby(["model", "column", "rule_values"])
                .agg(**agg_kwargs)
                .reset_index()
                .sort_values("avg_fail_rate", ascending=False)
                .head(15)
            )
            rows = ""
            for _, r in agg.iterrows():
                kst = r.get("sample_kst") if "sample_kst" in agg.columns else None
                kst_disp = kst if kst and pd.notna(kst) else "-"
                rows += f"""<tr>
  <td style="color:#7ab3f5;font-weight:600;">{r['model']}</td>
  <td style="font-family:Consolas,monospace;font-size:12px;">{r['column']}</td>
  <td style="color:#8a9abb;">{r['rule_values']}</td>
  <td style="text-align:center;color:#ff5a5a;font-weight:700;">{r['avg_fail_rate']:.2f}%</td>
  <td style="text-align:center;">{int(r['vehicles'])}대</td>
  <td style="text-align:center;color:#00c4a0;font-family:Consolas,monospace;">{kst_disp}</td>
  <td style="font-size:11px;color:#8a9abb;font-family:Consolas,monospace;">{r['top_values']}</td>
</tr>"""
            return rows

        html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>{title}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.3/dist/chart.umd.min.js"></script>
<style>
* {{ box-sizing:border-box; margin:0; padding:0; }}
body {{ font-family:'Segoe UI','Malgun Gothic',sans-serif; background:#0d1b2a; color:#e8f0fe; }}
.hdr {{ background:linear-gradient(135deg,#0f3055,#1a2e45); padding:28px 40px 22px;
        border-bottom:1px solid rgba(255,255,255,0.08); }}
.hdr h1 {{ font-size:22px; font-weight:800; margin-bottom:6px; }}
.hdr .meta {{ font-size:12px; color:#8a9abb; }}
.summary {{ display:flex; gap:14px; padding:20px 40px; background:#132236;
            border-bottom:1px solid rgba(255,255,255,0.06); flex-wrap:wrap; align-items:center; }}
.stat {{ text-align:center; padding:12px 22px; border-radius:10px; min-width:100px; }}
.stat .num {{ font-size:32px; font-weight:800; line-height:1; }}
.stat .lbl {{ font-size:11px; margin-top:5px; }}
.section {{ padding:28px 40px; border-bottom:1px solid rgba(255,255,255,0.05); }}
.sec-title {{ font-size:15px; font-weight:700; margin-bottom:18px; color:#7ab3f5; letter-spacing:0.3px; }}
.chart-grid {{ display:grid; grid-template-columns:1fr 1fr; gap:20px; }}
.card {{ background:#132236; border-radius:10px; padding:20px; }}
.card-wide {{ background:#132236; border-radius:10px; padding:20px; grid-column:1/-1; }}
.card-title {{ font-size:11px; font-weight:600; color:#8a9abb; margin-bottom:14px;
               text-transform:uppercase; letter-spacing:0.6px; }}
table {{ width:100%; border-collapse:collapse; font-size:12px; }}
th {{ background:rgba(26,107,204,0.18); color:#00c4a0; font-weight:600; text-align:left;
      padding:9px 12px; font-size:11px; letter-spacing:0.4px; position:sticky; top:0; z-index:1; }}
td {{ padding:8px 12px; border-bottom:1px solid rgba(255,255,255,0.04); vertical-align:middle; }}
tr:hover td {{ background:rgba(255,255,255,0.025); }}
.tbl-wrap {{ overflow-x:auto; max-height:480px; overflow-y:auto; border-radius:8px; }}
.hm-wrap {{ overflow-x:auto; overflow-y:auto; max-height:400px; border-radius:8px; }}
.hm {{ font-size:11px; border-collapse:collapse; white-space:nowrap; }}
.hm th {{ background:#0d1b2a; color:#8a9abb; padding:5px 10px; position:sticky;
           top:0; z-index:2; border-bottom:1px solid rgba(255,255,255,0.08); }}
.hm td {{ padding:5px 8px; text-align:center; border:1px solid rgba(255,255,255,0.03);
           min-width:58px; font-size:10px; }}
.hm .rh {{ color:#e8f0fe; font-weight:600; text-align:left; padding:5px 14px;
            position:sticky; left:0; background:#0d1b2a; z-index:1; }}
.footer {{ text-align:center; padding:16px; font-size:11px; color:#4a6a8a;
           border-top:1px solid rgba(255,255,255,0.05); }}

/* 이슈 드릴다운 카드 */
.issue-card {{ background:#132236; border-radius:10px; margin-bottom:10px;
               border:1px solid rgba(255,255,255,0.04); }}
.issue-card[open] {{ border-color:rgba(122,179,245,0.3); }}
.issue-card summary {{ list-style:none; cursor:pointer; padding:14px 20px;
                        display:flex; align-items:center; gap:14px; flex-wrap:wrap; }}
.issue-card summary::-webkit-details-marker {{ display:none; }}
.issue-card summary::before {{ content:'▶'; color:#7ab3f5; font-size:10px;
                                transition:transform 0.15s; }}
.issue-card[open] summary::before {{ transform:rotate(90deg); }}
.issue-card summary:hover {{ background:rgba(122,179,245,0.04); }}
.issue-badge {{ display:inline-block; padding:3px 10px; border-radius:9px;
                 font-weight:800; font-size:11px; min-width:60px; text-align:center; }}
.issue-title {{ flex:1; font-size:13px; }}
.issue-title code {{ font-family:Consolas,monospace; background:rgba(255,255,255,0.06);
                      padding:1px 6px; border-radius:3px; font-size:12px; }}
.issue-meta {{ font-size:11px; color:#8a9abb; }}
.issue-body {{ padding:0 20px 20px; }}
.issue-grid {{ display:grid; grid-template-columns:1.4fr 1fr; gap:18px; margin-top:6px; }}
.muted {{ color:#8a9abb; font-size:11px; }}
:target.issue-card {{ box-shadow:0 0 0 2px #7ab3f5; }}
@media (max-width: 1024px) {{
  .issue-grid {{ grid-template-columns:1fr; }}
}}
</style>
</head>
<body>

<div class="hdr">
  <h1>📊 BMS 에러 트렌드 분석 리포트</h1>
  <div class="meta">생성: {gen_at} &nbsp;·&nbsp; 차량 {total_vehicles}대 / {total_models}개 차종 / {total_files}개 파일</div>
</div>

<div class="summary">
  <div class="stat" style="background:rgba(122,179,245,0.10);">
    <div class="num" style="color:#7ab3f5;">{total_vehicles}</div>
    <div class="lbl" style="color:#7ab3f5;">분석 차량</div>
  </div>
  <div class="stat" style="background:rgba(0,196,160,0.10);">
    <div class="num" style="color:#00c4a0;">{total_models}</div>
    <div class="lbl" style="color:#00c4a0;">차종</div>
  </div>
  <div class="stat" style="background:rgba(192,122,238,0.10);">
    <div class="num" style="color:#c07aee;">{total_files}</div>
    <div class="lbl" style="color:#c07aee;">파일</div>
  </div>
  <div class="stat" style="background:rgba(255,90,90,0.10);">
    <div class="num" style="color:#ff5a5a;">{avg_fail_rate:.1f}%</div>
    <div class="lbl" style="color:#ff5a5a;">평균 에러율</div>
  </div>
</div>

<!-- 히트맵 -->
<div class="section">
  <div class="sec-title">🌡️ 차종별 항목 에러율 히트맵 (%)</div>
  <div class="hm-wrap" id="heatmap-area"></div>
</div>

<!-- 상위 에러 바 차트 -->
<div class="section">
  <div class="sec-title">🔴 에러율 상위 20개 항목</div>
  <div class="card-wide" style="background:#132236;border-radius:10px;padding:20px;">
    <div class="card-title">항목별 평균 에러율 (%) — 빨강: ≥5%, 주황: 1~5%, 초록: &lt;1%</div>
    <canvas id="topChart" height="80"></canvas>
  </div>
</div>

<!-- range 개선 제안 -->
<div class="section">
  <div class="sec-title">🎯 Range 개선 후보 (에러율 ≥ 1%)</div>
  <div class="tbl-wrap">
    <table>
      <thead>
        <tr>
          <th>차종</th><th>컬럼</th>
          <th style="text-align:center;">평균 에러율</th>
          <th style="text-align:center;">최대 에러율</th>
          <th style="text-align:center;">해당 차량/파일</th>
          <th>현재 범위</th>
          <th>제안 범위 (p01~p99)</th>
          <th>조치</th>
        </tr>
      </thead>
      <tbody>{sugg_table_rows()}</tbody>
    </table>
  </div>
</div>

<!-- valid_values 에러 -->
<div class="section">
  <div class="sec-title">⚠️ Valid-Values 에러 상위 항목</div>
  <div class="tbl-wrap">
    <table>
      <thead>
        <tr>
          <th>차종</th><th>컬럼</th><th>허용값</th>
          <th style="text-align:center;">평균 에러율</th>
          <th style="text-align:center;">해당 차량</th>
          <th style="text-align:center;">첫 에러 시점</th>
          <th>실제 등장값 (빈도순)</th>
        </tr>
      </thead>
      <tbody>{valid_val_rows()}</tbody>
    </table>
  </div>
</div>

<!-- 이슈 드릴다운 -->
<div class="section">
  <div class="sec-title">🔍 이슈 드릴다운 — PID / 날짜 / 샘플 단위 분석</div>
  <div style="font-size:11px;color:#8a9abb;margin-bottom:14px;">
    클릭해서 펼치기. 각 이슈별로 영향받는 PID 목록 · 날짜별 추이 · 조사용 S3 키 샘플을 확인할 수 있습니다.
  </div>
  {issue_cards()}
</div>

<!-- 제외 항목 -->
<div class="section">
  <div class="sec-title">🚫 배치 검증 제외 항목 (BMSDataValidator 검증 대상이나 트렌드 집계 불가)</div>
  <div class="tbl-wrap">
    <table>
      <thead>
        <tr>
          <th style="text-align:center;width:140px;">BMS 항목 ID</th>
          <th>검증 항목</th>
          <th>제외 이유</th>
        </tr>
      </thead>
      <tbody>{excluded_rows()}</tbody>
    </table>
  </div>
</div>

<div class="footer">DART · BMS Trend Analyzer &nbsp;·&nbsp; {gen_at}</div>

<script>
const CD = {json.dumps(chart_data, ensure_ascii=False)};

// ── 히트맵 ──
(function() {{
  const hm = CD.heatmap;
  if (!hm || !hm.models.length) return;

  function cellStyle(v) {{
    if (v === 0)  return 'background:#132236;color:#4a6a8a;';
    if (v < 0.5)  return 'background:rgba(245,166,35,0.10);color:#c9a030;';
    if (v < 2)    return 'background:rgba(245,166,35,0.25);color:#f5a623;font-weight:600;';
    if (v < 10)   return 'background:rgba(255,90,90,0.30);color:#ff7a7a;font-weight:700;';
    return 'background:rgba(255,90,90,0.65);color:#fff;font-weight:700;';
  }}

  let h = '<table class="hm"><thead><tr><th style="position:sticky;left:0;background:#0d1b2a;z-index:3;">차종</th>';
  hm.cols.forEach(c => h += `<th>${{c}}</th>`);
  h += '</tr></thead><tbody>';
  hm.models.forEach((m, mi) => {{
    h += `<tr><td class="rh">${{m}}</td>`;
    hm.data[mi].forEach(v => {{
      h += `<td style="${{cellStyle(v)}}">${{v ? v.toFixed(1) : '0'}}%</td>`;
    }});
    h += '</tr>';
  }});
  h += '</tbody></table>';
  document.getElementById('heatmap-area').innerHTML = h;
}})();

// ── 상위 에러 바 차트 ──
(function() {{
  const te = CD.topErrors;
  if (!te || !te.labels.length) return;
  new Chart(document.getElementById('topChart'), {{
    type: 'bar',
    data: {{
      labels: te.labels,
      datasets: [{{
        label: '평균 에러율 (%)',
        data: te.values,
        backgroundColor: te.values.map(v =>
          v >= 5 ? 'rgba(255,90,90,0.75)' :
          v >= 1 ? 'rgba(245,166,35,0.75)' : 'rgba(0,196,160,0.55)'
        ),
        borderWidth: 0,
        borderRadius: 3,
      }}]
    }},
    options: {{
      indexAxis: 'y',
      responsive: true,
      plugins: {{
        legend: {{ display: false }},
        tooltip: {{
          backgroundColor: '#132236',
          titleColor: '#e8f0fe',
          bodyColor: '#8a9abb',
          callbacks: {{
            afterLabel: ctx => {{
              const lines = [`차량 수: ${{te.vehicles[ctx.dataIndex]}}대`];
              if (te.sampleKst && te.sampleKst[ctx.dataIndex] && te.sampleKst[ctx.dataIndex] !== '-') {{
                lines.push(`첫 에러 시점: ${{te.sampleKst[ctx.dataIndex]}}`);
              }}
              return lines;
            }}
          }}
        }}
      }},
      scales: {{
        x: {{ ticks:{{ color:'#8a9abb' }}, grid:{{ color:'rgba(255,255,255,0.05)' }},
              title:{{ display:true, text:'평균 에러율 (%)', color:'#4a6a8a', font:{{size:11}} }} }},
        y: {{ ticks:{{ color:'#e8f0fe', font:{{size:11}} }}, grid:{{ display:false }} }}
      }}
    }}
  }});
}})();

// ── 이슈별 날짜 타임라인 차트 ──
(function() {{
  const tls = CD.timelines || {{}};
  Object.keys(tls).forEach(iid => {{
    const canvas = document.getElementById('tl-' + iid);
    if (!canvas) return;
    const tl = tls[iid];
    if (!tl.dates || !tl.dates.length) return;
    new Chart(canvas, {{
      type: 'line',
      data: {{
        labels: tl.dates,
        datasets: [{{
          label: '평균 에러율 (%)',
          data: tl.values,
          borderColor: 'rgba(255,90,90,0.85)',
          backgroundColor: 'rgba(255,90,90,0.15)',
          tension: 0.25,
          fill: true,
          pointBackgroundColor: '#ff5a5a',
          pointBorderColor: '#fff',
          pointRadius: 3,
        }}]
      }},
      options: {{
        responsive: true,
        plugins: {{
          legend: {{ display:false }},
          tooltip: {{
            backgroundColor: '#132236',
            titleColor: '#e8f0fe',
            bodyColor: '#8a9abb',
            callbacks: {{
              afterLabel: ctx => `영향 PID: ${{tl.pids[ctx.dataIndex]}}대`
            }}
          }}
        }},
        scales: {{
          x: {{ ticks:{{ color:'#8a9abb', maxRotation:45, minRotation:0, font:{{size:10}} }},
                grid:{{ color:'rgba(255,255,255,0.04)' }} }},
          y: {{ ticks:{{ color:'#8a9abb', font:{{size:10}}, callback:v=>v+'%' }},
                grid:{{ color:'rgba(255,255,255,0.05)' }},
                beginAtZero:true }}
        }}
      }}
    }});
  }});
}})();

// ── URL hash로 진입한 이슈 카드 자동 펼침 ──
(function() {{
  function openTarget() {{
    const h = window.location.hash;
    if (!h) return;
    const el = document.querySelector(h);
    if (el && el.tagName === 'DETAILS') {{
      el.open = true;
      el.scrollIntoView({{ behavior:'smooth', block:'start' }});
    }}
  }}
  window.addEventListener('hashchange', openTarget);
  setTimeout(openTarget, 100);
}})();
</script>
</body>
</html>"""

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"트렌드 리포트 저장: {output_path}")
        return output_path
