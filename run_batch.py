"""
run_batch.py
BMS 배치 검증 + 트렌드 분석 CLI 진입점

사용법:
  # 전체 버킷 스캔 + 분석
  python run_batch.py

  # PID 목록 + 기간 지정
  python run_batch.py --pids 2793 2794 2800 --start 2024-01-01 --end 2024-01-31

  # 특정 prefix만
  python run_batch.py --prefix "obd_co_id=MACRIOT/pid=2793/"

  # 기존 결과 파일로 분석만 (S3 접속 불필요)
  python run_batch.py --analyze-only results/batch_20240101_120000.parquet

  # 커스텀 규칙 JSON 파일 사용
  python run_batch.py --rules my_rules.json
"""

import os
import sys
import json
import argparse
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

from batch_validator import BatchValidator, DEFAULT_RULES
from trend_analyzer  import TrendAnalyzer


def load_rules(path):
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, list) else data.get("rules", DEFAULT_RULES)


def print_summary(analyzer):
    print("\n" + "=" * 60)
    print("에러율 상위 10개 항목")
    print("=" * 60)
    top = analyzer.top_errors(10)
    if not top.empty:
        for _, r in top.iterrows():
            bar = "█" * min(int(r["avg_fail_rate"] / 2), 30)
            print(f"  {r['model']:<20} {r['column']:<22} {r['avg_fail_rate']:>6.2f}%  {bar}")
    else:
        print("  에러 없음")

    print("\n" + "=" * 60)
    print("Range 개선 후보")
    print("=" * 60)
    sugg = analyzer.suggest_ranges()
    if not sugg.empty:
        for _, r in sugg.iterrows():
            print(
                f"  {r['model']:<20} {r['column']:<22} "
                f"현재[{r['current_min']},{r['current_max']}] "
                f"→ 제안[{r['suggested_min']},{r['suggested_max']}]  "
                f"({r['avg_fail_rate']:.2f}%)  {r['action']}"
            )
    else:
        print("  개선 후보 없음 (에러율 1% 미만)")
    print()


def main():
    parser = argparse.ArgumentParser(
        description="BMS 배치 검증 + 트렌드 분석",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--bucket",  default=os.getenv("S3_BUCKET", "eplat-validation-monitor"),
                        help="S3 버킷 이름")
    parser.add_argument("--prefix",  default="",
                        help="S3 키 접두사 (선택)")
    parser.add_argument("--pids",    nargs="+",
                        help="검증할 PID 목록 (예: 2793 2794 2800)")
    parser.add_argument("--start",   dest="start_date",
                        help="시작 날짜 YYYY-MM-DD (--pids 와 함께)")
    parser.add_argument("--end",     dest="end_date",
                        help="종료 날짜 YYYY-MM-DD (--pids 와 함께)")
    parser.add_argument("--server",  choices=["bcp", "d2"], default="bcp",
                        help="서버 타입 (기본: bcp)")
    parser.add_argument("--env",     choices=["stag", "prod"], default="stag",
                        help="환경 (기본: stag)")
    parser.add_argument("--workers", type=int, default=8,
                        help="병렬 처리 워커 수 (기본: 8)")
    parser.add_argument("--rules",
                        help="커스텀 규칙 JSON 파일 경로")
    parser.add_argument("--analyze-only", dest="analyze_only",
                        help="기존 결과 parquet 파일로 분석만 수행")
    parser.add_argument("--output-dir", default=".",
                        help="결과/리포트 저장 디렉터리 (기본: 현재 디렉터리)")
    args  = parser.parse_args()
    ts    = datetime.now().strftime("%Y%m%d_%H%M%S")
    today = datetime.now().strftime("%Y-%m-%d")

    # ── 저장 경로 자동 결정 ──────────────────────────────────────
    # 기존 다운로드 구조: downloads/{오늘날짜}/{pid}/
    # --output-dir 미지정 시 동일 구조로 자동 세팅
    base_downloads = Path(os.getcwd()) / "downloads" / today

    if args.output_dir != ".":
        # 명시적으로 지정한 경우 그대로 사용
        output_dir = Path(args.output_dir).resolve()
    elif args.pids and len(args.pids) == 1:
        # PID 1개: downloads/{오늘날짜}/{pid}/
        output_dir = base_downloads / str(args.pids[0])
    elif args.pids:
        # PID 여러 개: downloads/{오늘날짜}/batch/
        output_dir = base_downloads / "batch"
    else:
        # 전체 스캔: downloads/{오늘날짜}/batch/
        output_dir = base_downloads / "batch"

    output_dir.mkdir(parents=True, exist_ok=True)

    result_path = str(output_dir / f"batch_{ts}.parquet")
    report_path = str(output_dir / f"trend_{ts}.html")

    print(f"저장 위치: {output_dir}")

    # ── 분석 전용 모드 ──
    if args.analyze_only:
        src = Path(args.analyze_only).resolve()
        # 분석 결과는 입력 파일과 같은 폴더에 저장
        report_path = str(src.parent / f"trend_{ts}.html")
        print(f"기존 결과 로드: {src}")
        analyzer = TrendAnalyzer.from_parquet(str(src))
        print_summary(analyzer)
        analyzer.generate_html_report(report_path)
        print(f"리포트: {report_path}")
        return

    # ── 배치 검증 ──
    rules = load_rules(args.rules) if args.rules else DEFAULT_RULES
    print(f"적용 규칙 {len(rules)}개")

    validator = BatchValidator(
        bucket    = args.bucket,
        prefix    = args.prefix,
        rules     = rules,
        n_workers = args.workers,
    )

    if args.pids:
        if not args.start_date or not args.end_date:
            print("오류: --pids 지정 시 --start 와 --end 도 필요합니다.")
            sys.exit(1)
        results_df = validator.run(
            pids        = args.pids,
            start_date  = args.start_date,
            end_date    = args.end_date,
            server_type = args.server,
            env         = args.env,
            output_path = result_path,
        )
    else:
        results_df = validator.run(output_path=result_path)

    if results_df.empty:
        print("검증 결과 없음. 종료.")
        return

    # ── 트렌드 분석 + 리포트 ──
    analyzer = TrendAnalyzer(results_df)
    print_summary(analyzer)
    analyzer.generate_html_report(report_path)

    print(f"\n저장 위치    : {output_dir}")
    print(f"결과 parquet : {result_path}")
    print(f"트렌드 리포트: {report_path}")


if __name__ == "__main__":
    main()
