"""
dart_web.py
DART 웹 인터페이스 — Flask 기반
- stag/prod + BCP/D2 + PID + 날짜 입력 → BMS 검증 HTML 리포트 반환
- 모바일/PC 브라우저에서 모두 동작
"""

import os
import sys
import tempfile
import traceback
import boto3

from datetime import datetime, timedelta
from flask import Flask, request, render_template_string, Response, stream_with_context

# 현재 디렉토리를 Python 경로에 추가 (data_validator_bms 등 import)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_validator_bms import BMSDataValidator
from bms_report_generator import generate_html_report
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

AWS_ACCESS_KEY_ID     = os.getenv('AWS_ACCESS_KEY_ID', '')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', '')
AWS_DEFAULT_REGION    = os.getenv('AWS_REGION', 'ap-northeast-2')

# ──────────────────────────────────────────────
# 입력 폼 HTML (DART 다크 테마, 모바일 반응형)
# ──────────────────────────────────────────────
FORM_HTML = """<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>DART — BMS 검증</title>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: 'Segoe UI','Malgun Gothic',sans-serif; background:#0d1b2a; color:#e8f0fe;
       min-height:100vh; display:flex; flex-direction:column; align-items:center; justify-content:center; padding:24px; }
.card { background:#132236; border-radius:16px; padding:36px 32px; width:100%; max-width:480px;
        box-shadow:0 8px 40px rgba(0,0,0,0.4); border:1px solid rgba(255,255,255,0.07); }
.logo { font-size:13px; font-weight:700; color:#00c4a0; letter-spacing:2px; margin-bottom:6px; }
h1 { font-size:22px; font-weight:800; margin-bottom:4px; }
.sub { font-size:12px; color:#8a9abb; margin-bottom:28px; }
.field { margin-bottom:18px; }
label { display:block; font-size:12px; font-weight:600; color:#8a9abb; margin-bottom:6px; letter-spacing:0.4px; }
input[type=text], input[type=date], select {
  width:100%; padding:11px 14px; border-radius:10px; border:1px solid rgba(255,255,255,0.12);
  background:rgba(255,255,255,0.05); color:#e8f0fe; font-size:14px; outline:none;
  transition:border-color 0.15s; -webkit-appearance:none; }
input:focus, select:focus { border-color:#00c4a0; }
select option { background:#1a2e45; }
.row { display:flex; gap:12px; }
.row .field { flex:1; }
.toggle-group { display:flex; gap:8px; }
.tbn { flex:1; padding:10px; border-radius:8px; border:1px solid rgba(255,255,255,0.12);
       background:transparent; color:#8a9abb; font-size:13px; font-weight:600; cursor:pointer;
       transition:all 0.15s; text-align:center; }
.tbn.active { background:rgba(0,196,160,0.15); border-color:#00c4a0; color:#00c4a0; }
.tbn.active-warn { background:rgba(245,166,35,0.15); border-color:#f5a623; color:#f5a623; }
button[type=submit] {
  width:100%; padding:14px; border-radius:10px; border:none; cursor:pointer; margin-top:8px;
  background:linear-gradient(135deg,#00c4a0,#0090c8); color:#fff;
  font-size:15px; font-weight:700; letter-spacing:0.3px; transition:opacity 0.15s; }
button[type=submit]:hover { opacity:0.88; }
button[type=submit]:disabled { opacity:0.4; cursor:not-allowed; }
.error { background:rgba(255,90,90,0.12); border:1px solid rgba(255,90,90,0.3);
         border-radius:10px; padding:12px 16px; font-size:13px; color:#ff8a8a; margin-bottom:18px; }
.loading { display:none; flex-direction:column; align-items:center; gap:14px; padding:20px 0; }
.spinner { width:40px; height:40px; border:3px solid rgba(0,196,160,0.2);
           border-top-color:#00c4a0; border-radius:50%; animation:spin 0.8s linear infinite; }
@keyframes spin { to { transform:rotate(360deg); } }
.loading-msg { font-size:13px; color:#8a9abb; }
.footer { margin-top:24px; font-size:11px; color:#4a6a8a; }
</style>
</head>
<body>
<div class="card">
  <div class="logo">DART</div>
  <h1>BMS 데이터 검증</h1>
  <p class="sub">Data Assurance &amp; Reliability Tracker</p>

  {% if error %}
  <div class="error">{{ error }}</div>
  {% endif %}

  <form method="POST" action="/run" id="form">
    <div class="field">
      <label>서버 타입</label>
      <div class="toggle-group">
        <button type="button" class="tbn active" onclick="setType(this,'bcp')" id="btn-bcp">BCP</button>
        <button type="button" class="tbn" onclick="setType(this,'d2')" id="btn-d2">D2</button>
      </div>
      <input type="hidden" name="server_type" id="server_type" value="bcp">
    </div>

    <div class="field">
      <label>환경</label>
      <div class="toggle-group">
        <button type="button" class="tbn active-warn active" onclick="setEnv(this,'stag')" id="btn-stag">STAG</button>
        <button type="button" class="tbn" onclick="setEnv(this,'prod')" id="btn-prod">PROD</button>
      </div>
      <input type="hidden" name="env" id="env" value="stag">
    </div>

    <div class="field">
      <label>PID</label>
      <input type="text" name="pid" placeholder="예: 2793" value="{{ pid or '' }}" required inputmode="numeric">
    </div>

    <div class="row">
      <div class="field">
        <label>시작 날짜</label>
        <input type="date" name="start_date" value="{{ start_date or today }}" required>
      </div>
      <div class="field">
        <label>종료 날짜</label>
        <input type="date" name="end_date" value="{{ end_date or today }}" required>
      </div>
    </div>

    <div id="form-body">
      <button type="submit" id="submit-btn">검증 실행</button>
    </div>
    <div class="loading" id="loading">
      <div class="spinner"></div>
      <div class="loading-msg" id="loading-msg">S3에서 데이터 다운로드 중...</div>
    </div>
  </form>
</div>
<div class="footer">DART · BMS Data Quality Assurance System</div>

<script>
  function setType(btn, val) {
    document.querySelectorAll('#btn-bcp,#btn-d2').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    document.getElementById('server_type').value = val;
  }
  function setEnv(btn, val) {
    document.querySelectorAll('#btn-stag,#btn-prod').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    document.getElementById('env').value = val;
  }
  // Restore toggle state from URL params if redirected back
  const p = new URLSearchParams(location.search);
  if (p.get('server_type') === 'd2') setType(document.getElementById('btn-d2'), 'd2');
  if (p.get('env') === 'prod') setEnv(document.getElementById('btn-prod'), 'prod');

  document.getElementById('form').addEventListener('submit', function() {
    document.getElementById('submit-btn').disabled = true;
    document.getElementById('form-body').style.display = 'none';
    const ld = document.getElementById('loading');
    ld.style.display = 'flex';
    const msgs = ['S3에서 데이터 다운로드 중...', '데이터 검증 진행 중...', 'HTML 리포트 생성 중...'];
    let i = 0;
    setInterval(() => { i = (i+1) % msgs.length; document.getElementById('loading-msg').textContent = msgs[i]; }, 3000);
  });
</script>
</body>
</html>"""


# ──────────────────────────────────────────────
# 라우트
# ──────────────────────────────────────────────

@app.route('/', methods=['GET'])
def index():
    today = datetime.now().strftime('%Y-%m-%d')
    return render_template_string(FORM_HTML, error=None, today=today, pid='', start_date=today, end_date=today)


@app.route('/run', methods=['POST'])
def run_validation():
    server_type = request.form.get('server_type', 'bcp').strip().lower()
    env         = request.form.get('env', 'stag').strip().lower()
    pid_raw     = request.form.get('pid', '').strip()
    start_date  = request.form.get('start_date', '').strip()
    end_date    = request.form.get('end_date', '').strip()

    today = datetime.now().strftime('%Y-%m-%d')

    # ── 입력 검증 ──
    if not pid_raw.isdigit():
        return render_template_string(FORM_HTML, error="PID는 숫자여야 합니다.", today=today,
                                      pid=pid_raw, start_date=start_date, end_date=end_date)
    pid = int(pid_raw)

    try:
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt   = datetime.strptime(end_date,   '%Y-%m-%d')
    except ValueError:
        return render_template_string(FORM_HTML, error="날짜 형식이 올바르지 않습니다 (YYYY-MM-DD).",
                                      today=today, pid=pid_raw, start_date=start_date, end_date=end_date)

    if end_dt < start_dt:
        return render_template_string(FORM_HTML, error="종료 날짜가 시작 날짜보다 앞설 수 없습니다.",
                                      today=today, pid=pid_raw, start_date=start_date, end_date=end_date)

    if (end_dt - start_dt).days > 30:
        return render_template_string(FORM_HTML, error="날짜 범위는 최대 31일입니다.",
                                      today=today, pid=pid_raw, start_date=start_date, end_date=end_date)

    # ── S3 버킷 및 경로 결정 ──
    if server_type == 'bcp':
        bucket_name = f'bcp-{env}-d2-storage-private-apne2'
    else:
        bucket_name = 'eplat-validation-monitor'

    # ── S3 클라이언트 ──
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_DEFAULT_REGION,
    )

    # ── 날짜 범위 생성 ──
    date_list = []
    cur = start_dt
    while cur <= end_dt:
        date_list.append(cur.strftime('%Y-%m-%d'))
        cur += timedelta(days=1)

    downloaded_files = []

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            # ── 다운로드 ──
            for date_str in date_list:
                if server_type == 'bcp':
                    prefixes = [
                        f"obd_co_id=MACRIOT/pid={pid}/signal_kst_date={date_str}/",
                        f"obd_co_id=LGES/pid={pid}/signal_kst_date={date_str}/",
                    ]
                else:
                    prefixes = [
                        f"DVAL/d2/{env}/pid_{pid}/{date_str}/",
                    ]

                for prefix in prefixes:
                    resp = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
                    if 'Contents' not in resp:
                        continue
                    for obj in resp['Contents']:
                        key = obj['Key']
                        fname = os.path.basename(key)
                        if not fname:
                            continue
                        local_path = os.path.join(tmpdir, fname)
                        s3_client.download_file(bucket_name, key, local_path)
                        downloaded_files.append(local_path)
                    break  # BCP: MACRIOT 있으면 LGES 스킵

            if not downloaded_files:
                return render_template_string(
                    FORM_HTML,
                    error=f"S3에서 파일을 찾을 수 없습니다. (버킷: {bucket_name}, PID: {pid}, 기간: {start_date} ~ {end_date})",
                    today=today, pid=pid_raw, start_date=start_date, end_date=end_date,
                )

            # ── 검증 ──
            validator = BMSDataValidator()
            all_results = []
            last_df = None
            last_vehicle_model = None
            last_fleet = None
            last_source = None

            for fpath in sorted(downloaded_files):
                validator.validate_file(fpath)
                all_results.extend(validator.results)
                last_df            = validator.df
                last_vehicle_model = validator.vehicle_model
                last_fleet         = validator.fleet
                last_source        = fpath

            # ── HTML 리포트 생성 ──
            html_path = os.path.join(tmpdir, 'report.html')
            generate_html_report(
                results       = all_results,
                df            = last_df,
                vehicle_model = last_vehicle_model,
                fleet         = last_fleet,
                output_path   = html_path,
                source_file_path = last_source,
            )

            with open(html_path, 'r', encoding='utf-8') as f:
                html_content = f.read()

        return Response(html_content, mimetype='text/html; charset=utf-8')

    except Exception as e:
        tb = traceback.format_exc()
        app.logger.error(f"검증 실패:\n{tb}")
        return render_template_string(
            FORM_HTML,
            error=f"오류 발생: {str(e)}",
            today=today, pid=pid_raw, start_date=start_date, end_date=end_date,
        )


@app.route('/health')
def health():
    return {'status': 'ok', 'service': 'DART'}, 200


# ──────────────────────────────────────────────
if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    debug = os.getenv('FLASK_DEBUG', 'false').lower() == 'true'
    print(f"DART Web Server 시작: http://0.0.0.0:{port}")
    app.run(host='0.0.0.0', port=port, debug=debug)
