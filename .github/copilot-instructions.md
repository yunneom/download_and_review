# Copilot Instructions

이 프로젝트는 BMS(Battery Management System) 데이터 검증 도구입니다.

## 핵심 규칙
- 메인 검증 로직: `data_validator_bms.py`의 `BMSDataValidator` 클래스
- GUI: `prdmrt_ui_pyqt.py` (PyQt5)
- vehicle_master.json은 S3에서 **list** 형태로 로드하며 dict 변환하지 않음
- VWGKALRT Fleet: ignit_status 1→0 상태변경 시점만 16/19/20번 검증
- 충전 검출: VWGKALRT는 pack_curr > 1, 기타 Fleet은 pack_curr < -1
- serial_conn_cnt, module_temp_cnt는 JSON에서 문자열이므로 int() 변환 필요

## 상세 컨텍스트
전체 개발 이력과 아키텍처는 `COPILOT_CONTEXT.md` 참조
