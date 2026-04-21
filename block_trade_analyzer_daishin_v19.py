#!/usr/bin/env python3
"""
Block Trade Analyzer v18.12 - Daishin CYBOS Plus COM API (1-min bars)
=====================================================
수급 교차분석기 - 대신증권 CYBOS Plus API 1분봉 버전

변경 이력:
  v17: 키움 REST API (ka10084, ka10032, pykrx)
  v18: 대신증권 CYBOS Plus COM API (StockChart 틱, MarketEye)
  v18.1: 틱→1분봉 전환, threshold 폐지, 전체 수급 합산 방식
       - 매수/매도 구분: 호가비교 방식 (필드 10/11) → 정확도 대폭 향상
       - 종목 선정: MarketEye 전종목 일괄 조회 → pykrx 의존성 완전 제거
       - Rate limit: CYBOS Plus 내장 관리 (GetLimitRemainCount)
  v18.2: ETF/ETN/ELW 분류 정확도 개선 + 장전 전일분석 + 스케줄러
       - GetStockSectionKind()로 종목구분 (종목명 휴리스틱 제거)
       - 입구 필터링: 주권(1)+ETF(10,12)만 MarketEye 조회
       - 09:00 전 실행 시 전일 영업일 데이터로 분석
       - --repeat 모드: 전일분석 → 09:05 대기 → 장중 반복
       - KRX 휴장일 캘린더 (2025H2~2026) — 설/추석/대체공휴일 자동 건너뛰기
       - buy_ratio 수정: 매수금액/(매수+매도) × 100 (0~100% 범위)
       - 시가총액(억원) 컬럼 신설 (MarketEye 필드 20: 상장주식수)
       - 테마명 정규화: 세분류→대분류 (반도체 장비→반도체, 바이오 신약→바이오)
       - 네이버 테마 자동 크롤링 (정치테마 등 자동 포착)
  v18.3: MarketEye 필드20 상장주식수 단위 보정 (IsBigListingStock)
       - 상장주식수 20억+ 종목(삼성전자 등)은 천단위 수신 → ×1000 보정
       - CpUtil.CpCodeMgr.IsBigListingStock() 활용
  v18.4: Capture Ratio (지수 대비 상승/하락 포착비율) 컬럼 추가
       - KOSPI 1분봉 5거래일 기준 Up/Down Capture Ratio 계산
       - U/D Ratio (Up Cap ÷ Down Cap) + U-D (Up Cap − Down Cap) 2개 컬럼
       - 1분봉 매칭 부족 시 3분봉→5분봉 자동 집계 fallback
       - 당일 첫 실행에서만 API 호출 (이후 capture 캐시 재사용)
  v18.5: Up Cap → 호가/시총 비율 컬럼으로 교체
       - MarketEye 필드 13(총매도호가잔량), 14(총매수호가잔량) 추가
       - 호가잔량/시총 비율(%) = (총매도잔량+총매수잔량) / 상장주식수 × 100
       - 추가 API 호출 0건 (MarketEye 기존 일괄조회에 필드 추가만)
       - Capture Ratio STEP 3 제거 (종목당 API 호출 절감)
       - 호가/시총 상위 필터 체크박스 추가
  v18.6: 예상마감대금 비교 + 20일 고가대비 컬럼 추가
       - 예상마감대금 = 누적거래대금 × (390 / 경과분) — MarketEye 거래대금 활용
       - 예상대비D-1: 예상마감대금 vs 전일 거래대금 (%)
       - 예상대비D-2: 예상마감대금 vs 전전일 거래대금 (%)
       - 20일고가대비: 현재가 vs 20거래일 최고고가 (%)
       - StockChart 일봉 25일치 조회 (block trade 종목만, 일봉 캐시 적용)
       - 1분봉 고가(3)/저가(4) 필드 추가 → ATR(7)% 계산 지원
  v18.7: [5일내상/하한가] 탭 신설
       - 최근 5거래일 내 정규장 종가가 상한가 또는 하한가로 마감한 종목 별도 탭
       - 판별 기준: 전일종가 대비 등락률 ≥ ±29% (호가단위 반올림 감안)
       - [개별주] 탭과 중복 표기 (제거하지 않음)
       - 컬럼 구성은 [개별주] 탭과 동일
       - 추가 API 호출 0건 (기존 일봉 캐시 데이터 활용)
  v18.8: 삼각형 매수타점 컬럼 추가 (타점(2x) + 남은시간)
       - 점대칭 ∩ 패턴 기반: C(중심)→P(고점)→2x 매수타점 자동 산출
       - center_lb=10 (스윙 저점 탐지 민감도)
       - 남은시간 = (거리% / ATR7%)² 분 (BM 차원 분석 차용 휴리스틱 — ATR≠σ 보정 없음)
       - 1.5x 타점/남은시간은 tooltip으로 표시
       - 패턴 미형성 시 '-' 표시
       - sortTable에 data-sort-val 지원 추가 (숫자/텍스트 혼합 컬럼 정렬)
  v18.9: 거래대금 회귀 스크리닝 컬럼 추가 (이탈도 + 회복예상)
       - OU 직관 차용 휴리스틱: 20일 중앙값/평균 중 큰 값을 기준대금(μ)으로 설정
       - 이탈도(σ) = (μ - 예상마감대금) / σ_20 — 클수록 회복 여지 큼
       - AR(1) 계수로 θ(회귀속도) 근사 추정: θ ≈ 1 - β (1차 근사, β가 0.5 수준이면 오차 ~28%)
       - 회복예상 = 이탈도/θ × 390분 → "X시간 Y분" 형식 표시
       - 이탈도 ≤ 0 (이미 평균 이상): "충분" 표시
       - 데이터 부족/θ≤0: '-' 표시
       - 테스트용 신설 — 추후 백테스팅으로 유효성 검증 예정
  v18.10: Perplexity 지적 사항 패치
       - [버그수정] beta < 0 경고가 클리핑 후 dead code였던 문제 수정
         raw_beta를 클리핑 전에 별도 보존 → 경고는 raw_beta 기준으로 실행
       - [수식개선] θ = 1-β (1차 근사) → θ = -ln(β) (이산-연속 OU 정확 변환)
         β → 1에 가까울수록 차이 미미하나, β=0.5 수준에서 기존 오차 ~28% 제거
       - [주석수정] 20일고가대비: target_date 포함으로 확정, 모순 주석 제거
         (포함 의도이면서 "제외"라고 혼재하던 주석 통일)
       - [주석완화] 상하한가 29% 기준: "사실상 불가능" → "실무상 근사 기준"으로 완화
       - [주석완화] 남은시간 공식: "브라운 운동 역산" → 모델 가정 명시로 교체
  v18.11: 캐시 완결성 판정 + STEP 3 AUX 폴백 + U자형 보조값 + 스냅샷 중복 방지
       - [캐시] results cache에 is_complete + max_last_bar_time 메타 추가
         저장 시점에 tick_cache 최대 bar 시각이 1520 이상이면 complete=True
       - [캐시] 로드 시 2단계 검증 (메타 플래그 + 실데이터 max_last_bar_time)
         불완전 캐시 감지 시 tick_cache 비워서 기존 직접 fetch 경로로 유도
       - [STEP 3 AUX] 장외 전일분석 모드 전용 일봉 기반 재계산 루프 추가
         전일 실제 종가·거래대금을 주입해 est_eod/tv_itatdo/tv_recovery_mins 산출
         트리거: (1) 분봉 재수집 발생 (2) tv_itatdo 전체 None (3) tv_itatdo_u 전체 None
       - [이탈도 U자형 보조값] 단순 시간 비례(메인) + U자형 누적거래량 보정(보조) 병행
         KR_INTRADAY_VOLUME_PROFILE 경험값(09:30=18%, 10:00=27%, 12:00=49%, 15:30=100%)
         HTML 셀 툴팁에 양쪽 값 비교 노출 → 사용자가 경험적으로 검증 가능
       - [스냅샷] 전일 분석 모드에서 snapshot append 스킵 (중복 누적 방지)
         effective_date != today_str이면 triangle_snapshots jsonl에 append 안 함
  v18.20.1: [hotfix] 캐시 경로(장 마감 후 재실행)에서도 키움 호출
       - [버그] 장 마감 후 v18 재실행 시 results_cache_*.pkl을 사용하는 경로 A는
         run_analysis를 호출하지 않아서 program_trade 컬럼이 모두 "-"로 표시됨
       - [수정] _enrich_results_with_kiwoom_sync()를 HTML 생성 직전에 추가
         경로 A/B 모두에서 호출되며, 이미 주입된 경우 자동 스킵
       - [신규] _kiwoom_resolve_target_date(): 장 마감 후/주말/공휴일이면
         가장 최근 정규장 거래일 자동 결정 (KRX 공휴일 자동 처리)
       - 사용자 요구: "장 마감했거나 정규장 시작 전이면 가장 최근 정규장
         데이터를 가져와 활용"
  v18.20: 키움 ka90008 프로그램순매수 컬럼 + 모달 팝업 (개별/상한가 테이블)
       - [신규] kiwoom_program_trade.py 모듈 분리 (인증/캐시/병렬 호출)
       - [신규] "프로그램 순매수%" 컬럼 (당일거래대금 우측, ETF 제외)
         값 = 키움 ka90008의 (prm_netprps_qty / trde_qty) × 100, 최신 행 1개
         색상: 양수=빨강 / 음수=파랑 / |값|≥10% 매우진하게 / |값|≥5% 진하게
       - [신규] 셀 클릭 시 모달 팝업 (Chart.js 듀얼 Y축 + 9컬럼 시계열 표)
         시계열: gzip+base64로 HTML 임베드 (오프라인 재오픈 가능)
       - [성능] CYBOS 작업과 병렬 실행 (ThreadPool 1워커, 키움은 내부 8병렬)
         사이클 총 시간 증가 거의 0초 (키움 호출 ~2~5초 < CYBOS 작업)
       - [캐시] cache_block/program_trade_YYYYMMDD.pkl, 종목별 9분 TTL
         token_kiwoom.json 별도 캐시 (24시간 유효, appkey 변경 자동 감지)
       - [의존성] kiwoom_program_trade.py 미존재 시 자동 fallback (컬럼 "-")
  v18.19: prev_day_bars 캐시 최적화
       - [캐시] tick_cache에 prev_day_bars 키 추가: 장중 재실행 시 전일봉 API 호출 생략
         캐시 miss 시 fetch_minute_data_prev_day 호출 후 즉시 tick_cache에 저장
         캐시 hit 시 저장된 prev_day_bars 재사용 (종목당 BlockRequest 1회 절약)
  v18.18: 전일동시간대비(%) 컬럼 추가
       - [NEW] 전일동시간대비: 당일 누적거래대금 / 전일 동시각까지 누적거래대금 × 100
         PineScript 로직 동일: barTradeVal = (high+low)/2 × volume 기준
         전일 2일치 1분봉 (고가·저가 포함) 별도 조회 → 이진탐색으로 동시각 누적 산출
         색상: ≥100% 빨강(전일 대비 강함) / <100% 파랑(전일 대비 약함) / 데이터없음 회색
         위치: '시총대비 예상마감' 컬럼 바로 왼쪽
  v18.17: 툴팁 좌우 반전 + D-2 원복
       - [UI] 툴팁 우측 화면 이탈 시: 밀어내기 → 커서 왼쪽으로 반전 배치
         (기존 maxX 클램핑은 커서와 박스 겹침 + 강제 줄바꿈 문제 유발)
         Y축도 동일 원리 적용: 하단 이탈 시 커서 위쪽으로 반전
       - [UI] 예상대비D-2: col-hidden 복구 (v18.16에서 의도치 않게 노출됨)
         값·코드·데이터는 HTML에 그대로 유지, 화면 표시만 숨김
  v18.16: 탭 순서 변경 + 예상마감대금 컬럼 표시 개선
       - [UI] 탭 순서: 개별주→ETF→5일내상/하한가 → 5일내상/하한가→개별주→ETF
       - [UI] 기본 활성 탭: 개별주 → 5일내상/하한가 (홍인기식 우선 확인)
       - [UI] 예상마감대금(억) 컬럼 → 시총대비예상마감(%)으로 변경
         산식: (예상마감대금 ÷ 시가총액) × 100
         이유: 절대값 정렬 시 삼성전자 등 대형주가 항상 상위 → 의미 없음
         색상: ≥5% 빨강(매우강함) / 2~5% 파랑(강함) / <0.5% 회색(미미)
         tooltip: 마우스 올리면 예상마감대금(억) + 시총(억) 원본값 표시
       - [UI] 예상대비D-2 col-hidden 제거 → 화면에 항상 표시
  v18.15: theme_map 사전 동기화 — results 외 종목도 분류 보장
       - [버그수정] fill_missing_themes에 사전 동기화 단계(THEME-PRESYNC) 추가
         배경: v18.14의 sync는 `for r in results:` 루프 안에 있어, 블록딜 입구 필터를
               통과하지 못한 종목(예: 더즌 462860 — 거래대금 미달)은 theme_map에
               영원히 등록되지 않았음. 진단 결과 네이버=2,344종목 vs results=~490종목
               으로 약 1,800개 종목이 분류 누락 상태였음
         변경: results 루프 진입 전에 naver_map 전체를 theme_map에 사전 등록
         효과: 더즌처럼 results에 안 들어가는 종목도 정상 분류. 나중에 들어와도 즉시
               정확한 분류 적용
         부작용: theme_map 종목수 ~5배 증가 (~490 → ~2,344), cache 파일 크기 약 2배
       - [로깅] [THEME-PRESYNC] 로그 추가: 신규/변경/동일 카운트 + 이상 징후 경고
         (신규+변경 0건 시 또는 naver_map < 1000종목 시 WARNING)
  v18.14: 테마 분류 강제 동기화 + TTL + 페이지 한도 확대
       - [버그수정] fill_missing_themes 네이버 동기화 게이트 제거
         기존: `if not theme_map.get(code)` → 한 번 분류된 종목은 영원히 갱신 안 됨
         문제: 신규상장주(예: 더즌 462860)가 상장 직후 일부 테마만 가진 채 캐시되면,
               이후 네이버에 새 테마(예: 스테이블코인)가 추가돼도 영영 반영 안 되던 버그
         변경: 게이트 제거. 네이버에 종목이 있으면 무조건 전체 교체(REPLACE/sync)
               네이버에 없는 종목만 LLM/키워드 fallback 단계로 진입
         로깅: 실제 변경된 종목은 [THEME-SYNC] 로그로 audit 가능
       - [신규] theme_map TTL 추가 (cache에 theme_map_refresh_date 필드)
         정책: 매 거래일마다 갱신. cache의 refresh_date != 오늘이면 강제 재동기화
         off-market 경로에서도 fill_missing_themes를 명시적으로 호출하여 sync 보장
         휴장일에는 스크립트가 실행되지 않으므로 자연스럽게 skip
       - [확장] fetch_naver_theme_map의 max_theme_pages 12 → 30 으로 확대
         이유: 네이버 테마 페이지가 ~200개 이상이므로 12페이지 cap에 걸려 일부 테마
               (예: 스테이블코인 등 신규/하위 카테고리) 누락 가능성 있었음
               외부 API rate limit이 아닌 단순 스크립트 cap이므로 제약 없음
       - [별도] block_trade_theme_diagnosis.py 진단 스크립트 신규 작성
         tick_cache의 theme_map과 fresh naver_map을 비교하여 불일치 종목 리포트
  v18.13: 테마별 분포 바 종목수 불일치 + 드릴다운 코드셀 가독성 수정
       - [버그수정] make_theme_bar 내부 카운터: themes[0] 단일 매칭 → 전체 정규화 테마 매칭
         이유: v18.12에서 _compute_hot_cold_themes의 드릴다운 매칭은 수정했으나
               동일 패턴이 make_theme_bar에 그대로 남아 분포 바 라벨 숫자(예: 소캠 5)와
               드릴다운 클릭 시 표시되는 종목 수(예: 소캠 9)가 불일치하던 문제
       - [정합성] 카운트 방식 변경에 따라 퍼센트 분모도 total → tag_total 로 변경
         (한 종목이 여러 테마에 카운트되므로 합계가 종목수를 초과 → 바 너비 합 100% 유지)
       - [UI개선] 드릴다운 종목 코드 셀 스타일을 메인 테이블 .code-cell 클래스와 통일
         기존: color:#888; font-family:monospace (Windows에서 Courier New로 렌더 → 흐릿)
         변경: color:#8b8fa3; font-family:Consolas,monospace; font-size:11px
         효과: 드릴다운 패널의 종목 코드 가독성 개선
  v18.12: 드릴다운 버그 수정 — 테마 클릭 시 종목 누락 문제 해결
       - [버그수정] drilldown_data 생성부: themes[0] primary 단일 매칭 방식 제거
         → 종목이 보유한 전체 테마를 정규화한 뒤 theme in normalized_themes로 교체
         이유: 어떤 종목의 1차 테마가 다른 이름일 때도 2차 이하 테마가 일치하면 드릴다운에 포함해야 함
         (예: 건설 테마 9종목 중 2개만 보이던 문제 → 전체 종목 포함으로 수정)
       - [버그수정] current_theme_counts 블록 변수명 오류 전부 수정
         currentstocks → current_stocks, thememap → theme_map,
         normalizetheme → normalize_theme, marketcap → market_cap,
         changerate → change_rate (기존 코드에서 NameError 발생하던 구간)
       - [정렬보강] 드릴다운 종목 정렬: coldness 없는 종목 맨 뒤 + appearances 기준 추가
         기존: (coldness, -hot_count, -cum_score, name)
         변경: (coldness is None, coldness, -appearances, -hot_count, -cum_score, -mcap, name)
       - [로그추가] 테마별 드릴다운 생성 직후 total/has_coldness/no_coldness 로그 출력
         드릴다운 종목 수 이상 시 즉시 원인 파악 가능
       - [중복방지] seen_codes set으로 동일 종목 중복 append 방지

Requirements:
  - Python 3.x (32-bit)
  - pywin32 (pip install pywin32)
  - CYBOS Plus HTS 로그인 상태
  - (선택) OpenAI API key → api11.txt (LLM 테마분류)

Usage:
  1. CYBOS Plus HTS 로그인
  2. python block_trade_analyzer_daishin_v18.py
  3. python block_trade_analyzer_daishin_v18.py --threshold 500000000  (5억 기준)
  4. python block_trade_analyzer_daishin_v18.py --debug  (상세 로그)
"""

import os
import sys
import re
import json
import time
import pickle
import logging
import argparse
import threading  # [v19 hotfix] KiwoomAuth thread-safety
from datetime import datetime, date, timedelta
from collections import Counter, defaultdict
from typing import List, Dict, Tuple, Optional

# COM 라이브러리 (32-bit Python 필수)
try:
    import win32com.client
    import pythoncom
    HAS_COM = True
except ImportError:
    HAS_COM = False
    print("[FATAL] pywin32 not installed. Run: pip install pywin32", flush=True)
    sys.exit(1)

# HTTP (LLM 테마분류용 + [v18.20] 키움 REST API)
import requests

# [v18.20] 키움 프로그램매매 통합용
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============================================================================
#  Logging
# ============================================================================
# [v19] FileHandler 추가 — 관리자 cmd.exe의 Python stdout 미표시 문제 회피.
#   - SCRIPT_DIR은 아직 정의 전이므로 __file__로 직접 유도한다.
#   - logs/ 폴더는 스크립트와 같은 위치에 자동 생성.
#   - 파일명은 실행 시각 타임스탬프 포함(세션별 구분).
#   - 관리자 cmd에서 화면에 아무것도 안 보여도, 이 로그 파일만 있으면 원인 분석 가능.
_LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
try:
    os.makedirs(_LOG_DIR, exist_ok=True)
except Exception:
    _LOG_DIR = os.path.dirname(os.path.abspath(__file__))  # 폴더 생성 실패 시 스크립트 폴더에 저장
_LOG_FILE_PATH = os.path.join(
    _LOG_DIR, f"block_trade_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)

_file_handler = logging.FileHandler(_LOG_FILE_PATH, encoding="utf-8")
_file_handler.setLevel(logging.INFO)
_file_handler.setFormatter(logging.Formatter(
    fmt="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout), _file_handler],
)
log = logging.getLogger("BlockTradeV18")
log.info(f"[LOG] 로그 파일: {_LOG_FILE_PATH}")

# ============================================================================
#  [v18.20] 키움 REST API ka90008 (종목시간별프로그램매매추이) 통합
# ============================================================================
# 원래 별도 파일(kiwoom_program_trade.py)이었으나 v18.20에서 단일 파일로 통합.
# - 토큰 발급(au10001) + 캐싱
# - ka90008 호출 + cont-yn 페이지네이션
# - 데이터 캐시 (program_trade_YYYYMMDD.pkl, 종목별 9분 TTL)
# - ThreadPool 병렬 호출
# ----------------------------------------------------------------------------

KIWOOM_BASE_URL = "https://api.kiwoom.com"
KIWOOM_TOKEN_ENDPOINT = "/oauth2/token"
KIWOOM_KA90008_ENDPOINT = "/api/dostk/mrkcond"

# 키 파일 자동 탐색 후보 (디렉토리는 SCRIPT_DIR — 함수에서 lazy 참조)
_APPKEY_CANDIDATES = [
    "kiwoom_appkey.txt", "kiwoom_app_key.txt",
    "appkey_kiwoom.txt", "appkey.txt", "app_key.txt", "키움_appkey.txt",
]
_SECRETKEY_CANDIDATES = [
    "kiwoom_secretkey.txt", "kiwoom_secret_key.txt", "kiwoom_secret.txt",
    "secretkey_kiwoom.txt", "secretkey.txt", "secret_key.txt",
    "appsecret.txt", "app_secret.txt", "키움_secretkey.txt",
]

_KIWOOM_TOKEN_EXPIRY_MARGIN_SEC = 60
# [v19] TTL 사실상 제거 — 당일자이기만 하면 항상 fresh로 취급.
#   v18까지는 9분 TTL 만료 시 "전체 재조회 후 병합"을 수행 → 종목당 15~20페이지씩
#   매 10분 사이클마다 다시 받아와 1,820~2,425초가 걸려 120초 timeout에 걸렸음.
#   v19부터는 "캐시가 있으면 page 1만 받아 overlap 감지 후 조기 종료"하는
#   진정한 증분 수집으로 전환하므로, TTL은 당일이기만 하면 유효.
#   첫 호출(캐시 없음) 시에만 전체 페이지 수집이 일어난다.
_KIWOOM_DATA_CACHE_TTL_SEC = 24 * 60 * 60  # 24시간(실질 무한)
_KIWOOM_RATE_LIMIT_PER_SEC = 4       # 키움 한도(초당 5회)의 80%
_KIWOOM_RATE_LIMIT_DELAY = 1.0 / _KIWOOM_RATE_LIMIT_PER_SEC
_KIWOOM_DEFAULT_MAX_WORKERS = 8
_KIWOOM_HTTP_TIMEOUT_SEC = 10
_KIWOOM_MAX_RETRIES = 3
_KIWOOM_RETRY_BACKOFF_BASE_SEC = 2
# NOTE:
# ka90008 페이지당 행 수는 시장 상황/서버 응답에 따라 200보다 훨씬 작게 내려오는 경우가 있다.
# (실측 예: 25~30행/페이지). 기존 6페이지 상한에서는 장중 후반(예: 14:40대) 조회 시
# 09:00 구간이 잘리고 12시대부터만 남는 케이스가 발생했다.
# 여유 있게 20페이지로 확장해 장중 전구간(09:00~15:30) 보존 확률을 높인다.
_KIWOOM_MAX_PAGES = 20
_KIWOOM_MAX_PAGES_HARD_CAP = 80          # 동적 확장 상한 (무한확장 방지)


def _kiwoom_token_cache_path() -> str:
    """SCRIPT_DIR/CACHE_DIR이 module-level에서 정의되기 전이므로 lazy."""
    return os.path.join(CACHE_DIR, "token_kiwoom.json")

# Rate limit 제어
_kiwoom_last_call_time: float = 0.0


def _kiwoom_find_keys(key_dir: str = None) -> Tuple[Optional[str], Optional[str]]:
    """키 디렉토리에서 appkey/secretkey 파일 자동 탐색."""
    if key_dir is None:
        key_dir = SCRIPT_DIR  # lazy 참조
    log.info(f"[KIWOOM] 키 탐색: {key_dir}")
    if not os.path.isdir(key_dir):
        log.warning(f"[KIWOOM] ⚠️ 디렉토리 없음: {key_dir}")
        return None, None
    try:
        files = sorted(os.listdir(key_dir))
    except Exception as e:
        log.error(f"[KIWOOM] 디렉토리 읽기 실패: {e}")
        return None, None

    appkey_path = None
    secretkey_path = None

    # 정확한 후보 우선
    for cand in _APPKEY_CANDIDATES:
        p = os.path.join(key_dir, cand)
        if os.path.isfile(p):
            appkey_path = p; break
    for cand in _SECRETKEY_CANDIDATES:
        p = os.path.join(key_dir, cand)
        if os.path.isfile(p):
            secretkey_path = p; break

    # 추정 매칭
    if not appkey_path:
        for f in files:
            fl = f.lower()
            if ("appkey" in fl or "app_key" in fl) and "secret" not in fl and f.endswith(".txt"):
                appkey_path = os.path.join(key_dir, f); break
    if not secretkey_path:
        for f in files:
            fl = f.lower()
            if ("secret" in fl or "secretkey" in fl or "secret_key" in fl) and f.endswith(".txt"):
                secretkey_path = os.path.join(key_dir, f); break

    if appkey_path:
        log.info(f"[KIWOOM] ✓ appkey: {os.path.basename(appkey_path)}")
    else:
        log.warning(f"[KIWOOM] ⚠️ appkey 못찾음")
    if secretkey_path:
        log.info(f"[KIWOOM] ✓ secretkey: {os.path.basename(secretkey_path)}")
    else:
        log.warning(f"[KIWOOM] ⚠️ secretkey 못찾음")
    return appkey_path, secretkey_path


def _kiwoom_load_key(path: str) -> Optional[str]:
    """키 파일 1개 로드 (utf-8 → cp949 fallback)."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read().strip()
        if content:
            return content
        log.warning(f"[KIWOOM] ⚠️ 빈 파일: {path}")
        return None
    except UnicodeDecodeError:
        try:
            with open(path, "r", encoding="cp949") as f:
                return f.read().strip()
        except Exception as e:
            log.error(f"[KIWOOM] 로드 실패(cp949): {path} → {e}")
            return None
    except Exception as e:
        log.error(f"[KIWOOM] 로드 실패: {path} → {e}")
        return None


class KiwoomAuth:
    """키움 REST API 토큰 발급 + 캐싱

    [v19 hotfix 2026-04-20] Thread-safety 추가.
      - 기존 v19는 ThreadPoolExecutor 8 워커가 동시에 get_token()을 호출하면
        각자 "토큰 없음" 판단 → 동시에 8번 발급 요청 → 키움 API가 같은 appkey의
        이전 토큰을 뒤 토큰으로 무효화 → 대부분의 워커가 무효 토큰으로 ka90008
        호출하여 [8005:Token이 유효하지 않습니다] 오류 폭발.
      - 해결: threading.Lock으로 발급 구간 직렬화 (이중체크 락 패턴).
      - 추가 방어: _kiwoom_fetch_parallel() 시작 시 메인 스레드에서 get_token()
        1회 선호출하여 캐시 워밍업(아래 _kiwoom_fetch_parallel 함수 참조).
    """

    def __init__(self, appkey: str, secretkey: str):
        self.appkey = appkey
        self.secretkey = secretkey
        self.token: Optional[str] = None
        self.expires_at: Optional[datetime] = None
        self._token_lock = threading.Lock()  # [v19 hotfix] 발급 구간 직렬화
        self._load_cached_token()

    def _load_cached_token(self) -> None:
        if not os.path.isfile(_kiwoom_token_cache_path()):
            return
        try:
            with open(_kiwoom_token_cache_path(), "r", encoding="utf-8") as f:
                data = json.load(f)
            expires_at = datetime.fromisoformat(data["expires_at"])
            if datetime.now() >= expires_at - timedelta(seconds=_KIWOOM_TOKEN_EXPIRY_MARGIN_SEC):
                log.info(f"[KIWOOM] 캐시 토큰 만료/임박 → 무시")
                return
            if data.get("appkey_hint") != self.appkey[:8]:
                log.warning("[KIWOOM] 캐시 토큰 appkey 불일치 → 무시")
                return
            self.token = data["token"]
            self.expires_at = expires_at
            log.info(f"[KIWOOM] ✓ 캐시 토큰 사용 (만료: {expires_at.strftime('%H:%M:%S')})")
        except Exception as e:
            log.warning(f"[KIWOOM] 캐시 토큰 로드 실패: {e}")

    def _save_cached_token(self) -> None:
        os.makedirs(CACHE_DIR, exist_ok=True)
        try:
            with open(_kiwoom_token_cache_path(), "w", encoding="utf-8") as f:
                json.dump({
                    "token": self.token,
                    "expires_at": self.expires_at.isoformat(),
                    "appkey_hint": self.appkey[:8],
                }, f, ensure_ascii=False, indent=2)
            log.info(f"[KIWOOM] 토큰 캐시 저장: {os.path.basename(_kiwoom_token_cache_path())}")
        except Exception as e:
            log.warning(f"[KIWOOM] 토큰 캐시 저장 실패: {e}")

    def get_token(self) -> Optional[str]:
        # [v19 hotfix] Double-checked locking pattern.
        # 1차 체크 (락 없이): 토큰이 이미 유효하면 바로 반환 → hot-path 최적화.
        if (self.token and self.expires_at and
            datetime.now() < self.expires_at - timedelta(seconds=_KIWOOM_TOKEN_EXPIRY_MARGIN_SEC)):
            return self.token

        # Lock 안에서 2차 체크 → 발급 → 락 해제.
        # 여러 워커가 동시에 1차 체크를 통과해도 2차 체크에서 1명만 실제 발급하고
        # 나머지는 발급 완료된 토큰을 재사용한다.
        with self._token_lock:
            # 2차 체크: 락 대기 중 다른 스레드가 이미 발급했을 수 있음.
            if (self.token and self.expires_at and
                datetime.now() < self.expires_at - timedelta(seconds=_KIWOOM_TOKEN_EXPIRY_MARGIN_SEC)):
                return self.token

            log.info("[KIWOOM] 토큰 신규 발급 (au10001)")
            try:
                url = KIWOOM_BASE_URL + KIWOOM_TOKEN_ENDPOINT
                headers = {
                    "Content-Type": "application/json;charset=UTF-8",
                    "api-id": "au10001",
                }
                body = {
                    "grant_type": "client_credentials",
                    "appkey": self.appkey,
                    "secretkey": self.secretkey,  # ⚠️ 'appsecretkey' 아님!
                }
                resp = requests.post(url, headers=headers, json=body, timeout=_KIWOOM_HTTP_TIMEOUT_SEC)
                log.info(f"[KIWOOM] 토큰 응답: HTTP {resp.status_code}")
                if resp.status_code != 200:
                    log.error(f"[KIWOOM] ⚠️ 비정상 status: {resp.status_code} body={resp.text[:300]}")
                    return None
                data = resp.json()
                if data.get("return_code") != 0:
                    log.error(f"[KIWOOM] ⚠️ return_code={data.get('return_code')} msg={data.get('return_msg')}")
                    return None
                self.token = data["token"]
                self.expires_at = datetime.strptime(data["expires_dt"], "%Y%m%d%H%M%S")
                log.info(f"[KIWOOM] ✓ 토큰 발급 (만료: {self.expires_at})")
                self._save_cached_token()
                return self.token
            except requests.exceptions.RequestException as e:
                log.error(f"[KIWOOM] ⚠️ 네트워크 에러: {e}")
                return None
            except Exception as e:
                log.error(f"[KIWOOM] ⚠️ 토큰 발급 예외: {e}")
                return None


def _kiwoom_rate_limit_wait():
    global _kiwoom_last_call_time
    now = time.monotonic()
    elapsed = now - _kiwoom_last_call_time
    if elapsed < _KIWOOM_RATE_LIMIT_DELAY:
        time.sleep(_KIWOOM_RATE_LIMIT_DELAY - elapsed)
    _kiwoom_last_call_time = time.monotonic()


def _kiwoom_post_one_page(url: str, headers: dict, body: dict,
                          stk_cd: str, page: int) -> Tuple[Optional[dict], Optional[str], Optional[str]]:
    """ka90008 단일 페이지 호출. 반환: (data, cont_yn, next_key)."""
    for attempt in range(1, _KIWOOM_MAX_RETRIES + 1):
        try:
            _kiwoom_rate_limit_wait()
            t0 = time.monotonic()
            resp = requests.post(url, headers=headers, json=body, timeout=_KIWOOM_HTTP_TIMEOUT_SEC)
            elapsed_ms = (time.monotonic() - t0) * 1000

            if resp.status_code == 429:
                wait = _KIWOOM_RETRY_BACKOFF_BASE_SEC * attempt
                log.warning(f"[KA90008] {stk_cd} p{page} | HTTP 429 → {wait}s 대기 ({attempt}/{_KIWOOM_MAX_RETRIES})")
                time.sleep(wait); continue

            if resp.status_code != 200:
                log.warning(f"[KA90008] {stk_cd} p{page} | HTTP {resp.status_code} body={resp.text[:200]}")
                return None, None, None

            data = resp.json()
            if data.get("return_code") != 0:
                log.warning(f"[KA90008] {stk_cd} p{page} | return_code={data.get('return_code')} msg={data.get('return_msg')}")
                return None, None, None

            cont_yn = resp.headers.get("cont-yn", "N")
            next_key = resp.headers.get("next-key", "")
            log.info(f"[KA90008] {stk_cd} p{page} | rows={len(data.get('stk_tm_prm_trde_trnsn', []))} "
                     f"| cont_yn={cont_yn} | {elapsed_ms:.0f}ms")
            return data, cont_yn, next_key

        except requests.exceptions.Timeout:
            log.warning(f"[KA90008] {stk_cd} p{page} | 타임아웃 ({attempt}/{_KIWOOM_MAX_RETRIES})")
            time.sleep(_KIWOOM_RETRY_BACKOFF_BASE_SEC * attempt)
        except Exception as e:
            log.warning(f"[KA90008] {stk_cd} p{page} | 예외: {e} ({attempt}/{_KIWOOM_MAX_RETRIES})")
            time.sleep(_KIWOOM_RETRY_BACKOFF_BASE_SEC * attempt)

    log.error(f"[KA90008] ⚠️ {stk_cd} p{page} | {_KIWOOM_MAX_RETRIES}회 재시도 실패")
    return None, None, None


def _kiwoom_fetch_program_trade(stk_cd: str, date_yyyymmdd: str,
                                auth: KiwoomAuth,
                                max_pages: int = _KIWOOM_MAX_PAGES,
                                prev_newest_tm: str = ""
                                ) -> Tuple[Optional[List[dict]], bool]:
    """
    ka90008 호출 (cont-yn 페이지네이션 자동 처리).

    [v19] prev_newest_tm 파라미터 추가 — 증분 수집 지원.
      - prev_newest_tm: 이전 캐시의 가장 최신 row의 tm (HHMMSS). 빈 문자열이면 전체 수집.
      - 응답은 최신→과거 순이므로, 매 페이지의 가장 오래된 row의 tm이
        prev_newest_tm 이하로 떨어지면 이전 캐시와 overlap 발생 → 조기 종료.
      - 이 로직으로 캐시가 이미 있는 경우 대부분 page 1에서 바로 종료된다.

    반환:
      - rows: 실패 None, 빈 응답 []
      - hit_max_pages: 페이지 상한 도달로 강제 종료됐는지 여부
    """
    token = auth.get_token()
    if not token:
        log.error(f"[KA90008] {stk_cd} | 토큰 없음 → 스킵")
        return None, False

    url = KIWOOM_BASE_URL + KIWOOM_KA90008_ENDPOINT
    base_headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "authorization": f"Bearer {token}",
        "api-id": "ka90008",
    }
    body = {"amt_qty_tp": "2", "stk_cd": stk_cd, "date": date_yyyymmdd}

    all_rows: List[dict] = []
    cont_yn = "N"; next_key = ""
    prev_tm_norm = _kiwoom_normalize_tm(prev_newest_tm) if prev_newest_tm else ""
    incremental_mode = bool(prev_tm_norm)

    if incremental_mode:
        log.info(f"[KA90008] {stk_cd} | 증분 모드 (prev_newest_tm={prev_tm_norm})")

    for page in range(1, max_pages + 1):
        headers = dict(base_headers)
        if page > 1:
            headers["cont-yn"] = "Y"
            headers["next-key"] = next_key

        data, cont_yn, next_key = _kiwoom_post_one_page(url, headers, body, stk_cd, page)
        if data is None:
            if page == 1:
                return None, False
            log.warning(f"[KA90008] {stk_cd} | p{page}부터 실패 → 누적 {len(all_rows)}행만 반환")
            break

        rows = data.get("stk_tm_prm_trde_trnsn", []) or []
        all_rows.extend(rows)

        # [v19] 증분 모드: 현재 페이지의 가장 오래된 tm이 prev_newest_tm 이하면
        # 이전 캐시와 overlap — 신규분은 이미 모두 수집했으므로 페이지네이션 중단.
        if incremental_mode and rows:
            cur_oldest_tm = _kiwoom_normalize_tm((rows[-1] or {}).get("tm", ""))
            if cur_oldest_tm and cur_oldest_tm <= prev_tm_norm:
                log.info(f"[KA90008] {stk_cd} | 증분 overlap 감지 "
                         f"(page={page}, cur_oldest={cur_oldest_tm} <= prev_newest={prev_tm_norm}) → 조기종료")
                return all_rows, False

        if cont_yn != "Y" or not next_key:
            log.info(f"[KA90008] {stk_cd} | 페이징 종료 (cont_yn={cont_yn}) | 총 {page}페이지 {len(all_rows)}행")
            break
    else:
        log.warning(f"[KA90008] ⚠️ {stk_cd} | MAX_PAGES({max_pages}) 도달. 총 {len(all_rows)}행")
        if len(all_rows) > 0:
            _oldest_tm = _kiwoom_normalize_tm((all_rows[-1] or {}).get("tm", ""))
            if _oldest_tm:
                log.warning(f"[KA90008] {stk_cd} | 상한 도달 시점 oldest_tm={_oldest_tm}")
        if len(all_rows) == 0:
            return all_rows, True
        return all_rows, True

    if len(all_rows) == 0:
        log.warning(f"[KA90008] ⚠️ {stk_cd} | 전체 응답 0행")
    return all_rows, False


def _kiwoom_oldest_tm(rows: List[dict]) -> str:
    """rows(최신→과거)에서 가장 오래된 tm(HHMMSS) 반환. 없으면 빈 문자열."""
    if not rows:
        return ""
    return _kiwoom_normalize_tm((rows[-1] or {}).get("tm", ""))


def _kiwoom_fetch_program_trade_adaptive(stk_cd: str, date_yyyymmdd: str,
                                         auth: KiwoomAuth,
                                         prev_newest_tm: str = "") -> Optional[List[dict]]:
    """
    페이지 상한에 걸려 오전(09:00) 구간이 잘린 경우 max_pages를 동적으로 늘려 재수신.
    - 조건: 상한 도달(hit_max_pages=True) && oldest_tm > 09:00:00
    - 확장: 기본값에서 2배씩 증가, _KIWOOM_MAX_PAGES_HARD_CAP까지

    [v19] 증분 모드(prev_newest_tm 제공) 시:
      - 첫 호출(캐시 없음)이 아니라 신규분만 수집하므로 backfill 확장이 불필요.
      - 증분 overlap이 감지되면 fetch 함수가 조기종료 → hit_max_pages는 False가 되어
        adaptive 루프가 한 번에 종료된다.
      - 혹시 max_pages 상한에 걸려도 확장하지 않고 바로 반환(신규분만 얻으면 됨).
    """
    pages = max(1, int(_KIWOOM_MAX_PAGES))
    incremental_mode = bool(prev_newest_tm)

    while True:
        rows, hit_max_pages = _kiwoom_fetch_program_trade(
            stk_cd, date_yyyymmdd, auth,
            max_pages=pages,
            prev_newest_tm=prev_newest_tm,
        )
        if rows is None:
            return None

        # 증분 모드: 확장 안 함. 신규분만 받으면 끝.
        if incremental_mode:
            return rows

        oldest_tm = _kiwoom_oldest_tm(rows)
        needs_backfill = bool(oldest_tm and oldest_tm > "090000")

        if not (hit_max_pages and needs_backfill):
            return rows

        if pages >= _KIWOOM_MAX_PAGES_HARD_CAP:
            log.warning(f"[KA90008] {stk_cd} | 동적확장 상한({_KIWOOM_MAX_PAGES_HARD_CAP}) 도달 "
                        f"(oldest_tm={oldest_tm})")
            return rows

        next_pages = min(_KIWOOM_MAX_PAGES_HARD_CAP, pages * 2)
        log.info(f"[KA90008] {stk_cd} | 데이터 절단 의심(oldest_tm={oldest_tm}) → "
                 f"max_pages {pages}→{next_pages} 재수신")
        pages = next_pages


def _kiwoom_calc_latest_ratio(rows: List[dict]) -> Optional[float]:
    """최신 행에서 (prm_netprps_qty / trde_qty) × 100 계산."""
    if not rows:
        return None
    try:
        latest = rows[0]  # 응답은 최신→과거 순
        net_qty_str = str(latest.get("prm_netprps_qty", "0")).replace("--", "-").replace("+", "")
        trde_qty_str = str(latest.get("trde_qty", "0")).replace("--", "-").replace("+", "")
        net_qty = float(net_qty_str) if net_qty_str else 0.0
        trde_qty = float(trde_qty_str) if trde_qty_str else 0.0
        if trde_qty == 0:
            return None
        return (net_qty / trde_qty) * 100.0
    except Exception as e:
        log.error(f"[KA90008] 비율 계산 실패: {e}")
        return None


def _kiwoom_data_cache_path(date_yyyymmdd: str) -> str:
    return os.path.join(CACHE_DIR, f"program_trade_{date_yyyymmdd}.pkl")


def _kiwoom_load_data_cache(date_yyyymmdd: str) -> Dict[str, dict]:
    """당일자 캐시 로드 (다음날 자동 stale: 파일명에 날짜 포함)."""
    path = _kiwoom_data_cache_path(date_yyyymmdd)
    if not os.path.isfile(path):
        log.info(f"[KIWOOM_CACHE] MISS: {os.path.basename(path)} 없음")
        return {}
    try:
        with open(path, "rb") as f:
            cache = pickle.load(f)
        log.info(f"[KIWOOM_CACHE] HIT: {len(cache)}종목")
        return cache
    except Exception as e:
        log.warning(f"[KIWOOM_CACHE] 로드 실패: {e}")
        return {}


def _kiwoom_save_data_cache(cache: Dict[str, dict], date_yyyymmdd: str) -> None:
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = _kiwoom_data_cache_path(date_yyyymmdd)
    try:
        with open(path, "wb") as f:
            pickle.dump(cache, f, protocol=pickle.HIGHEST_PROTOCOL)
        log.info(f"[KIWOOM_CACHE] 저장: {len(cache)}종목 → {os.path.basename(path)}")
    except Exception as e:
        log.warning(f"[KIWOOM_CACHE] 저장 실패: {e}")


def _kiwoom_is_cache_fresh(entry: dict, date_yyyymmdd: str) -> bool:
    """
    [v19] 의미 변경: "API 호출을 완전히 스킵해도 되는가?"가 아니라
    "rows를 신뢰할 수 있는 당일자 데이터로 볼 수 있는가?"를 뜻한다.

    - fresh=True  : rows가 있고 날짜가 당일자 → _kiwoom_fetch_parallel이 증분 수집(page 1만)으로 처리
    - fresh=False : rows 없거나 날짜 불일치 → 전체 페이지 수집 (초기 수집 케이스)

    v18까지는 TTL 9분 만료 시 전체 재조회 후 merge를 수행해
    사이클당 30~40분이 걸려 120초 timeout에 걸렸다. v19에서는 TTL 체크를 사실상
    제거하고, 증분 수집 + 병합으로 사이클당 2~4분까지 단축한다.
    """
    if not entry or "fetched_at" not in entry:
        return False

    rows = entry.get("rows") or []
    if not rows:
        return False

    try:
        fetched = datetime.fromisoformat(entry["fetched_at"])
        target_date = str(entry.get("target_date") or fetched.strftime("%Y%m%d"))
        # 날짜 불일치 → stale (익일 자동 전환). 이 체크만 살려둔다.
        if fetched.strftime("%Y%m%d") != str(date_yyyymmdd) or target_date != str(date_yyyymmdd):
            return False
        # [v19] TTL 체크 무력화 — 당일자면 항상 fresh.
        #   (상수는 24h로 남겨두어 혹시 장 마감 후 자정 넘어 돌면 자동 stale되도록 함)
        age_sec = (datetime.now() - fetched).total_seconds()
        if age_sec > _KIWOOM_DATA_CACHE_TTL_SEC:
            log.info(f"[KIWOOM_CACHE] TTL 만료 ({age_sec/3600:.1f}h) → 재수집")
            return False
        return True
    except Exception:
        return False


def _kiwoom_normalize_tm(tm: str) -> str:
    """시간문자열을 HHMMSS 6자리로 정규화. 실패 시 빈 문자열."""
    s = ''.join(ch for ch in str(tm or '') if ch.isdigit())
    if not s:
        return ''
    if len(s) >= 6:
        return s[:6]
    return s.zfill(6)


def _kiwoom_merge_program_trade_rows(old_rows: List[dict], new_rows: List[dict]) -> List[dict]:
    """
    기존 rows를 버리지 않고, 새로 받은 rows로 같은 시각 행만 교체하면서 병합한다.
    - ka90008 문서상 date/stk_cd만 제공되고 start time/since 파라미터가 없어
      '진짜 증분 요청'은 불가능하다. 따라서 수동 refresh나 불완전 캐시 복구 시에는
      전체 재조회 결과를 기존 캐시에 병합하는 방식으로 시계열 보존을 우선한다.
    - 최신순(desc) 정렬 유지.
    """
    merged: Dict[str, dict] = {}

    for row in old_rows or []:
        tm = _kiwoom_normalize_tm(row.get('tm', ''))
        if tm:
            merged[tm] = row

    for row in new_rows or []:
        tm = _kiwoom_normalize_tm(row.get('tm', ''))
        if tm:
            merged[tm] = row  # 동일 시각은 최신 fetch 값으로 교체

    if merged:
        return [merged[tm] for tm in sorted(merged.keys(), reverse=True)]

    # tm 없는 비정상 데이터만 있는 경우 안전 fallback
    return list(new_rows or old_rows or [])


def _kiwoom_fetch_parallel(
    stock_codes: List[str],
    date_yyyymmdd: str,
    auth: KiwoomAuth,
    max_workers: int = _KIWOOM_DEFAULT_MAX_WORKERS,
    use_cache: bool = True,
) -> Dict[str, dict]:
    """
    ka90008 병렬 호출.

    [v19] 증분 수집 모델:
      - 캐시 있는 종목(incremental) : 이전 last_tm 이후의 신규분만 page 1 위주로 수집
        → 대부분 page 1에서 overlap 감지 조기종료, API 호출 1회로 끝남.
      - 캐시 없는 종목(initial)      : 당일 전체 시계열 수집(기존 로직).

    이전(v18)에는 TTL 만료 시 전 종목을 "stale" 처리하고 전체 페이지를 다시 받아와서
    사이클당 30~40분이 걸렸음. v19는 캐시 유무만으로 초기/증분을 구분한다.
    """
    log.info(f"[KIWOOM_PT] 병렬 시작 | 종목 {len(stock_codes)}개 | workers={max_workers} | cache={use_cache}")
    t0 = time.monotonic()

    # [v19 hotfix 2026-04-20] 병렬 실행 전 메인 스레드에서 토큰 1회 선발급.
    # KiwoomAuth.get_token()에 Lock이 걸려 있어도, "첫 토큰 발급 지연으로
    # 워커들이 Lock 경합을 기다리는" 상황이 불필요하게 발생할 수 있음.
    # 메인에서 미리 1회 성공시키면 ThreadPool 워커들은 모두 1차 체크(락 없음)
    # 단계에서 기존 토큰을 바로 얻을 수 있어 hot-path만 타게 된다.
    _warmup_token = auth.get_token()
    if _warmup_token is None:
        log.error("[KIWOOM_PT] ⚠️ 토큰 선발급 실패 → 전 종목 실패 예상")
        # 그래도 캐시된 데이터 반환 시도(호출측이 폴백 처리)
        return _kiwoom_load_data_cache(date_yyyymmdd) if use_cache else {}
    log.info(f"[KIWOOM_PT] ✓ 토큰 워밍업 완료 (만료: {auth.expires_at})")

    cache = _kiwoom_load_data_cache(date_yyyymmdd) if use_cache else {}

    # [v19] 모든 종목을 처리 대상으로 삼되, 기존 rows 유무로 증분/초기 모드를 분기.
    #   - incremental_mode 종목: prev_newest_tm 전달 → page 1만 수집 후 overlap 조기종료
    #   - initial_mode 종목    : prev_newest_tm 없음 → 전체 페이지 수집
    target_codes: List[str] = []
    incremental_cnt = 0
    initial_cnt = 0
    for code in stock_codes:
        entry = cache.get(code) if use_cache else None
        has_fresh_rows = bool(use_cache and _kiwoom_is_cache_fresh(entry, date_yyyymmdd))
        target_codes.append(code)
        if has_fresh_rows:
            incremental_cnt += 1
        else:
            initial_cnt += 1

    log.info(f"[KIWOOM_PT] 수집 모드: 증분={incremental_cnt} 초기={initial_cnt} "
             f"| policy=incremental-merge (v19)")

    if not target_codes:
        log.info(f"[KIWOOM_PT] 처리 대상 없음. {(time.monotonic()-t0)*1000:.0f}ms")
        return cache

    success = 0; fail = 0; empty = 0
    early_stop_cnt = 0  # 증분 overlap으로 조기종료된 종목 수

    def _one(code: str):
        # 기존 캐시에서 가장 최신 tm 추출(증분 기준점)
        entry = cache.get(code) or {}
        prev_rows = entry.get("rows") or []
        prev_newest_tm = ""
        if prev_rows and _kiwoom_is_cache_fresh(entry, date_yyyymmdd):
            # rows는 최신→과거 desc 정렬. rows[0]이 가장 최신.
            prev_newest_tm = _kiwoom_normalize_tm(
                (prev_rows[0] or {}).get("tm", "") or entry.get("last_tm", "")
            )
        rows = _kiwoom_fetch_program_trade_adaptive(
            code, date_yyyymmdd, auth,
            prev_newest_tm=prev_newest_tm,
        )
        if rows is None:
            return code, None, None, prev_newest_tm
        return code, rows, _kiwoom_calc_latest_ratio(rows), prev_newest_tm

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_one, code): code for code in target_codes}
        for fut in as_completed(futures):
            code = futures[fut]
            try:
                _, rows, ratio, prev_tm = fut.result()
                if rows is None:
                    fail += 1; continue
                if not rows:
                    empty += 1
                prev_entry = cache.get(code) or {}
                merged_rows = _kiwoom_merge_program_trade_rows(prev_entry.get("rows") or [], rows or [])
                merged_ratio = _kiwoom_calc_latest_ratio(merged_rows)
                # 증분 모드였는데 신규 행이 전혀 추가되지 않았는지 확인(디버깅용)
                if prev_tm and rows:
                    # rows의 최신이 prev_tm보다 뒤쪽이면 신규분 확보 성공
                    new_newest = _kiwoom_normalize_tm((rows[0] or {}).get("tm", ""))
                    if new_newest and new_newest <= prev_tm:
                        early_stop_cnt += 1
                cache[code] = {
                    "fetched_at": datetime.now().isoformat(timespec="seconds"),
                    "target_date": date_yyyymmdd,
                    "rows": merged_rows,
                    "latest_ratio": merged_ratio if merged_ratio is not None else ratio,
                    "last_tm": _kiwoom_normalize_tm((merged_rows[0] if merged_rows else {}).get("tm", "")),
                }
                success += 1
            except Exception as e:
                log.error(f"[KIWOOM_PT] {code} | future 예외: {e}")
                fail += 1

    elapsed = time.monotonic() - t0
    log.info(f"[KIWOOM_PT] 완료 | success={success} fail={fail} empty={empty} "
             f"| 증분정체={early_stop_cnt} | {elapsed:.1f}s")
    if fail > 0:
        log.warning(f"[KIWOOM_PT] ⚠️ {fail}/{len(target_codes)}종목 실패")

    if use_cache:
        _kiwoom_save_data_cache(cache, date_yyyymmdd)
    return cache


# ============================================================================
#  [v18.20] 키움 통합 헬퍼 (run_analysis 안에서 호출)
# ============================================================================
# 설계: Step 1(종목선정) 직후 백그라운드 future로 ka90008 호출 시작 →
#       Step 2~끝(CYBOS 1분봉 + 분석)을 메인 스레드에서 처리 →
#       run_analysis 끝나기 직전 future.result()로 회수해서 results에 주입.
# 효과: CYBOS 작업이 보통 30~60초 걸리므로, 키움 호출이 그 안에 끝나서
#       사이클 총 시간 증가가 거의 0초.

def _start_kiwoom_program_trade_future(top_stocks: List[dict], date_yyyymmdd: str):
    """Step 1 직후 호출. STOCK 타입만 추려서 백그라운드 future 시작."""
    # STOCK 타입만 (ETF 제외 - 사용자 요구사항)
    stock_codes: List[str] = []
    for s in top_stocks:
        code = s.get("code_clean", "")
        name = s.get("name", "")
        if not code:
            continue
        st = classify_stock_type(code, name)  # 호출 시점 평가
        if st == "STOCK":
            stock_codes.append(code)

    if not stock_codes:
        log.info("[KIWOOM_PT] STOCK 타입 종목 없음 → 스킵")
        return None

    # 키 로드 + 인증
    appkey_path, secretkey_path = _kiwoom_find_keys()
    if not appkey_path or not secretkey_path:
        log.warning("[KIWOOM_PT] 키 파일 못찾음 → 스킵 (컬럼은 '-'로 표시됨)")
        return None
    appkey = _kiwoom_load_key(appkey_path)
    secretkey = _kiwoom_load_key(secretkey_path)
    if not appkey or not secretkey:
        log.warning("[KIWOOM_PT] 키 로드 실패 → 스킵")
        return None

    auth = KiwoomAuth(appkey, secretkey)

    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="kiwoom_pt")
    log.info(f"[KIWOOM_PT] ▶ 백그라운드 호출 시작 ({len(stock_codes)}종목, date={date_yyyymmdd})")
    future = executor.submit(_kiwoom_fetch_parallel, stock_codes, date_yyyymmdd, auth)
    future._kpt_executor = executor
    future._kpt_stock_codes = stock_codes
    future._kpt_start_time = time.time()
    return future


def _attach_kiwoom_results(results: List[dict], future,
                           date_yyyymmdd: Optional[str] = None) -> None:
    """
    run_analysis 끝나기 직전 호출. results에 'program_trade' 키 주입.

    [v19] 변경점:
      1) timeout 120 → 600초 확대. 초기 수집(캐시 없는 날 첫 사이클)은
         485종목 × 전체 페이지가 필요하므로 120초로는 확실히 부족.
         증분 수집이 자리잡으면 보통 10~60초로 끝남.
      2) 타임아웃/예외 발생 시 cache={}로 무력화하지 않고, 디스크 캐시를 읽어
         과거 사이클에서 성공한 데이터로 폴백. 이로써 "프로그램순매수 전체 하이픈"
         재발을 방지. date_yyyymmdd가 없으면 오늘 날짜로 폴백.
    """
    if future is None:
        for r in results:
            r["program_trade"] = None
        return

    # 디스크 폴백용 날짜 결정
    if not date_yyyymmdd:
        try:
            date_yyyymmdd = _kiwoom_resolve_target_date()
        except Exception:
            date_yyyymmdd = datetime.now().strftime("%Y%m%d")

    try:
        log.info("[KIWOOM_PT] ◀ 백그라운드 결과 회수 중 (timeout=600s)...")
        cache = future.result(timeout=600)
        elapsed = time.time() - getattr(future, "_kpt_start_time", time.time())
        log.info(f"[KIWOOM_PT] ◀ 회수 완료: {len(cache)}종목 (백그라운드 총 {elapsed:.1f}s)")
    except Exception as e:
        # 타임아웃이든 다른 예외든, 디스크 캐시로 폴백 → "전 종목 하이픈" 사태 방지
        log.warning(f"[KIWOOM_PT] ⚠️ 회수 실패: {type(e).__name__}: {e} → 디스크 캐시 폴백 시도")
        try:
            cache = _kiwoom_load_data_cache(date_yyyymmdd) or {}
            log.info(f"[KIWOOM_PT] 디스크 폴백: {len(cache)}종목 로드 (date={date_yyyymmdd})")
        except Exception as e2:
            log.error(f"[KIWOOM_PT] ⚠️ 디스크 폴백도 실패: {e2}")
            cache = {}
    finally:
        try:
            future._kpt_executor.shutdown(wait=False)
        except Exception:
            pass

    n_attached = 0; n_with_data = 0
    for r in results:
        code = r.get("code", "")
        entry = cache.get(code)
        if entry:
            r["program_trade"] = {
                "latest_ratio": entry.get("latest_ratio"),
                "rows": entry.get("rows") or [],
                "fetched_at": entry.get("fetched_at", ""),
            }
            n_attached += 1
            if entry.get("latest_ratio") is not None:
                n_with_data += 1
        else:
            r["program_trade"] = None

    log.info(f"[KIWOOM_PT] results 주입: {n_attached}/{len(results)}종목 (유효 비율값 {n_with_data}건)")
    if n_with_data == 0 and n_attached > 0:
        log.warning("[KIWOOM_PT] ⚠️ 회수했지만 유효한 비율값 0건 (장외시간이거나 거래량 0)")
    if n_attached == 0:
        log.warning("[KIWOOM_PT] ⚠️ results에 주입된 종목 0건 — 프로그램순매수 컬럼이 전부 '-'로 표시됩니다")


def _kiwoom_resolve_target_date(today_str: str = None) -> str:
    """
    [v18.20.1] 키움 ka90008용 대상 날짜 결정.
    - 장중(09:00~15:30): 오늘
    - 그 외(장 마감 후/장 시작 전/주말/공휴일): 가장 최근 정규장 거래일
    today_str: 외부에서 명시 시 우선 사용. 그 날짜가 휴일이면 직전 거래일 폴백.
    """
    now = datetime.now()
    if today_str:
        # 외부 지정 날짜가 휴일이면 직전 거래일로
        try:
            dt = datetime.strptime(today_str, "%Y%m%d").date()
            if is_krx_holiday(dt):
                last = get_last_trading_day(before=dt + timedelta(days=1))
                resolved = last.strftime("%Y%m%d")
                log.info(f"[KIWOOM_DATE] 지정일({today_str})이 휴일 → 직전 거래일 {resolved}")
                return resolved
            return today_str
        except Exception:
            pass

    today = now.date()
    is_market = (now.hour == 9 or (10 <= now.hour <= 14) or
                 (now.hour == 15 and now.minute < 40))

    if is_market and not is_krx_holiday(today):
        return today.strftime("%Y%m%d")

    # 09시 이전이거나 마감 후 또는 휴일 → 가장 최근 거래일
    if now.hour < 9 or is_krx_holiday(today):
        # 오늘 미포함 직전 거래일
        last = get_last_trading_day(before=today)
    else:
        # 마감 후 → 오늘이 거래일이면 오늘, 아니면 직전 거래일
        last = today if not is_krx_holiday(today) else get_last_trading_day(before=today)

    resolved = last.strftime("%Y%m%d")
    if resolved != now.strftime("%Y%m%d"):
        log.info(f"[KIWOOM_DATE] 비장중({now.strftime('%H:%M')}) → "
                 f"가장 최근 정규장 {resolved} 사용")
    return resolved


def _enrich_results_with_kiwoom_sync(results: List[dict],
                                     target_date: str = None) -> None:
    """
    [v18.20.1] 캐시 경로(경로 A)용 동기 키움 호출.
    이미 program_trade 키가 있으면 스킵 (run_analysis 경로에서 이미 주입됨).
    target_date 미지정 시 _kiwoom_resolve_target_date()가 자동 결정.
    """
    if not results:
        return

    # 이미 한 종목이라도 program_trade 키가 있으면 (run_analysis 경로) 스킵
    if any("program_trade" in r for r in results):
        log.info("[KIWOOM_PT] results에 이미 키움 데이터 있음 → 스킵 (run_analysis 경로)")
        return

    resolved_date = _kiwoom_resolve_target_date(target_date)

    # STOCK 타입만 (ETF 제외)
    stock_codes: List[str] = []
    for r in results:
        code = r.get("code", "")
        name = r.get("name", "")
        if not code:
            continue
        st = classify_stock_type(code, name)
        if st == "STOCK":
            stock_codes.append(code)

    if not stock_codes:
        log.info("[KIWOOM_PT] STOCK 타입 종목 없음 → 모두 None")
        for r in results:
            r["program_trade"] = None
        return

    # 키 로드 + 인증
    appkey_path, secretkey_path = _kiwoom_find_keys()
    if not appkey_path or not secretkey_path:
        log.warning("[KIWOOM_PT] 키 파일 못찾음 → 모두 None")
        for r in results:
            r["program_trade"] = None
        return
    appkey = _kiwoom_load_key(appkey_path)
    secretkey = _kiwoom_load_key(secretkey_path)
    if not (appkey and secretkey):
        log.warning("[KIWOOM_PT] 키 로드 실패 → 모두 None")
        for r in results:
            r["program_trade"] = None
        return

    auth = KiwoomAuth(appkey, secretkey)
    log.info(f"[KIWOOM_PT] ★ 캐시 경로 동기 호출 시작 ({len(stock_codes)}종목, date={resolved_date})")
    t0 = time.time()
    cache = _kiwoom_fetch_parallel(stock_codes, resolved_date, auth)
    log.info(f"[KIWOOM_PT] ★ 동기 호출 완료 ({time.time()-t0:.1f}s)")

    # results에 주입
    n_with_data = 0
    for r in results:
        code = r.get("code", "")
        entry = cache.get(code)
        if entry:
            r["program_trade"] = {
                "latest_ratio": entry.get("latest_ratio"),
                "rows": entry.get("rows") or [],
                "fetched_at": entry.get("fetched_at", ""),
            }
            if entry.get("latest_ratio") is not None:
                n_with_data += 1
        else:
            r["program_trade"] = None
    log.info(f"[KIWOOM_PT] 주입: 유효 비율값 {n_with_data}/{len(results)}종목 (date={resolved_date})")


# ============================================================================
#  Constants
# ============================================================================
# [v18.1] threshold 폐지: 1분봉 전체 합산 방식으로 전환
# BLOCK_TRADE_THRESHOLD = 100_000_000  # (틱 모드 전용, 주석처리)
CHANGE_RATE_THRESHOLD = 5.0          # 급등 필터: ≥5%
TOP_N_BY_VOLUME = 500                # 거래대금 상위 N (MarketEye 전종목 중)
TOP_N_ETF = 30                       # ETF 거래대금 상위 N
# TICK_REQUEST_COUNT = 2000          # (틱 모드 전용, 주석처리)
# MAX_TICK_PAGES = 30                # (틱 모드 전용, 주석처리)
MIN_REQUEST_COUNT = 400              # StockChart 1분봉 1회 요청 수 (당일 최대 390봉)

# [v18.4] Capture Ratio (지수 대비 상승/하락 포착비율)
CAPTURE_BAR_COUNT = 2000             # 5거래일 1분봉 (390×5=1950, 여유분 포함)
CAPTURE_MIN_MATCHED = 50             # 1분봉 기준 최소 매칭 수익률 개수
CAPTURE_MIN_MATCHED_3M = 20          # 3분봉 fallback 최소
CAPTURE_MIN_MATCHED_5M = 10          # 5분봉 fallback 최소
CAPTURE_INDEX_CODE = "U001"          # KOSPI 종합지수
CAPTURE_FIELDS = [0, 1, 5, 8]       # 날짜, 시간, 종가, 거래량

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(SCRIPT_DIR, "cache_block")
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output_block")

# MarketEye 요청 필드
ME_FIELDS = [0, 2, 3, 4, 10, 11, 13, 14, 17, 20, 23, 24]
# 0:종목코드, 2:대비부호, 3:전일대비, 4:현재가, 10:거래량,
# 11:거래대금(원), 13:총매도호가잔량, 14:총매수호가잔량,
# 17:종목명, 20:총상장주식수(주), 23:전일종가, 24:체결강도

# StockChart 1분봉 요청 필드
SC_MIN_FIELDS = [0, 1, 3, 4, 5, 8, 10, 11]  # [v18.6] 고가(3),저가(4) 추가 → ATR 계산용
# 0:날짜, 1:시간(hhmm), 5:종가, 8:거래량,
# 10:누적체결매도수량(호가비교), 11:누적체결매수수량(호가비교)
# ※ 필드 10/11은 차트타입 'T'(틱) 또는 'm'(분)에서만 유효

# [v18.6] 일봉 요청 필드 (예상마감대금 비교 + 20일 고가 계산용)
SC_DAILY_FIELDS = [0, 3, 5, 9]  # 0:날짜, 3:고가, 5:종가, 9:거래대금
DAILY_BAR_COUNT = 25            # 20거래일 + 여유분
TOTAL_SESSION_MINUTES = 390     # 09:00~15:30 = 390분

# [v18.1] 틱 모드 필드 (주석처리, 원복 시 사용)
# SC_TICK_FIELDS = [0, 1, 5, 8, 10, 11]


# ============================================================================
#  CYBOS Plus Utilities
# ============================================================================

class CybosConnection:
    """CYBOS Plus 연결 상태 관리 및 Rate Limit 처리"""

    def __init__(self):
        self.cp_status = win32com.client.Dispatch("CpUtil.CpCybos")
        self.connected = False
        self._call_count = 0

    def check_connect(self) -> bool:
        """CYBOS Plus 연결 상태 확인"""
        try:
            self.connected = bool(self.cp_status.IsConnect)
            if self.connected:
                log.info("[CYBOS] CYBOS Plus connected. Ready.")
            else:
                log.error("[CYBOS] CYBOS Plus NOT connected!")
                log.error("[CYBOS] → CYBOS Plus HTS를 먼저 실행하고 로그인하세요.")
            return self.connected
        except Exception as e:
            log.error(f"[CYBOS] Connection check failed: {e}")
            return False

    def wait_for_rate_limit(self):
        """시세 조회 rate limit 대기 (type=1: 시세)"""
        remain = self.cp_status.GetLimitRemainCount(1)
        self._call_count += 1
        if remain <= 0:
            wait_ms = self.cp_status.LimitRequestRemainTime
            wait_sec = wait_ms / 1000 + 0.05  # 안전 마진
            if wait_sec > 0.5:
                log.info(f"[RATE] Remaining=0, waiting {wait_sec:.1f}s "
                         f"(calls so far: {self._call_count})")
            time.sleep(wait_sec)

    @property
    def call_count(self) -> int:
        return self._call_count

    def get_remain_count(self) -> int:
        """남은 시세 조회 가능 횟수"""
        return self.cp_status.GetLimitRemainCount(1)


# ============================================================================
#  KRX 휴장일 캘린더
# ============================================================================

# 2026년 KRX 휴장일 (주말 제외, 평일 휴장만)
# 출처: 한국거래소 공시 + 관공서 공휴일에 관한 규정
# ※ 매년 초 또는 연말에 다음 연도 목록 업데이트 필요
KRX_HOLIDAYS = {
    # ---- 2025년 (하반기) ----
    "20250901", "20250902", "20250903",  # 추석 연휴 (월~수)
    "20251003",                           # 개천절 (금)
    "20251006",                           # 대체공휴일 (월, 한글날 10/9 목 → 10/6 대체)
    "20251009",                           # 한글날 (목)
    "20251225",                           # 성탄절 (목)
    "20251231",                           # 연말 휴장 (수)
    # ---- 2026년 ----
    "20260101",                           # 신정 (목)
    "20260216", "20260217", "20260218",   # 설 연휴 (월~수)
    "20260302",                           # 삼일절 대체공휴일 (월, 3/1 일요일)
    "20260501",                           # 근로자의 날 (금)
    "20260505",                           # 어린이날 (화)
    "20260525",                           # 부처님오신날 대체공휴일 (월, 5/24 일요일)
    "20260603",                           # 지방선거 (수) — 임시공휴일 예상
    "20260717",                           # 제헌절 (금) — 2026년 공휴일 재지정
    "20260817",                           # 광복절 대체공휴일 (월, 8/15 토요일)
    "20260924", "20260925",               # 추석 연휴 (목~금, 9/26 토요일)
    "20261005",                           # 개천절 대체공휴일 (월, 10/3 토요일)
    "20261009",                           # 한글날 (금)
    "20261225",                           # 성탄절 (금)
    "20261231",                           # 연말 휴장 (목)
}


def is_krx_holiday(d: date) -> bool:
    """주말 또는 KRX 휴장일인지 확인"""
    if d.weekday() >= 5:  # 토(5), 일(6)
        return True
    return d.strftime("%Y%m%d") in KRX_HOLIDAYS


def get_last_trading_day(before: date) -> date:
    """before 이전의 마지막 거래일 (before 포함하지 않음)"""
    d = before - timedelta(days=1)
    while is_krx_holiday(d):
        d -= timedelta(days=1)
    return d


def get_target_date() -> Tuple[str, bool]:
    """
    분석 대상 날짜 결정

    [v18.2] 09:00 이전 실행 시 직전 거래일 데이터 사용
    - 주말 + KRX 공휴일 모두 건너뜀
    - 예: 설 연휴 후 첫 날 08:30 → 설 전 마지막 거래일
    Returns: (target_date_str "YYYYMMDD", is_previous_day: bool)
    """
    now = datetime.now()
    MARKET_OPEN_HOUR = 9

    if now.hour < MARKET_OPEN_HOUR:
        # 장전: 직전 거래일
        d = get_last_trading_day(before=now.date())
        target = d.strftime("%Y%m%d")
        days_back = (now.date() - d).days
        log.info(f"[DATE] Pre-market ({now.strftime('%H:%M')}). "
                 f"Last trading day: {target} ({days_back}일 전)")
        return target, True
    else:
        # 장중이지만 오늘이 휴장일이면? (실제로는 CYBOS가 안 열리므로 이론적)
        target = now.strftime("%Y%m%d")
        return target, False


def get_all_stock_codes(conn: CybosConnection) -> Tuple[List[str], List[str]]:
    """
    CpCodeMgr로 전체 종목코드 조회 + 부구분코드 필터링

    [v18.2] 보통주(1) + ETF(10,12)만 반환. ETN/ELW/신주인수권 등 제외.
    부구분코드:
      1=주권, 2=투자회사, 3=부동산투자회사, 4=선박투자회사,
      5=사회간접자본, 6=주식예탁증서, 7=신주인수권증권, 8=신주인수권증서,
      9=ELW, 10=ETF, 11=수익증권, 12=해외ETF, 13=외국주권, 17=ETN

    Returns: (kospi_codes, kosdaq_codes) - 'A' prefix 포함, 필터링 완료
    Side effect: _SECTION_KIND_CACHE에 {code: kind} 저장 (classify_stock_type용)
    """
    global _SECTION_KIND_CACHE
    code_mgr = win32com.client.Dispatch("CpUtil.CpCodeMgr")

    # 허용 부구분코드: 주권(보통주+우선주) + ETF + 해외ETF
    ALLOWED_KINDS = {1, 10, 12}

    # 시장구분: 1=KOSPI, 2=KOSDAQ
    kospi_raw = list(code_mgr.GetStockListByMarket(1) or [])
    kosdaq_raw = list(code_mgr.GetStockListByMarket(2) or [])

    total_raw = len(kospi_raw) + len(kosdaq_raw)

    # 필터링 + 부구분코드 캐시
    section_cache = {}
    kospi_filtered = []
    kosdaq_filtered = []
    excluded_counts = {}  # 제외된 종류별 카운트

    for code in kospi_raw:
        kind = code_mgr.GetStockSectionKind(code)
        section_cache[code] = kind
        if kind in ALLOWED_KINDS:
            kospi_filtered.append(code)
        else:
            excluded_counts[kind] = excluded_counts.get(kind, 0) + 1

    for code in kosdaq_raw:
        kind = code_mgr.GetStockSectionKind(code)
        section_cache[code] = kind
        if kind in ALLOWED_KINDS:
            kosdaq_filtered.append(code)
        else:
            excluded_counts[kind] = excluded_counts.get(kind, 0) + 1

    _SECTION_KIND_CACHE = section_cache

    total_filtered = len(kospi_filtered) + len(kosdaq_filtered)
    total_excluded = total_raw - total_filtered

    log.info(f"[CODES] Raw: KOSPI={len(kospi_raw)} KOSDAQ={len(kosdaq_raw)} "
             f"Total={total_raw}")
    log.info(f"[CODES] Filtered (주권+ETF): KOSPI={len(kospi_filtered)} "
             f"KOSDAQ={len(kosdaq_filtered)} Total={total_filtered}")
    if excluded_counts:
        # 부구분코드 → 이름 매핑
        kind_names = {
            0: "구분없음", 2: "투자회사", 3: "리츠", 4: "선박투자",
            5: "사회간접자본", 6: "예탁증서", 7: "신주인수권증권",
            8: "신주인수권증서", 9: "ELW", 11: "수익증권",
            13: "외국주권", 14: "선물", 15: "옵션", 17: "ETN",
        }
        excluded_detail = ", ".join(
            f"{kind_names.get(k, f'code{k}')}:{v}"
            for k, v in sorted(excluded_counts.items()))
        log.info(f"[CODES] Excluded {total_excluded}: {excluded_detail}")

    return kospi_filtered, kosdaq_filtered


# 부구분코드 캐시 (get_all_stock_codes에서 채움, classify_stock_type에서 참조)
_SECTION_KIND_CACHE: Dict[str, int] = {}


def strip_a_prefix(code: str) -> str:
    """'A005930' → '005930'"""
    if code and code.startswith("A"):
        return code[1:]
    return code


def ensure_a_prefix(code: str) -> str:
    """'005930' → 'A005930'"""
    code = code.strip()
    if code and not code.startswith("A"):
        return "A" + code.zfill(6)
    return code



# ============================================================================
#  MarketEye - 전종목 일괄 시세 조회
# ============================================================================

def fetch_market_eye_batch(conn: CybosConnection,
                           codes: List[str],
                           fields: List[int] = None) -> List[dict]:
    """
    MarketEye로 종목 일괄 시세 조회 (최대 200종목/회)
    codes: 'A' prefix 포함 종목코드 리스트
    Returns: [{"code": "A005930", "name": "삼성전자", "price": 72000, ...}, ...]
    """
    if fields is None:
        fields = ME_FIELDS

    me_obj = win32com.client.Dispatch("CpSysDib.MarketEye")
    # [v18.3] 상장주식수 20억+ 종목 보정용 (천단위→일단위 변환)
    code_mgr = win32com.client.Dispatch("CpUtil.CpCodeMgr")

    all_results = []
    batch_size = 200  # MarketEye 최대 200종목/회

    total_batches = (len(codes) + batch_size - 1) // batch_size
    log.info(f"[ME] Querying {len(codes)} stocks in {total_batches} batches...")

    for batch_idx in range(0, len(codes), batch_size):
        batch = codes[batch_idx:batch_idx + batch_size]
        batch_num = batch_idx // batch_size + 1

        conn.wait_for_rate_limit()

        me_obj.SetInputValue(0, fields)
        me_obj.SetInputValue(1, batch)
        me_obj.SetInputValue(2, ord('2'))  # 호가비교방식

        me_obj.BlockRequest()

        status = me_obj.GetDibStatus()
        if status != 0:
            msg = me_obj.GetDibMsg1()
            log.warning(f"[ME] Batch {batch_num}/{total_batches} error: "
                        f"status={status}, msg={msg}")
            continue

        num_fields = me_obj.GetHeaderValue(0)
        num_stocks = me_obj.GetHeaderValue(2)

        if batch_num <= 2 or batch_num == total_batches:
            log.info(f"[ME] Batch {batch_num}/{total_batches}: "
                     f"{num_stocks} stocks, {num_fields} fields, "
                     f"remain={conn.get_remain_count()}")

        # 필드 인덱스 매핑 (요청한 필드 값의 오름차순으로 정렬됨)
        sorted_fields = sorted(fields)
        field_idx_map = {fval: idx for idx, fval in enumerate(sorted_fields)}

        for row in range(num_stocks):
            try:
                stock_data = {}
                for fval in fields:
                    fidx = field_idx_map[fval]
                    val = me_obj.GetDataValue(fidx, row)
                    stock_data[fval] = val

                # 파싱
                code_raw = str(stock_data.get(0, ""))
                name = str(stock_data.get(17, ""))
                price = int(stock_data.get(4, 0) or 0)
                prev_close = int(stock_data.get(23, 0) or 0)
                sign = str(stock_data.get(2, "3"))       # 대비부호
                change_abs = float(stock_data.get(3, 0) or 0)
                volume = int(stock_data.get(10, 0) or 0)
                trading_amount = int(stock_data.get(11, 0) or 0)
                strength = float(stock_data.get(24, 0) or 0)
                listed_shares = int(stock_data.get(20, 0) or 0)
                # [v18.3] 상장주식수 20억+ 종목은 천단위 수신 → ×1000 보정
                # (2026.02 기준 삼성전자 보통주가 유일한 해당 종목)
                if listed_shares > 0 and code_mgr.IsBigListingStock(code_raw):
                    listed_shares *= 1000
                    if batch_num <= 2:
                        log.debug(f"[ME] BigListing ×1000 applied: {code_raw} "
                                  f"({name}) → {listed_shares:,}주")

                # 시가총액 (억원 단위)
                market_cap_억 = round(price * listed_shares / 1e8) if price > 0 else 0

                # [v18.5] 호가잔량/시총 비율
                ask_remain = int(stock_data.get(13, 0) or 0)  # 총매도호가잔량(주)
                bid_remain = int(stock_data.get(14, 0) or 0)  # 총매수호가잔량(주)
                total_ob_shares = ask_remain + bid_remain
                # 비율(%) = (총호가잔량 / 총상장주식수) × 100
                if listed_shares > 0 and total_ob_shares > 0:
                    ob_ratio = total_ob_shares / listed_shares * 100
                else:
                    ob_ratio = 0.0

                # 등락률 계산
                if prev_close > 0:
                    change_rate = (price - prev_close) / prev_close * 100
                elif change_abs != 0 and price > 0:
                    # fallback: 부호 반영
                    if sign in ('4', '5'):  # 하한, 하락
                        change_rate = -abs(change_abs) / (price + abs(change_abs)) * 100
                    else:
                        change_rate = abs(change_abs) / (price - abs(change_abs)) * 100 if price > abs(change_abs) else 0
                else:
                    change_rate = 0.0

                all_results.append({
                    "code": code_raw,              # 'A005930' 형태
                    "code_clean": strip_a_prefix(code_raw),  # '005930'
                    "name": name,
                    "price": price,
                    "prev_close": prev_close,
                    "change_rate": round(change_rate, 2),
                    "volume": volume,
                    "trading_amount": trading_amount,  # 원 단위
                    "strength": strength,
                    "market_cap": market_cap_억,       # 억원 단위
                    "ob_ratio": round(ob_ratio, 4),    # 호가잔량/시총 비율(%)
                    "ask_remain": ask_remain,          # 총매도호가잔량(주)
                    "bid_remain": bid_remain,          # 총매수호가잔량(주)
                })
            except Exception as e:
                if batch_num <= 2:
                    log.warning(f"[ME] Parse error row {row}: {e}")

    log.info(f"[ME] Total: {len(all_results)} stocks retrieved, "
             f"API calls: {total_batches}")

    return all_results


def select_target_stocks(conn: CybosConnection,
                         top_n_volume: int = TOP_N_BY_VOLUME,
                         surge_rate: float = CHANGE_RATE_THRESHOLD,
                         top_n_etf: int = TOP_N_ETF) -> List[dict]:
    """
    전종목 중 분석 대상 종목 선정
    기준: 거래대금 상위 N + 등락률 ≥ surge_rate% 종목
    """
    # 1) 전종목 코드 조회
    kospi_codes, kosdaq_codes = get_all_stock_codes(conn)
    all_codes = kospi_codes + kosdaq_codes

    if not all_codes:
        log.error("[SELECT] No stock codes returned from CpCodeMgr!")
        return []

    # 2) MarketEye 일괄 조회
    all_stocks = fetch_market_eye_batch(conn, all_codes)

    if not all_stocks:
        log.error("[SELECT] No stock data returned from MarketEye!")
        return []

    # 3) 유효 종목 필터 (가격 > 0, 거래대금 > 0)
    valid = [s for s in all_stocks if s["price"] > 0 and s["trading_amount"] > 0]
    log.info(f"[SELECT] Valid stocks (price>0, amt>0): {len(valid)}/{len(all_stocks)}")

    # 4) 급등 종목 (등락률 ≥ threshold)
    surging = [s for s in valid if s["change_rate"] >= surge_rate]
    log.info(f"[SELECT] Surging (≥{surge_rate}%): {len(surging)} stocks")

    # 5) 거래대금 상위 N
    by_amount = sorted(valid, key=lambda x: x["trading_amount"], reverse=True)
    top_volume = by_amount[:top_n_volume]
    log.info(f"[SELECT] Top {top_n_volume} by trading amount")

    # 6) 합집합 (중복 제거)
    seen = set()
    targets = []
    for s in surging:
        if s["code"] not in seen:
            s["_source"] = "surge"
            targets.append(s)
            seen.add(s["code"])
    for s in top_volume:
        if s["code"] not in seen:
            s["_source"] = "volume_top"
            targets.append(s)
            seen.add(s["code"])

    # 급등이면서 거래대금 상위인 종목 표시
    surge_codes = {s["code"] for s in surging}
    volume_codes = {s["code"] for s in top_volume}
    for t in targets:
        if t["code"] in surge_codes and t["code"] in volume_codes:
            t["_source"] = "surge+volume"

    log.info(f"[SELECT] Final targets: {len(targets)} stocks "
             f"(surge: {len(surging)}, volume: {top_n_volume}, "
             f"overlap: {len(surge_codes & volume_codes)})")

    # 샘플 로깅
    if targets:
        top5 = sorted(targets, key=lambda x: x["trading_amount"], reverse=True)[:5]
        for s in top5:
            log.info(f"[SELECT]   {s['code_clean']} {s['name']} "
                     f"chg={s['change_rate']:+.1f}% "
                     f"amt={s['trading_amount']/1e8:.0f}억 "
                     f"src={s['_source']}")

    return targets



# ============================================================================
#  StockChart - 틱 데이터 조회
# ============================================================================

# ============================================================================
# [v18.1] 아래는 틱 모드 함수 (주석처리, 원복 시 해제)
# ============================================================================
# def fetch_tick_data_daishin(conn: CybosConnection,
#                             stock_code: str,
#                             last_time: str = "",
#                             today_str: str = "") -> Tuple[List[dict], int]:
#     """
#     StockChart 틱 모드로 당일 체결 틱 데이터 조회
# 
#     Args:
#         stock_code: 종목코드 ('A' prefix 포함 또는 미포함)
#         last_time: 증분수집용 마지막 시간 ("HHMM" 형식)
#         today_str: 오늘 날짜 "YYYYMMDD" (필터용)
# 
#     Returns:
#         (ticks, api_calls)
#         ticks: [{"time": "0930", "price": 72000, "volume": 100,
#                  "cum_sell": 50000, "cum_buy": 60000}, ...]
#                시간순 정렬 (오래된 것 먼저)
#         api_calls: 사용한 API 호출 수
#     """
#     code_a = ensure_a_prefix(stock_code)
#     if not today_str:
#         today_str = datetime.now().strftime("%Y%m%d")
#     today_int = int(today_str)
# 
#     chart_obj = win32com.client.Dispatch("CpSysDib.StockChart")
#     all_ticks = []
#     api_calls = 0
# 
#     for page in range(MAX_TICK_PAGES):
#         conn.wait_for_rate_limit()
# 
#         chart_obj.SetInputValue(0, code_a)
#         chart_obj.SetInputValue(1, ord('2'))  # 개수 기준 요청
#         chart_obj.SetInputValue(4, TICK_REQUEST_COUNT)
#         chart_obj.SetInputValue(5, SC_TICK_FIELDS)
#         chart_obj.SetInputValue(6, ord('T'))  # 틱 차트
#         chart_obj.SetInputValue(9, ord('1'))  # 수정주가
# 
#         chart_obj.BlockRequest()
#         api_calls += 1
# 
#         status = chart_obj.GetDibStatus()
#         if status != 0:
#             msg = chart_obj.GetDibMsg1()
#             log.warning(f"[TICK] {stock_code} page {page}: "
#                         f"status={status}, msg={msg}")
#             break
# 
#         count = chart_obj.GetHeaderValue(3)  # 수신 데이터 개수
#         if count == 0:
#             if page == 0:
#                 log.info(f"[TICK] {stock_code} empty response (no tick data)")
#             break
# 
#         # 필드 인덱스 (SC_TICK_FIELDS 오름차순 정렬 기준)
#         sorted_fields = sorted(SC_TICK_FIELDS)
#         fidx = {fval: idx for idx, fval in enumerate(sorted_fields)}
# 
#         page_ticks = []
#         stop_flag = False
# 
#         for i in range(count):
#             try:
#                 tick_date = int(chart_obj.GetDataValue(fidx[0], i) or 0)
#                 tick_time = int(chart_obj.GetDataValue(fidx[1], i) or 0)
#                 tick_price = int(chart_obj.GetDataValue(fidx[5], i) or 0)
#                 tick_vol = int(chart_obj.GetDataValue(fidx[8], i) or 0)
#                 cum_sell = int(chart_obj.GetDataValue(fidx[10], i) or 0)
#                 cum_buy = int(chart_obj.GetDataValue(fidx[11], i) or 0)
#             except (ValueError, TypeError) as e:
#                 if page == 0 and i < 3:
#                     log.warning(f"[TICK] {stock_code} parse error row {i}: {e}")
#                 continue
# 
#             # 날짜 필터: 당일만
#             if tick_date != today_int:
#                 stop_flag = True
#                 break
# 
#             # 시간 문자열: 정수 → "HHMM" 형식 (예: 930 → "0930")
#             time_str = str(tick_time).zfill(4)
# 
#             # 증분수집: last_time 이전 틱은 이미 수집됨
#             if last_time and time_str <= last_time:
#                 stop_flag = True
#                 break
# 
#             page_ticks.append({
#                 "time": time_str,
#                 "price": abs(tick_price),  # 가격은 절대값
#                 "volume": abs(tick_vol),
#                 "cum_sell": cum_sell,
#                 "cum_buy": cum_buy,
#             })
# 
#         all_ticks.extend(page_ticks)
# 
#         # 첫 페이지 디버그 로깅
#         if page == 0 and conn.call_count <= 10:
#             log.info(f"[TICK] {strip_a_prefix(stock_code)} "
#                      f"page0: {count} rows, valid={len(page_ticks)}, "
#                      f"price={page_ticks[0]['price'] if page_ticks else '?'}")
# 
#         if stop_flag:
#             break
# 
#         # 연속조회 가능 여부
#         has_more = chart_obj.Continue
#         if not has_more:
#             break
# 
#     # StockChart는 최신→과거 순서이므로 뒤집어서 시간순 정렬
#     all_ticks.reverse()
# 
#     if all_ticks and api_calls <= 3:
#         log.info(f"[TICK] {strip_a_prefix(stock_code)} "
#                  f"total: {len(all_ticks)} ticks, "
#                  f"{api_calls} API calls, "
#                  f"time range: {all_ticks[0]['time']}~{all_ticks[-1]['time']}")
# 
#     return all_ticks, api_calls


def fetch_minute_data_daishin(conn: CybosConnection,
                               stock_code: str,
                               today_str: str = "") -> Tuple[List[dict], int]:
    """
    StockChart 1분봉 모드로 당일 체결 데이터 조회

    Args:
        stock_code: 종목코드 ('A' prefix 포함 또는 미포함)
        today_str: 오늘 날짜 "YYYYMMDD" (필터용)

    Returns:
        (bars, api_calls)
        bars: [{"time": "0930", "price": 72000, "volume": 100,
                "cum_sell": 50000, "cum_buy": 60000}, ...]
              시간순 정렬 (오래된 것 먼저)
        api_calls: 사용한 API 호출 수

    Notes:
        - 1분봉은 당일 최대 390봉 (09:00~15:30)
        - 1회 요청으로 당일 전체 커버 가능 (연속조회 불필요)
        - 필드 10/11: 누적 매도/매수 수량 (호가비교 방식)
    """
    code_a = ensure_a_prefix(stock_code)
    if not today_str:
        today_str = datetime.now().strftime("%Y%m%d")
    today_int = int(today_str)

    chart_obj = win32com.client.Dispatch("CpSysDib.StockChart")

    conn.wait_for_rate_limit()

    chart_obj.SetInputValue(0, code_a)
    chart_obj.SetInputValue(1, ord('2'))  # 개수 기준 요청
    chart_obj.SetInputValue(4, MIN_REQUEST_COUNT)
    chart_obj.SetInputValue(5, SC_MIN_FIELDS)
    chart_obj.SetInputValue(6, ord('m'))  # 1분봉 차트
    chart_obj.SetInputValue(9, ord('1'))  # 수정주가

    chart_obj.BlockRequest()

    status = chart_obj.GetDibStatus()
    if status != 0:
        msg = chart_obj.GetDibMsg1()
        log.warning(f"[MIN] {stock_code} error: status={status}, msg={msg}")
        return [], 1

    count = chart_obj.GetHeaderValue(3)  # 수신 데이터 개수
    if count == 0:
        log.info(f"[MIN] {strip_a_prefix(stock_code)} empty response")
        return [], 1

    # 필드 인덱스 (SC_MIN_FIELDS 오름차순 정렬 기준)
    sorted_fields = sorted(SC_MIN_FIELDS)
    fidx = {fval: idx for idx, fval in enumerate(sorted_fields)}

    bars = []
    for i in range(count):
        try:
            bar_date = int(chart_obj.GetDataValue(fidx[0], i) or 0)
            bar_time = int(chart_obj.GetDataValue(fidx[1], i) or 0)
            bar_high = int(chart_obj.GetDataValue(fidx[3], i) or 0)   # [v18.6] 고가
            bar_low = int(chart_obj.GetDataValue(fidx[4], i) or 0)    # [v18.6] 저가
            bar_close = int(chart_obj.GetDataValue(fidx[5], i) or 0)
            bar_vol = int(chart_obj.GetDataValue(fidx[8], i) or 0)
            cum_sell = int(chart_obj.GetDataValue(fidx[10], i) or 0)
            cum_buy = int(chart_obj.GetDataValue(fidx[11], i) or 0)
        except (ValueError, TypeError) as e:
            if i < 3:
                log.warning(f"[MIN] {stock_code} parse error row {i}: {e}")
            continue

        # 날짜 필터: 당일만
        if bar_date != today_int:
            continue

        time_str = str(bar_time).zfill(4)

        bars.append({
            "time": time_str,
            "high": abs(bar_high),      # [v18.6]
            "low": abs(bar_low),        # [v18.6]
            "price": abs(bar_close),
            "volume": abs(bar_vol),
            "cum_sell": cum_sell,
            "cum_buy": cum_buy,
        })

    # StockChart는 최신→과거 순서이므로 뒤집어서 시간순 정렬
    bars.reverse()

    if bars:
        log.info(f"[MIN] {strip_a_prefix(stock_code)} "
                 f"{len(bars)} bars, 1 API call, "
                 f"range: {bars[0]['time']}~{bars[-1]['time']}")

    return bars, 1


# ============================================================================
#  [v18.18] 전일동시간대비용 2일치 1분봉 조회 (고가·저가 포함)
# ============================================================================

def fetch_minute_data_prev_day(conn: CybosConnection,
                                stock_code: str,
                                today_str: str = "") -> Tuple[List[dict], int]:
    """
    전일동시간대비 계산을 위한 전일 1분봉 조회

    당일 포함 최근 2거래일(약 800봉)을 요청하여,
    전일(today_str 이전 거래일) bars만 반환한다.

    반환 bars 필드: {"date": int, "time": "HHMM", "high": int, "low": int,
                    "price": int, "volume": int}
    PineScript의 barTradeVal = (high+low)/2 * volume 계산에 필요한
    고가·저가를 모두 포함한다.

    Returns:
        (prev_day_bars, api_calls)
        prev_day_bars: 전일 1분봉만 (시간순, 오래된 것 먼저)
        api_calls: 사용한 API 호출 수
    """
    code_a = ensure_a_prefix(stock_code)
    if not today_str:
        today_str = datetime.now().strftime("%Y%m%d")
    today_int = int(today_str)

    # 2거래일치 = 당일(최대 390봉) + 전일(최대 390봉) + 여유 20봉
    TWO_DAY_BAR_COUNT = 800

    chart_obj = win32com.client.Dispatch("CpSysDib.StockChart")
    conn.wait_for_rate_limit()

    chart_obj.SetInputValue(0, code_a)
    chart_obj.SetInputValue(1, ord('2'))          # 개수 기준 요청
    chart_obj.SetInputValue(4, TWO_DAY_BAR_COUNT)
    chart_obj.SetInputValue(5, SC_MIN_FIELDS)     # 날짜(0),시간(1),고가(3),저가(4),종가(5),거래량(8),cum_sell(10),cum_buy(11)
    chart_obj.SetInputValue(6, ord('m'))          # 1분봉
    chart_obj.SetInputValue(9, ord('1'))          # 수정주가

    chart_obj.BlockRequest()

    status = chart_obj.GetDibStatus()
    if status != 0:
        msg = chart_obj.GetDibMsg1()
        log.warning(f"[MIN_PREV] {stock_code} error: status={status}, msg={msg}")
        return [], 1

    count = chart_obj.GetHeaderValue(3)
    if count == 0:
        log.info(f"[MIN_PREV] {strip_a_prefix(stock_code)} empty response")
        return [], 1

    sorted_fields = sorted(SC_MIN_FIELDS)
    fidx = {fval: idx for idx, fval in enumerate(sorted_fields)}

    all_bars_2d = []
    for i in range(count):
        try:
            bar_date = int(chart_obj.GetDataValue(fidx[0], i) or 0)
            bar_time = int(chart_obj.GetDataValue(fidx[1], i) or 0)
            bar_high = int(chart_obj.GetDataValue(fidx[3], i) or 0)
            bar_low  = int(chart_obj.GetDataValue(fidx[4], i) or 0)
            bar_close = int(chart_obj.GetDataValue(fidx[5], i) or 0)
            bar_vol  = int(chart_obj.GetDataValue(fidx[8], i) or 0)
        except (ValueError, TypeError) as e:
            if i < 3:
                log.warning(f"[MIN_PREV] {stock_code} parse error row {i}: {e}")
            continue

        # 당일(today_int) 이전 날짜만 전일봉으로 수집
        # (당일봉은 fetch_minute_data_daishin 결과를 그대로 사용)
        if bar_date >= today_int:
            continue
        if bar_date <= 0:
            continue

        time_str = str(bar_time).zfill(4)
        all_bars_2d.append({
            "date":   bar_date,
            "time":   time_str,
            "high":   abs(bar_high),
            "low":    abs(bar_low),
            "price":  abs(bar_close),
            "volume": abs(bar_vol),
        })

    # StockChart는 최신→과거 순서이므로 뒤집어서 시간순 정렬
    all_bars_2d.reverse()

    if not all_bars_2d:
        log.info(f"[MIN_PREV] {strip_a_prefix(stock_code)} no prev-day bars")
        return [], 1

    # 전일 = 수집된 bars 중 가장 최신 날짜 (today_int 미만의 최대값)
    prev_day_date = max(b["date"] for b in all_bars_2d)
    prev_day_bars = [b for b in all_bars_2d if b["date"] == prev_day_date]

    log.info(f"[MIN_PREV] {strip_a_prefix(stock_code)} "
             f"prev_date={prev_day_date} {len(prev_day_bars)} bars, 1 API call")

    return prev_day_bars, 1


# ============================================================================
#  [v18.18] 전일동시간대비 계산
# ============================================================================

def calc_prev_day_sync_ratio(today_bars: List[dict],
                              prev_day_bars: List[dict]) -> float:
    """
    전일동시간대비(%) 계산

    PineScript 로직과 동일:
      - barTradeVal = (high + low) / 2.0 * volume  (봉별 거래대금)
      - 당일 누적거래대금 = today_bars 전체 합산
      - 전일 동시각 누적거래대금 = today_bars 마지막 봉의 time까지
        prev_day_bars에서 이진탐색으로 찾아 합산
      - 비율 = (당일 누적 / 전일 동시각 누적) × 100

    Args:
        today_bars: 당일 1분봉 (fetch_minute_data_daishin 결과)
        prev_day_bars: 전일 1분봉 (fetch_minute_data_prev_day 결과)

    Returns:
        pct (float): 전일동시간대비 (%)
                     0.0 → 데이터 없음 (표시: "-")
    """
    if not today_bars or not prev_day_bars:
        log.info("[SYNC_RATIO] today_bars 또는 prev_day_bars 없음 → 0.0 반환")
        return 0.0

    # ── 당일 누적거래대금 계산 ──
    cum_today = 0.0
    for b in today_bars:
        h = b.get("high", b.get("price", 0))
        l = b.get("low",  b.get("price", 0))
        v = b.get("volume", 0)
        bar_val = (h + l) / 2.0 * v
        cum_today += bar_val

    if cum_today <= 0:
        log.info("[SYNC_RATIO] cum_today <= 0 → 0.0 반환")
        return 0.0

    # ── 현재 시각(당일 마지막 봉 time) 기준으로 전일 동시각 누적 산출 ──
    cur_time = today_bars[-1]["time"]  # "HHMM" 문자열

    # 전일 bars에서 cur_time 이하 봉만 이진탐색으로 찾기
    # prev_day_bars는 시간순 정렬되어 있음
    lo, hi, best = 0, len(prev_day_bars) - 1, -1
    while lo <= hi:
        mid = (lo + hi) // 2
        if prev_day_bars[mid]["time"] <= cur_time:
            best = mid
            lo = mid + 1
        else:
            hi = mid - 1

    if best < 0:
        log.info(f"[SYNC_RATIO] 전일 동시각 봉 없음 (cur_time={cur_time}) → 0.0 반환")
        return 0.0

    # 전일 동시각까지 누적
    cum_prev = 0.0
    for b in prev_day_bars[:best + 1]:
        h = b.get("high", b.get("price", 0))
        l = b.get("low",  b.get("price", 0))
        v = b.get("volume", 0)
        bar_val = (h + l) / 2.0 * v
        cum_prev += bar_val

    if cum_prev <= 0:
        log.info(f"[SYNC_RATIO] cum_prev <= 0 (cur_time={cur_time}) → 0.0 반환")
        return 0.0

    pct = cum_today / cum_prev * 100.0
    log.info(f"[SYNC_RATIO] cur_time={cur_time} "
             f"cum_today={cum_today/1e8:.1f}억 "
             f"cum_prev={cum_prev/1e8:.1f}억 "
             f"ratio={pct:.1f}%")
    return round(pct, 1)


# ============================================================================
#  [v18.6] 일봉 데이터 조회 — 예상마감대금 비교 + 20일 고가
# ============================================================================

def fetch_daily_ohlcv(conn: CybosConnection,
                       stock_code: str,
                       bar_count: int = DAILY_BAR_COUNT) -> Tuple[List[dict], int]:
    """
    StockChart 일봉 모드로 최근 N거래일 OHLCV + 거래대금 조회

    Args:
        stock_code: 종목코드 ('A' prefix 포함 또는 미포함)
        bar_count: 요청 봉 개수 (25 = 20거래일 + 여유분)

    Returns:
        (daily_bars, api_calls)
        daily_bars: [{"date": 20260401, "high": 15000, "close": 14500,
                      "trading_amount": 123456789}, ...]
                    날짜순 정렬 (오래된 것 먼저)
        api_calls: 사용한 API 호출 수
    """
    code_a = ensure_a_prefix(stock_code)

    chart_obj = win32com.client.Dispatch("CpSysDib.StockChart")
    conn.wait_for_rate_limit()

    sorted_fields = sorted(SC_DAILY_FIELDS)
    fidx = {fval: idx for idx, fval in enumerate(sorted_fields)}

    chart_obj.SetInputValue(0, code_a)
    chart_obj.SetInputValue(1, ord('2'))     # 개수 기준
    chart_obj.SetInputValue(4, bar_count)
    chart_obj.SetInputValue(5, SC_DAILY_FIELDS)
    chart_obj.SetInputValue(6, ord('D'))     # 일봉
    chart_obj.SetInputValue(9, ord('1'))     # 수정주가

    chart_obj.BlockRequest()

    status = chart_obj.GetDibStatus()
    if status != 0:
        msg = chart_obj.GetDibMsg1()
        log.warning(f"[DAILY] {stock_code} error: status={status}, msg={msg}")
        return [], 1

    count = chart_obj.GetHeaderValue(3)
    if count == 0:
        log.info(f"[DAILY] {strip_a_prefix(stock_code)} empty response (0 bars)")
        return [], 1

    bars = []
    for i in range(count):
        try:
            bar_date = int(chart_obj.GetDataValue(fidx[0], i) or 0)
            bar_high = int(chart_obj.GetDataValue(fidx[3], i) or 0)
            bar_close = int(chart_obj.GetDataValue(fidx[5], i) or 0)
            bar_trade_amt = int(chart_obj.GetDataValue(fidx[9], i) or 0)
        except (ValueError, TypeError) as e:
            if i < 3:
                log.warning(f"[DAILY] {stock_code} parse error row {i}: {e}")
            continue

        if bar_date <= 0:
            continue

        bars.append({
            "date": bar_date,
            "high": abs(bar_high),
            "close": abs(bar_close),
            "trading_amount": abs(bar_trade_amt),
        })

    # StockChart는 최신→과거 순서이므로 뒤집어서 날짜순 정렬
    bars.reverse()

    if bars:
        log.debug(f"[DAILY] {strip_a_prefix(stock_code)} "
                  f"{len(bars)} days, range: {bars[0]['date']}~{bars[-1]['date']}")

    return bars, 1


# ============================================================================
#  [v18.9] 한국 정규장 intraday 누적 거래량 프로파일 (U자형)
# ============================================================================
# 단순 시간 비례 보정(390/elapsed)은 장 초반/마감 거래 집중을 무시해
# est_eod를 과대 추정함. 경험적 U자형 누적 거래량 프로파일로 보정.
# 출처: 한국거래소 일반론 (개장 동시호가 + 갭 갈피, 마감 동시호가 집중)
# ⚠️ 경험적 근사값. 종목·시장 상황에 따라 편차 있음. 백테스팅으로 보정 가능.
# 형식: (개장 후 경과 분, 누적 거래량 비율)
KR_INTRADAY_VOLUME_PROFILE = [
    (0,   0.000),
    (30,  0.180),  # 09:30 — 동시호가+갭 갈피 (전체 18%)
    (60,  0.270),  # 10:00
    (90,  0.330),  # 10:30
    (120, 0.390),  # 11:00
    (150, 0.440),  # 11:30
    (180, 0.490),  # 12:00 (점심시간 둔화 시작)
    (210, 0.540),  # 12:30
    (240, 0.600),  # 13:00
    (270, 0.660),  # 13:30
    (300, 0.720),  # 14:00
    (330, 0.790),  # 14:30
    (360, 0.870),  # 15:00 (마감 램프업 시작)
    (380, 0.930),  # 15:20 (마감 동시호가 직전)
    (390, 1.000),  # 15:30 (마감)
]


def kr_intraday_cumulative_fraction(elapsed_mins: float) -> float:
    """
    경과 분에 따른 한국 정규장 누적 거래량 비율 (U자형, 선형 보간)

    Args:
        elapsed_mins: 09:00부터 경과한 분 (1~390)
    Returns:
        0.001~1.0 사이의 누적 비율 (0 방지)
    """
    if elapsed_mins <= 0:
        return 0.001
    if elapsed_mins >= 390:
        return 1.0
    for i in range(len(KR_INTRADAY_VOLUME_PROFILE) - 1):
        t0, f0 = KR_INTRADAY_VOLUME_PROFILE[i]
        t1, f1 = KR_INTRADAY_VOLUME_PROFILE[i + 1]
        if t0 <= elapsed_mins <= t1:
            ratio = (elapsed_mins - t0) / (t1 - t0)
            return max(0.001, f0 + (f1 - f0) * ratio)
    return 1.0


def calc_estimated_eod_metrics(daily_bars: List[dict],
                                current_price: int,
                                current_trading_amount: int,
                                target_date_str: str) -> dict:
    """
    예상마감대금 관련 지표 + 20일 고가대비 계산

    Args:
        daily_bars: fetch_daily_ohlcv() 결과 (날짜순)
        current_price: 현재가 (MarketEye)
        current_trading_amount: 현재 누적 거래대금 (MarketEye, 원 단위)
        target_date_str: 분석 대상 날짜 "YYYYMMDD"

    Returns: {
        "est_eod": int,             # 예상마감대금 (원)
        "est_eod_vs_d1": float,     # 예상대비D-1 (%, +이면 어제보다 큼)
        "est_eod_vs_d2": float,     # 예상대비D-2 (%, +이면 그저께보다 큼)
        "close_vs_20d_high": float, # 20일고가대비 (%, 0이면 최고가, -이면 아래)
    }
    """
    default = {
        "est_eod": 0,
        "est_eod_vs_d1": 0.0,
        "est_eod_vs_d2": 0.0,
        "close_vs_20d_high": 0.0,
    }

    if not daily_bars or current_trading_amount <= 0:
        return default

    target_int = int(target_date_str)

    # ── 예상마감대금 계산 ──
    # [v18.9] 메인값은 기존 단순 시간 비례 유지(검증 목적), U자형 보정값은 별도 필드로 보조 산출
    now = datetime.now()
    now_mins = now.hour * 60 + now.minute
    open_mins = 9 * 60  # 09:00

    # 장중이면 경과시간 기반 추정, 장외면 390분(마감) 적용
    elapsed = now_mins - open_mins
    elapsed_clamped = max(1, min(elapsed, TOTAL_SESSION_MINUTES))

    # 장 마감 후이거나 전일 분석 모드이면 경과=390 (보정 없음)
    if now.hour > 15 or (now.hour == 15 and now.minute >= 30):
        elapsed_clamped = TOTAL_SESSION_MINUTES

    # target_date가 오늘이 아니면 (전일 분석) → 이미 마감된 날
    today_str = now.strftime("%Y%m%d")
    if target_date_str != today_str:
        elapsed_clamped = TOTAL_SESSION_MINUTES

    # 메인값: 단순 시간 비례 (원래 로직)
    est_eod = int(current_trading_amount * (TOTAL_SESSION_MINUTES / elapsed_clamped))

    # 보조값: U자형 누적 거래량 프로파일 보정 (검증 비교용)
    # elapsed=390이면 cum_fraction=1.0이라 est_eod_u == est_eod (전일 분석 시 동일)
    cum_fraction = kr_intraday_cumulative_fraction(elapsed_clamped)
    est_eod_u = int(current_trading_amount / cum_fraction)

    # ── 전일/전전일 거래대금 찾기 ──
    # target_date 이전의 거래일을 날짜 역순으로 정렬
    prior_days = [b for b in daily_bars if b["date"] < target_int]
    # 날짜 역순 (가장 최근 먼저)
    prior_days.sort(key=lambda x: x["date"], reverse=True)

    d1_amt = prior_days[0]["trading_amount"] if len(prior_days) >= 1 else 0
    d2_amt = prior_days[1]["trading_amount"] if len(prior_days) >= 2 else 0

    est_eod_vs_d1 = 0.0
    est_eod_vs_d2 = 0.0

    if d1_amt > 0:
        est_eod_vs_d1 = (est_eod / d1_amt - 1) * 100
    if d2_amt > 0:
        est_eod_vs_d2 = (est_eod / d2_amt - 1) * 100

    # ── 20거래일 최고고가 대비 ──
    # target_date 포함 기준: 당일 일봉 고가도 포함한 최근 20거래일 최고고가 대비
    # (당일 일봉 고가는 장 마감 후 확정값, 장중에는 미완성값일 수 있음 — 참고용)
    # 주의: 장중 실행 시 당일 일봉 고가가 API에서 미확정값으로 수신될 수 있으므로
    #       이 컬럼은 장 마감 후 분석에서 가장 의미가 정확합니다.
    all_bars_up_to = [b for b in daily_bars if b["date"] <= target_int]
    all_bars_up_to.sort(key=lambda x: x["date"], reverse=True)
    # 최근 20일치 고가 (target_date 일봉 포함)
    recent_20 = all_bars_up_to[:20]

    close_vs_20d_high = 0.0
    if recent_20 and current_price > 0:
        max_high_20d = max(b["high"] for b in recent_20)
        if max_high_20d > 0:
            close_vs_20d_high = (current_price / max_high_20d - 1) * 100

    result = {
        "est_eod": est_eod,
        "est_eod_u": est_eod_u,  # [v18.9] U자형 보조값 (검증 비교용)
        "est_eod_vs_d1": round(est_eod_vs_d1, 1),
        "est_eod_vs_d2": round(est_eod_vs_d2, 1),
        "close_vs_20d_high": round(close_vs_20d_high, 1),
    }

    log.debug(f"[EST_EOD] est={est_eod/1e8:.0f}억 (u={est_eod_u/1e8:.0f}억) "
              f"D-1={est_eod_vs_d1:+.1f}% D-2={est_eod_vs_d2:+.1f}% "
              f"20dH={close_vs_20d_high:+.1f}%")

    return result


# ============================================================================
#  [v18.9] 거래대금 회귀 스크리닝 (OU 직관 차용 휴리스틱 — 백테스팅 미검증)
# ============================================================================

def calc_turnover_reversion(daily_bars: list,
                             est_eod: int,
                             target_date_str: str) -> dict:
    """
    거래대금 평균회귀 지표 계산 (테스트용 실험 지표)

    ⚠️ 수학적 주의사항:
      - 이 함수는 OU(Ornstein-Uhlenbeck) 과정의 직관을 차용한 실험적 지표입니다.
      - OU 과정의 정식 추정 절차(MLE, Kalman filter 등)를 따르지 않으며,
        서로 다른 목적의 추정량들을 하나의 점수로 엮은 혼합 휴리스틱입니다.
      - θ = 1-β는 연속시간 OU를 이산 AR(1)로 변환할 때의 1차 근사입니다.
        정확한 관계는 θ = -ln(β)이며, β가 0.5~0.7 수준이면 오차 15~28%입니다.
      - 회복시간 = 이탈도/θ × 390분은 엄밀한 OU 도출식이 아니라,
        OU의 선형 스케일링을 차용한 직관적 점수화입니다.
        (OU 정확 동작은 지수감쇠: X(t) = μ + (X0-μ)e^{-θt})
      - 백테스팅으로 유효성 검증 전까지 실전 의사결정에 단독 사용 금지.

    핵심 아이디어:
      - μ_V = max(20일 중앙값, 20일 평균) → 테마 초기/중기/후기 모두 커버
      - 이탈도(σ) = (μ_V - est_eod) / σ_20 → 클수록 회복 여지 큼
      - θ ≈ 1 - β  (AR(1) 계수 β에서 1차 근사, β가 1에 가까울수록 정확)
      - 회복예상 = (이탈도/θ) × 390분 → "X시간 Y분" 포맷 (스케일링 휴리스틱)

    Args:
        daily_bars: fetch_daily_ohlcv() 결과 (날짜순, 오래된 것 먼저)
        est_eod: 오늘 예상마감대금 (원 단위, calc_estimated_eod_metrics에서 산출)
        target_date_str: 분석 대상 날짜 "YYYYMMDD"

    Returns: {
        "itatdo": float or None,         # 이탈도 (σ 단위)
        "recovery_mins": float or None,  # 회복 예상 시간 (분, 휴리스틱)
        "mu_v": int,                     # 기준대금 μ (원)
        "theta": float or None,          # AR(1) 근사 회귀속도 (= 1-β, 1차 근사)
        "note": str,                     # 진단 메모
    }
    """
    default = {"itatdo": None, "recovery_mins": None,
               "mu_v": 0, "theta": None, "note": "N/A"}

    if not daily_bars or est_eod <= 0:
        return {**default, "note": "일봉없음 또는 예상대금0"}

    target_int = int(target_date_str)

    # target_date 이전 거래일만 사용 (오늘 확정 안 된 데이터 제외)
    prior_days = [b for b in daily_bars if b["date"] < target_int]
    prior_days.sort(key=lambda x: x["date"], reverse=True)  # 최신 먼저

    if len(prior_days) < 5:
        log.warning(f"[TV_REV] prior_days={len(prior_days)} < 5 — 데이터 부족")
        return {**default, "note": f"prior일수={len(prior_days)}<5"}

    # 최근 20거래일 trading_amount 추출
    recent = prior_days[:20]
    amounts = [b["trading_amount"] for b in recent]
    n = len(amounts)

    # ── μ_V = max(중앙값, 평균) ──
    sorted_a = sorted(amounts)
    # 중앙값: 짝수면 두 중간값 평균
    mid = n // 2
    if n % 2 == 1:
        median_v = sorted_a[mid]
    else:
        median_v = (sorted_a[mid - 1] + sorted_a[mid]) // 2
    mean_v = sum(amounts) // n
    mu_v = max(median_v, mean_v)

    # ── σ (모표준편차, numpy 없이 순수 Python) ──
    variance = sum((x - mean_v) ** 2 for x in amounts) / n
    sigma_v = variance ** 0.5 if variance > 0 else 0.0

    if sigma_v <= 0:
        return {**default, "mu_v": mu_v,
                "note": "σ=0 (거래대금 변동없음)"}

    # ── 이탈도 ──
    # est_eod: 오늘 예상마감대금 (현재 시점 추정치)
    itatdo = (mu_v - est_eod) / sigma_v

    # ── AR(1) β 추정 (최소자승, 순수 Python) ──
    # V_t = α + β×V_{t-1} + ε  →  β = Cov(x,y)/Var(x)
    # amounts는 최신→과거 순이므로 뒤집어서 시간순으로 처리
    time_order = list(reversed(amounts))  # 과거→최신
    if len(time_order) >= 4:
        x_lag = time_order[:-1]   # V_{t-1}
        y_cur = time_order[1:]    # V_t
        nx = len(x_lag)
        mean_x = sum(x_lag) / nx
        mean_y = sum(y_cur) / nx
        cov_xy = sum((x_lag[i] - mean_x) * (y_cur[i] - mean_y)
                     for i in range(nx)) / nx
        var_x = sum((v - mean_x) ** 2 for v in x_lag) / nx
        raw_beta = (cov_xy / var_x) if var_x > 0 else 0.5
        # β를 [0.01, 0.99] 클리핑 (불안정 케이스 방지, 하한 0.01은 ln(0) 방지)
        # raw_beta를 별도 보존하여 역상관 경고 판단에 사용
        beta = max(0.01, min(0.99, raw_beta))
        # θ = -ln(β) : 연속 OU ↔ 이산 AR(1) 정확 변환
        # 근거: β = e^{-θΔt} (Δt=1거래일) → θ = -ln(β)
        # 구 방식 θ=1-β는 테일러 1차 근사: β=0.9이면 오차 ~5%, β=0.5이면 오차 ~28%
        import math as _math
        theta = -_math.log(beta)  # 단위: 1/거래일
    else:
        raw_beta = 0.5
        beta = 0.5
        import math as _math
        theta = -_math.log(beta)  # = ln(2) ≈ 0.693

    # ── 회복 예상 기간 (분 단위) ──
    # ⚠️ 이 공식은 엄밀한 OU 도출식이 아닌 직관적 스케일링입니다.
    # OU 정확 동작: X(t) = μ + (X0-μ)e^{-θt}  (지수감쇠, 완전 도달 시간 = ∞)
    # 코드 방식: 회복일 수 = 이탈도/θ  (선형 비례, 휴리스틱)
    # 1 거래일 = 390분
    # 이탈도 ≤ 0 → 이미 μ 이상 (회복 완료 상태)
    if itatdo <= 0:
        recovery_mins = 0.0
    elif theta < 0.01:
        # θ가 너무 작으면 회귀력 없음 → None
        recovery_mins = None
    else:
        recovery_days = itatdo / theta
        recovery_mins = recovery_days * 390.0

    log.info(f"[TV_REV] μ={mu_v/1e8:.0f}억(median={median_v/1e8:.0f}/mean={mean_v/1e8:.0f}) "
             f"σ={sigma_v/1e8:.0f}억 est={est_eod/1e8:.0f}억 "
             f"이탈도={itatdo:.2f}σ raw_β={raw_beta:.3f} β={beta:.3f} θ≈{theta:.3f} "
             f"회복={f'{recovery_mins:.0f}분' if recovery_mins is not None else 'N/A'}")

    # 이상 징후 경고 (raw_beta로 검사 — 클리핑 전 원래 값)
    if raw_beta < 0:
        log.warning(f"[TV_REV] ⚠️ raw_β={raw_beta:.3f} < 0 — 거래대금 역상관. θ 신뢰도 낮음 (클리핑으로 β=0 처리됨)")
    if abs(itatdo) > 5.0:
        log.warning(f"[TV_REV] ⚠️ 이탈도={itatdo:.1f}σ — 극단값. 데이터 이상 가능성")

    note = f"n={n} raw_β={raw_beta:.2f} β={beta:.2f} θ≈{theta:.2f}"

    return {
        "itatdo": round(itatdo, 2),
        "recovery_mins": round(recovery_mins, 1) if recovery_mins is not None else None,
        "mu_v": mu_v,
        "theta": round(theta, 3),
        "note": note,
    }


def fmt_recovery_mins(mins) -> str:
    """회복 예상 분 수 → '5시간 34분' 형식 변환"""
    if mins is None:
        return "-"
    if mins <= 0:
        return "충분"
    total = int(round(mins))
    hours = total // 60
    rem = total % 60
    if hours == 0:
        return f"{rem}분"
    if rem == 0:
        return f"{hours}시간"
    return f"{hours}시간 {rem}분"


# ============================================================================
#  [v18.7] 최근 5거래일 상/하한가 마감 판별
# ============================================================================

LIMIT_CLOSE_THRESHOLD = 0.29   # 29% — 호가 단위 반올림 감안 (실제 상한가 30%)
LIMIT_CLOSE_LOOKBACK = 5       # 최근 5거래일

def check_limit_close_5d(daily_bars: List[dict],
                          target_date_str: str) -> dict:
    """
    최근 5거래일 내 정규장 종가가 상한가 또는 하한가로 마감한 날이 있는지 판별

    판별 기준: 전일종가 대비 등락률 >= +29% (상한가) 또는 <= -29% (하한가)
    - KRX 가격제한폭 ±30%이나, 호가 단위(tick size) 반올림으로 29.xx%일 수 있음
    - ⚠️ 실무 근사 판별: 대부분의 일반 종목에서는 29% 이상 종가 = 상한가로 보아도
      무방하나, ETN·재상장·가격제한폭 변경·특수 케이스에서는 예외가 존재할 수 있음.
      수학적 필연이 아닌 실무 휴리스틱으로 이해할 것.

    Args:
        daily_bars: fetch_daily_ohlcv() 결과 (날짜순, 오래된 것 먼저)
        target_date_str: 분석 대상 날짜 "YYYYMMDD"

    Returns: {
        "has_limit": bool,        # 5거래일 내 상/하한가 마감 있음?
        "limit_days": list,       # [{"date": 20260401, "type": "upper", "pct": 29.97}, ...]
        "limit_count": int,       # 상/하한가 마감 일수
    }
    """
    default = {"has_limit": False, "limit_days": [], "limit_count": 0}

    if not daily_bars or len(daily_bars) < 2:
        return default

    target_int = int(target_date_str)

    # target_date 이하의 봉만 필터 (날짜순 유지)
    filtered = [b for b in daily_bars if b["date"] <= target_int]
    if len(filtered) < 2:
        return default

    # 최근 5거래일 = filtered의 마지막 5개 봉 (각 봉의 등락률 판단 위해 이전 봉도 필요)
    # filtered[-6:]이면 최대 5쌍의 (전일, 당일) 비교 가능
    recent_pairs = filtered[-(LIMIT_CLOSE_LOOKBACK + 1):]

    limit_days = []
    for i in range(1, len(recent_pairs)):
        prev_close = recent_pairs[i - 1]["close"]
        curr_close = recent_pairs[i]["close"]
        curr_date = recent_pairs[i]["date"]

        if prev_close <= 0 or curr_close <= 0:
            continue

        change_pct = (curr_close / prev_close - 1)  # 소수 비율

        if change_pct >= LIMIT_CLOSE_THRESHOLD:
            limit_days.append({
                "date": curr_date,
                "type": "upper",
                "pct": round(change_pct * 100, 2),
            })
        elif change_pct <= -LIMIT_CLOSE_THRESHOLD:
            limit_days.append({
                "date": curr_date,
                "type": "lower",
                "pct": round(change_pct * 100, 2),
            })

    return {
        "has_limit": len(limit_days) > 0,
        "limit_days": limit_days,
        "limit_count": len(limit_days),
    }


# ============================================================================
#  [v18.4] Capture Ratio — 지수 대비 상승/하락 포착비율
# ============================================================================

def fetch_bars_multi_day(conn: CybosConnection, code: str,
                          bar_count: int = CAPTURE_BAR_COUNT) -> Tuple[List[dict], int]:
    """
    StockChart 분봉 데이터 다일치 조회 (Capture Ratio 계산용)

    Args:
        code: 종목코드 (주식 'A005930', 지수 'U001' — 그대로 전달, 접두사 가공 없음)
        bar_count: 요청 봉 개수 (2000 = 약 5거래일)

    Returns:
        (bars, api_calls)
        bars: [{"date": 20260309, "time": "0930", "close": 72000.0, "volume": 100}, ...]
              시간순 정렬 (오래된 것 먼저)
    """
    chart_obj = win32com.client.Dispatch("CpSysDib.StockChart")
    conn.wait_for_rate_limit()

    sorted_fields = sorted(CAPTURE_FIELDS)
    fidx = {fval: idx for idx, fval in enumerate(sorted_fields)}

    chart_obj.SetInputValue(0, code)         # 종목코드 or 지수코드
    chart_obj.SetInputValue(1, ord('2'))     # 개수 기준
    chart_obj.SetInputValue(4, bar_count)
    chart_obj.SetInputValue(5, CAPTURE_FIELDS)
    chart_obj.SetInputValue(6, ord('m'))     # 분봉
    chart_obj.SetInputValue(9, ord('1'))     # 수정주가

    chart_obj.BlockRequest()

    status = chart_obj.GetDibStatus()
    if status != 0:
        msg = chart_obj.GetDibMsg1()
        log.warning(f"[CAPTURE] {code} StockChart error: status={status}, msg={msg}")
        return [], 1

    count = chart_obj.GetHeaderValue(3)
    if count == 0:
        log.info(f"[CAPTURE] {code} empty response (0 bars)")
        return [], 1

    bars = []
    for i in range(count):
        try:
            bar_date = int(chart_obj.GetDataValue(fidx[0], i) or 0)
            bar_time = int(chart_obj.GetDataValue(fidx[1], i) or 0)
            bar_close = float(chart_obj.GetDataValue(fidx[5], i) or 0)
            bar_vol = int(chart_obj.GetDataValue(fidx[8], i) or 0)
        except (ValueError, TypeError):
            continue

        if bar_close <= 0:
            continue

        time_str = str(bar_time).zfill(4)
        bars.append({
            "date": bar_date,
            "time": time_str,
            "close": bar_close,
            "volume": bar_vol,
        })

    # StockChart는 최신→과거 순서이므로 뒤집어서 시간순 정렬
    bars.reverse()

    if bars:
        unique_dates = len(set(b["date"] for b in bars))
        log.info(f"[CAPTURE] {code} | 1min | "
                 f"{len(bars)} bars, {unique_dates} days, "
                 f"range: {bars[0]['date']}/{bars[0]['time']}~"
                 f"{bars[-1]['date']}/{bars[-1]['time']}")
    else:
        log.warning(f"[CAPTURE] {code} | 0 valid bars after filtering")

    return bars, 1


def _aggregate_bars(bars: List[dict], period_minutes: int) -> List[dict]:
    """
    1분봉을 N분봉으로 집계 (in-memory, API 호출 없음)

    각 윈도우의 마지막 종가를 사용, 거래량은 합산.
    날짜 경계를 넘지 않도록 날짜별로 분리하여 집계.
    """
    if not bars or period_minutes <= 1:
        return bars

    # 날짜별로 분리
    by_date = defaultdict(list)
    for b in bars:
        by_date[b["date"]].append(b)

    aggregated = []
    for d in sorted(by_date.keys()):
        day_bars = by_date[d]
        for i in range(0, len(day_bars), period_minutes):
            window = day_bars[i:i + period_minutes]
            if not window:
                continue
            last = window[-1]
            total_vol = sum(b["volume"] for b in window)
            aggregated.append({
                "date": last["date"],
                "time": last["time"],
                "close": last["close"],
                "volume": total_vol,
            })

    return aggregated


def _calc_capture_inner(stock_bars: List[dict],
                         index_bars: List[dict],
                         timeframe: str) -> dict:
    """
    Capture Ratio 내부 계산 (타임프레임별)

    수익률 = (close[i] - close[i-1]) / close[i-1]
    같은 날짜 내의 연속 봉만 계산 (장 시작/마감 경계 제외)
    (date, time) 기준으로 지수와 종목 수익률 매칭
    """
    default = {
        "up_cap": 0.0, "down_cap": 0.0,
        "ud_ratio": 0.0, "ud_diff": 0.0,
        "matched_count": 0, "timeframe": timeframe,
        "up_count": 0, "down_count": 0,
    }

    if len(stock_bars) < 5 or len(index_bars) < 5:
        return default

    # 1) 수익률 계산 (같은 날짜 내 연속 봉 간 차분)
    def calc_returns(bars):
        returns = {}
        for i in range(1, len(bars)):
            prev = bars[i - 1]
            curr = bars[i]
            if prev["date"] != curr["date"]:
                continue
            if prev["close"] <= 0:
                continue
            ret = (curr["close"] - prev["close"]) / prev["close"]
            key = (curr["date"], curr["time"])
            returns[key] = ret
        return returns

    stock_returns = calc_returns(stock_bars)
    index_returns = calc_returns(index_bars)

    # 2) 매칭: 같은 (date, time)에 둘 다 수익률이 있는 경우만
    matched_keys = set(stock_returns.keys()) & set(index_returns.keys())

    # 지수 수익률 0인 봉은 분류 불가이므로 제외
    matched = []
    for k in sorted(matched_keys):
        sr = stock_returns[k]
        ir = index_returns[k]
        if ir == 0:
            continue
        matched.append((sr, ir))

    if len(matched) < 5:
        return {**default, "matched_count": len(matched)}

    # 3) 지수 상승/하락 분봉 분리
    up_pairs = [(sr, ir) for sr, ir in matched if ir > 0]
    down_pairs = [(sr, ir) for sr, ir in matched if ir < 0]

    # 4) Capture Ratio 계산
    if up_pairs:
        avg_stock_up = sum(sr for sr, _ in up_pairs) / len(up_pairs)
        avg_index_up = sum(ir for _, ir in up_pairs) / len(up_pairs)
        up_cap = (avg_stock_up / avg_index_up) * 100 if avg_index_up != 0 else 0.0
    else:
        up_cap = 0.0

    if down_pairs:
        avg_stock_down = sum(sr for sr, _ in down_pairs) / len(down_pairs)
        avg_index_down = sum(ir for _, ir in down_pairs) / len(down_pairs)
        down_cap = (avg_stock_down / avg_index_down) * 100 if avg_index_down != 0 else 0.0
    else:
        down_cap = 0.0

    # 5) U/D Ratio, U-D
    if down_cap != 0:
        ud_ratio = up_cap / down_cap
    else:
        ud_ratio = 0.0

    ud_diff = up_cap - down_cap

    return {
        "up_cap": up_cap,
        "down_cap": down_cap,
        "ud_ratio": ud_ratio,
        "ud_diff": ud_diff,
        "matched_count": len(matched),
        "timeframe": timeframe,
        "up_count": len(up_pairs),
        "down_count": len(down_pairs),
    }


def calculate_capture_ratio(stock_bars: List[dict],
                             index_bars: List[dict]) -> dict:
    """
    Up/Down Capture Ratio 계산 (1분봉 → 3분봉 → 5분봉 fallback)

    "지수가 오를 때 더 오르고, 내릴 때 덜 떨어지는" 종목을 수치화.

    Returns: {
        "up_cap": float,      # Upside Capture (%)
        "down_cap": float,    # Downside Capture (%)
        "ud_ratio": float,    # U/D Ratio = Up Cap / Down Cap
        "ud_diff": float,     # U-D = Up Cap - Down Cap
        "matched_count": int, # 매칭된 수익률 개수
        "timeframe": str,     # 사용된 타임프레임 ("1m", "3m", "5m")
        "up_count": int,      # 지수 상승 분봉 수
        "down_count": int,    # 지수 하락 분봉 수
    }
    """
    default = {
        "up_cap": 0.0, "down_cap": 0.0,
        "ud_ratio": 0.0, "ud_diff": 0.0,
        "matched_count": 0, "timeframe": "-",
        "up_count": 0, "down_count": 0,
    }

    if len(stock_bars) < 10 or len(index_bars) < 10:
        return default

    # 타임프레임별 시도: 1분 → 3분 → 5분 (집계 방식, 추가 API 호출 없음)
    result = default
    for period, label, min_req in [(1, "1m", CAPTURE_MIN_MATCHED),
                                    (3, "3m", CAPTURE_MIN_MATCHED_3M),
                                    (5, "5m", CAPTURE_MIN_MATCHED_5M)]:
        if period == 1:
            s_bars = stock_bars
            i_bars = index_bars
        else:
            s_bars = _aggregate_bars(stock_bars, period)
            i_bars = _aggregate_bars(index_bars, period)

        result = _calc_capture_inner(s_bars, i_bars, label)

        if result["matched_count"] >= min_req:
            return result

    # 모든 타임프레임에서 매칭 부족 → 마지막 시도 결과 반환
    return result


# ============================================================================
#  대량체결 분석 (호가비교 방식)
# ============================================================================

# ============================================================================
# [v18.1] 아래는 틱 단위 분석 함수 (주석처리, 원복 시 해제)
# ============================================================================
# def analyze_block_trades(ticks: List[dict],
#                          threshold: int = BLOCK_TRADE_THRESHOLD) -> dict:
#     """
#     틱 데이터에서 대량체결 분석 (대신증권 호가비교 방식)
# 
#     매수/매도 구분 로직:
#       - cum_buy(필드11): 누적 체결매수수량 (매수주문이 매도호가에서 체결)
#       - cum_sell(필드10): 누적 체결매도수량 (매도주문이 매수호가에서 체결)
#       - 연속 틱 간 차분으로 틱별 매수/매도 수량 계산
#       - buy_delta > sell_delta → 매수 체결
#       - sell_delta > buy_delta → 매도 체결
# 
#     Returns: {
#         "block_buy_count": int,    # 대량매수 건수
#         "block_buy_amount": int,   # 대량매수 금액
#         "block_sell_count": int,   # 대량매도 건수
#         "block_sell_amount": int,  # 대량매도 금액
#         "net_amount": int,         # 순매수금액 (매수-매도)
#         "buy_ratio": float,        # 매수비율 (대량매수금액/전체대량금액 %)
#         "direction": str,          # 방향성 판정
#         "block_ticks": list,       # 대량체결 틱 목록
#         "total_ticks": int,        # 전체 틱 수
#     }
#     """
#     result = {
#         "block_buy_count": 0,
#         "block_buy_amount": 0,
#         "block_sell_count": 0,
#         "block_sell_amount": 0,
#         "net_amount": 0,
#         "buy_ratio": 0.0,
#         "direction": "-",
#         "block_ticks": [],
#         "total_ticks": len(ticks),
#     }
# 
#     if len(ticks) < 2:
#         return result
# 
#     # 시간순 정렬된 틱 (index 0 = 가장 오래된 틱)
#     for i in range(1, len(ticks)):
#         prev = ticks[i - 1]
#         curr = ticks[i]
# 
#         # 누적값 차분 → 이 틱의 매수/매도 체결 수량
#         buy_delta = curr["cum_buy"] - prev["cum_buy"]
#         sell_delta = curr["cum_sell"] - prev["cum_sell"]
# 
#         # 유효성 검증: 누적값은 증가만 가능
#         if buy_delta < 0:
#             buy_delta = 0
#         if sell_delta < 0:
#             sell_delta = 0
# 
#         price = curr["price"]
#         volume = curr["volume"]
# 
#         if price <= 0 or volume <= 0:
#             continue
# 
#         trade_amount = price * volume
# 
#         if trade_amount < threshold:
#             continue
# 
#         # 매수/매도 판정: 호가비교 차분 기준
#         is_buy = buy_delta >= sell_delta
# 
#         block_tick = {
#             "time": curr["time"],
#             "price": price,
#             "qty": volume,
#             "amount": trade_amount,
#             "is_buy": is_buy,
#             "buy_delta": buy_delta,
#             "sell_delta": sell_delta,
#         }
#         result["block_ticks"].append(block_tick)
# 
#         if is_buy:
#             result["block_buy_count"] += 1
#             result["block_buy_amount"] += trade_amount
#         else:
#             result["block_sell_count"] += 1
#             result["block_sell_amount"] += trade_amount
# 
#     # 집계
#     total_block = result["block_buy_amount"] + result["block_sell_amount"]
#     result["net_amount"] = result["block_buy_amount"] - result["block_sell_amount"]
# 
#     if total_block > 0:
#         result["buy_ratio"] = result["block_buy_amount"] / total_block * 100
#     else:
#         result["buy_ratio"] = 50.0
# 
#     # 방향성 판정
#     br = result["buy_ratio"]
#     if br >= 70:
#         result["direction"] = "Strong Buy"
#     elif br >= 60:
#         result["direction"] = "Buy"
#     elif br <= 30:
#         result["direction"] = "Strong Sell"
#     elif br <= 40:
#         result["direction"] = "Sell"
#     else:
#         result["direction"] = "Neutral"
# 
#     return result


def analyze_minute_data(bars: List[dict]) -> dict:
    """
    1분봉 데이터에서 수급 분석 (대신증권 호가비교 방식)

    [v18.1] threshold 폐지 → 전체 봉의 매수/매도 누적 차분 합산

    매수/매도 구분 로직:
      - cum_buy(필드11): 누적 체결매수수량 (매수주문이 매도호가에서 체결)
      - cum_sell(필드10): 누적 체결매도수량 (매도주문이 매수호가에서 체결)
      - 1분봉 간 차분으로 봉별 매수/매도 수량 계산
      - buy_delta > sell_delta → 매수우세 봉
      - sell_delta > buy_delta → 매도우세 봉

    Returns: {
        "buy_bars": int,           # 매수우세 봉 수
        "buy_amount": int,         # 매수체결 금액 합산
        "sell_bars": int,          # 매도우세 봉 수
        "sell_amount": int,        # 매도체결 금액 합산
        "net_amount": int,         # 순매수금액 (매수-매도)
        "buy_ratio": float,        # 매수비율 (%)
        "direction": str,          # 방향성 판정
        "total_bars": int,         # 전체 봉 수
    }
    """
    result = {
        "buy_bars": 0,
        "buy_amount": 0,
        "sell_bars": 0,
        "sell_amount": 0,
        "net_amount": 0,
        "buy_ratio": 0.0,
        "direction": "-",
        "total_bars": len(bars),
        # v17 호환 필드 (HTML 리포트에서 참조)
        "block_buy_count": 0,
        "block_buy_amount": 0,
        "block_sell_count": 0,
        "block_sell_amount": 0,
    }

    if len(bars) < 2:
        return result

    for i in range(1, len(bars)):
        prev = bars[i - 1]
        curr = bars[i]

        buy_delta = curr["cum_buy"] - prev["cum_buy"]
        sell_delta = curr["cum_sell"] - prev["cum_sell"]

        if buy_delta < 0:
            buy_delta = 0
        if sell_delta < 0:
            sell_delta = 0

        price = curr["price"]
        if price <= 0:
            continue

        # 봉별 매수/매도 금액 = 차분 수량 × 종가
        buy_amt = buy_delta * price
        sell_amt = sell_delta * price

        result["buy_amount"] += buy_amt
        result["sell_amount"] += sell_amt

        # 매수우세/매도우세 봉 카운팅
        if buy_delta > sell_delta:
            result["buy_bars"] += 1
        elif sell_delta > buy_delta:
            result["sell_bars"] += 1
        # buy_delta == sell_delta → 어느 쪽에도 카운트하지 않음

    # v17 호환 필드 매핑
    result["block_buy_count"] = result["buy_bars"]
    result["block_buy_amount"] = result["buy_amount"]
    result["block_sell_count"] = result["sell_bars"]
    result["block_sell_amount"] = result["sell_amount"]

    # 집계
    total_amount = result["buy_amount"] + result["sell_amount"]
    result["net_amount"] = result["buy_amount"] - result["sell_amount"]

    if total_amount > 0:
        result["buy_ratio"] = result["buy_amount"] / total_amount * 100
    else:
        result["buy_ratio"] = 50.0

    # 방향성 판정
    br = result["buy_ratio"]
    if br >= 70:
        result["direction"] = "Strong Buy"
    elif br >= 60:
        result["direction"] = "Buy"
    elif br <= 30:
        result["direction"] = "Strong Sell"
    elif br <= 40:
        result["direction"] = "Sell"
    else:
        result["direction"] = "Neutral"

    # [v18.6] ATR(7)% 계산 — 1분봉 기준, 확정봉[1] 사용 (리페인팅 방지)
    result["atr7_pct"] = _calc_atr7_pct(bars)

    return result


def _calc_atr7_pct(bars: List[dict], period: int = 7) -> float:
    """1분봉 bars에서 ATR(7)%를 계산.

    ATR = RMA(TR, 7), TR = max(H-L, |H-prevC|, |L-prevC|)
    ATR% = ATR / close * 100
    확정봉 기준 = 마지막 봉이 아닌 마지막-1 봉의 값 사용
    """
    if len(bars) < period + 2:
        return 0.0

    # True Range 시퀀스 계산
    tr_list = []
    for i in range(1, len(bars)):
        h = bars[i].get("high", bars[i]["price"])
        l = bars[i].get("low", bars[i]["price"])
        prev_c = bars[i - 1]["price"]

        if h <= 0 or l <= 0 or prev_c <= 0:
            continue

        tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
        tr_list.append(tr)

    if len(tr_list) < period:
        return 0.0

    # RMA (Wilder's smoothing) = EMA with alpha=1/period
    alpha = 1.0 / period
    rma = sum(tr_list[:period]) / period  # seed: SMA of first 'period' values
    for tr_val in tr_list[period:]:
        rma = alpha * tr_val + (1 - alpha) * rma

    # 확정봉[1] 기준: 마지막에서 2번째 봉의 종가
    ref_close = bars[-2]["price"] if len(bars) >= 2 else bars[-1]["price"]
    if ref_close <= 0:
        return 0.0

    return round(rma / ref_close * 100, 4)


# ── [v18.8] 삼각형 매수타점 계산 (점대칭 2x 기반) ──────────────────────

def _calc_triangle_target(bars: List[dict], atr7_pct: float,
                          center_lb: int = 10) -> dict:
    """삼각형 패턴 기반 매수타점 산출.

    스케일 B (∩ 기반 대파동):
      C (중심) = ∩ 왼쪽 저점 = 당일 고점 직전의 마지막 스윙 저점
      P (고점) = 당일 최고가
      X = P - C
      매수타점 1.5x = P - 1.5*X   (장 좋을 때)
      매수타점 2.0x = P - 2.0*X   (장 보통/나쁠 때)

    남은시간 = (distance_pct / atr7_pct)^2 분
    ⚠️ 수학적 주의: BM에서 E[이동시간] ∝ d²/σ² 를 차용한 휴리스틱
       ATR ≈ 0.798σ이므로 σ≠ATR (약 25% 과소추정), 드리프트/변동성군집 무시
       실전 예측이 아닌 실험적 참고 지표로 사용할 것

    Note: bars는 fetch_minute_data_daishin()이 적절히 필터링한 상태.
          - 장전 실행: 전일 영업일 1분봉
          - 장중 실행: 당일 1분봉
          삼각형 함수는 bars의 날짜를 판단하지 않고 받은 대로 계산.

    Returns:
        dict with keys: status, center, peak, x, target_2x, target_1_5x,
                        time_remain_2x, time_remain_1_5x, current_price
        status: 'OK' | 'INSUFFICIENT' | 'NO_WAVE' | 'NO_ATR'
    """
    result = {"status": "INSUFFICIENT"}

    # 최소 데이터: 고점 탐지 1봉 + 현재가 1봉 + fallback 여유 → 3봉
    if len(bars) < 3:
        result["reason"] = f"bars<3 ({len(bars)})"
        return result

    # 현재가 = 확정봉[1] 기준 (마지막에서 2번째 봉)
    current_price = bars[-2]["price"] if len(bars) >= 2 else bars[-1]["price"]
    if current_price <= 0:
        result["reason"] = "no_current_price"
        return result

    # 전체 bars에서 고점(P) 탐색
    peak_price = 0
    peak_idx = -1
    for i, b in enumerate(bars):
        h = b.get("high", b["price"])
        if h > peak_price:
            peak_price = h
            peak_idx = i

    if peak_price <= 0:
        result["reason"] = "no_peak"
        return result

    # 고점 이전 구간 확보 (최소 1봉 이상 필요)
    if peak_idx < 1:
        result["status"] = "NO_WAVE"
        result["reason"] = f"peak_at_start (idx={peak_idx})"
        return result

    before_peak = bars[:peak_idx + 1]

    # 스윙 저점(C) 탐색: center_lb 기준 좌우 비교
    # before_peak가 center_lb*2+1 미만이면 스윙 탐색 불가 → fallback으로 직행
    swing_lows = []
    if len(before_peak) >= center_lb * 2 + 1:
        for i in range(center_lb, len(before_peak) - center_lb):
            low_val = before_peak[i].get("low", before_peak[i]["price"])
            is_swing = True
            for j in range(1, center_lb + 1):
                left_low = before_peak[i - j].get("low",
                                                    before_peak[i - j]["price"])
                right_low = before_peak[i + j].get("low",
                                                     before_peak[i + j]["price"])
                if low_val >= left_low or low_val >= right_low:
                    is_swing = False
                    break
            if is_swing:
                swing_lows.append({"price": low_val, "idx": i,
                                   "time": before_peak[i]["time"]})

    if swing_lows:
        center = swing_lows[-1]  # 고점 직전의 마지막 스윙 저점
        center_price = center["price"]
        method = "swing"
    else:
        # fallback: 고점 이전 전체 최저가
        min_low = float("inf")
        for b in before_peak:
            low_val = b.get("low", b["price"])
            if low_val < min_low:
                min_low = low_val
        if min_low == float("inf"):
            result["status"] = "NO_WAVE"
            result["reason"] = "no_low_found"
            return result
        center_price = min_low
        method = "fallback_min"

    x = peak_price - center_price
    if x <= 0:
        result["status"] = "NO_WAVE"
        result["reason"] = f"x<=0 (P={peak_price} C={center_price})"
        return result

    target_2x = peak_price - int(x * 2)
    target_1_5x = peak_price - int(x * 1.5)

    result["status"] = "OK"
    result["center"] = center_price
    result["peak"] = peak_price
    result["x"] = x
    result["target_2x"] = target_2x
    result["target_1_5x"] = target_1_5x
    result["current_price"] = current_price
    result["method"] = method

    # 남은시간 계산 (브라운 운동 차원 분석 기반 휴리스틱)
    # ⚠️ 수학적 주의: BM에서 거리 d의 이동 기대시간 ∝ d²/σ²을 차용한 식
    #   - ATR ≠ σ: BM이면 ATR ≈ 0.798σ이므로 σ ≈ 1.253×ATR (약 25% 보정 없음)
    #   - 드리프트·변동성 군집·호가 단위·장중 계절성 무시
    #   - ATR이 매우 작으면 시간 값이 폭발적으로 커짐
    #   → 물리적 차원 구조(%/% → 무차원 제곱)는 맞지만 실전 정밀도는 낮음
    #   → 실험용 참고 지표로만 사용할 것
    if atr7_pct > 0 and current_price > 0:
        # 2x 타점까지
        dist_2x_pct = (current_price - target_2x) / current_price * 100
        if dist_2x_pct <= 0:
            result["time_remain_2x"] = 0.0  # 이미 도달
        else:
            result["time_remain_2x"] = round((dist_2x_pct / atr7_pct) ** 2, 1)

        # 1.5x 타점까지
        dist_1_5x_pct = (current_price - target_1_5x) / current_price * 100
        if dist_1_5x_pct <= 0:
            result["time_remain_1_5x"] = 0.0
        else:
            result["time_remain_1_5x"] = round((dist_1_5x_pct / atr7_pct) ** 2, 1)
    else:
        # ATR 없어도 가격 정보는 유지, time만 None
        result["time_remain_2x"] = None
        result["time_remain_1_5x"] = None

    return result


# ============================================================================
#  [v18.8] 매수타점 사후 검증 + 누적 통계 분석 (Stage 2 + 3)
# ============================================================================
#
# 동작:
#   - 매 run_once 종료 시 자동으로 호출됨
#   - cache/triangle_snapshots_*.jsonl 중 아직 검증 안 된 날짜를 찾아 검증
#   - 검증 결과를 cache/verified_*.csv 에 저장
#   - 새로 검증된 게 있으면 누적 통계를 콘솔에 출력
#   - 이미 처리된 날짜는 자동 스킵 → 매번 돌려도 부담 없음
#
# 검증 기준:
#   - 도달: 1분봉 저가가 타점 이하로 1번이라도 닿으면 적중
#   - 반등: 도달 직후 봉부터 5/10/20/30/60/120/240분 후 종가 비교
#   - 진입가: 정확히 타점 가격 (slippage 0)

REBOUND_OFFSETS = [5, 10, 20, 30, 60, 120, 240]


def _find_first_reach(future_bars: List[dict], target: int) -> Optional[int]:
    """미래 봉 중 1분봉 저가가 target 이하로 닿는 첫 인덱스(0-base)."""
    if target is None or target <= 0:
        return None
    for i, b in enumerate(future_bars):
        low = b.get("low", b.get("price", 0))
        if low > 0 and low <= target:
            return i
    return None


def _calc_rebounds(future_bars: List[dict], reach_idx: int,
                   entry_price: int, suffix: str = "") -> Dict[str, Optional[float]]:
    """도달 직후 봉부터 N분 후 종가 기준 반등률(%)."""
    rebounds = {}
    for offset in REBOUND_OFFSETS:
        future_idx = reach_idx + offset
        key = f"rebound_{offset}m{suffix}"
        if future_idx < len(future_bars) and entry_price > 0:
            close_after = future_bars[future_idx].get("price", 0)
            if close_after > 0:
                rebounds[key] = round(
                    (close_after / entry_price - 1) * 100, 3
                )
            else:
                rebounds[key] = None
        else:
            rebounds[key] = None
    return rebounds


def _verify_triangle_day(date_str: str, cache_dir: str) -> List[dict]:
    """단일 거래일 매수타점 사후 검증."""
    snapshot_file = os.path.join(cache_dir, f"triangle_snapshots_{date_str}.jsonl")
    tick_cache_file = os.path.join(cache_dir, f"tick_cache_daishin_{date_str}.pkl")

    if not os.path.exists(snapshot_file):
        return []
    if not os.path.exists(tick_cache_file):
        log.warning(f"[VERIFY] {date_str} tick_cache 없음 (검증 불가)")
        return []

    # 스냅샷 로드
    snapshots = []
    try:
        with open(snapshot_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    snapshots.append(json.loads(line))
    except Exception as e:
        log.warning(f"[VERIFY] {date_str} 스냅샷 로드 실패: {e}")
        return []

    if not snapshots:
        return []

    # 1분봉 캐시 로드
    try:
        with open(tick_cache_file, 'rb') as f:
            tick_cache = pickle.load(f)
    except Exception as e:
        log.warning(f"[VERIFY] {date_str} tick_cache 로드 실패: {e}")
        return []

    results = []
    n_hit_2x = 0
    n_hit_15x = 0

    for snap in snapshots:
        code = snap.get("code", "")
        snap_time = snap.get("snap_time", "")
        bars = tick_cache.get(code, {}).get("bars", [])
        if not bars:
            continue

        future_bars = [b for b in bars if b.get("time", "") > snap_time]
        if not future_bars:
            continue

        t2x = snap.get("target_2x")
        t15x = snap.get("target_1_5x")

        reach_2x = _find_first_reach(future_bars, t2x)
        reach_15x = _find_first_reach(future_bars, t15x)

        if reach_2x is not None:
            rebounds_2x = _calc_rebounds(future_bars, reach_2x, t2x, "")
            n_hit_2x += 1
        else:
            rebounds_2x = {f"rebound_{o}m": None for o in REBOUND_OFFSETS}

        if reach_15x is not None:
            rebounds_15x = _calc_rebounds(future_bars, reach_15x, t15x, "_15x")
            n_hit_15x += 1
        else:
            rebounds_15x = {f"rebound_{o}m_15x": None for o in REBOUND_OFFSETS}

        pred_2x = snap.get("time_remain_2x")
        actual_2x_min = (reach_2x + 1) if reach_2x is not None else None
        time_err_2x = (actual_2x_min - pred_2x
                       if actual_2x_min is not None and pred_2x is not None
                       else None)

        pred_15x = snap.get("time_remain_1_5x")
        actual_15x_min = (reach_15x + 1) if reach_15x is not None else None
        time_err_15x = (actual_15x_min - pred_15x
                        if actual_15x_min is not None and pred_15x is not None
                        else None)

        record = {
            "date": date_str,
            **snap,
            "hit_2x": reach_2x is not None,
            "hit_1_5x": reach_15x is not None,
            "actual_min_to_2x": actual_2x_min,
            "actual_min_to_1_5x": actual_15x_min,
            "time_err_2x": time_err_2x,
            "time_err_1_5x": time_err_15x,
            **rebounds_2x,
            **rebounds_15x,
        }
        results.append(record)

    log.info(f"[VERIFY] {date_str}: {len(results)}건 검증 | "
             f"2x적중={n_hit_2x} ({n_hit_2x/max(len(results),1)*100:.1f}%) | "
             f"1.5x적중={n_hit_15x} ({n_hit_15x/max(len(results),1)*100:.1f}%)")

    return results


def _save_verified_csv(results: List[dict], output_path: str):
    """검증 결과를 CSV로 저장."""
    if not results:
        return
    import csv as _csv
    all_keys = []
    seen = set()
    for r in results:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                all_keys.append(k)
    with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
        w = _csv.DictWriter(f, fieldnames=all_keys, extrasaction='ignore')
        w.writeheader()
        w.writerows(results)


def _auto_verify_pending(cache_dir: str) -> int:
    """검증 대기 중인 모든 날짜를 찾아 자동 검증.

    반환: 새로 검증된 날짜 수
    """
    if not os.path.exists(cache_dir):
        return 0

    # 스냅샷 있는데 verified 없는 날짜 찾기
    pending = []
    for fname in os.listdir(cache_dir):
        if fname.startswith("triangle_snapshots_") and fname.endswith(".jsonl"):
            date_str = fname[len("triangle_snapshots_"):-len(".jsonl")]
            if not (len(date_str) == 8 and date_str.isdigit()):
                continue
            verified_path = os.path.join(cache_dir, f"verified_{date_str}.csv")
            if not os.path.exists(verified_path):
                # 단, 오늘 날짜는 아직 장이 진행 중일 수 있으니 스킵
                today_str = datetime.now().strftime("%Y%m%d")
                if date_str < today_str:
                    pending.append(date_str)
                elif date_str == today_str and not _is_market_hours_static():
                    # 오늘이지만 장 종료 후면 검증 가능
                    pending.append(date_str)

    if not pending:
        return 0

    pending.sort()
    log.info(f"[VERIFY] 검증 대기 {len(pending)}일: {pending}")

    n_verified = 0
    for date_str in pending:
        results = _verify_triangle_day(date_str, cache_dir)
        if results:
            output_path = os.path.join(cache_dir, f"verified_{date_str}.csv")
            _save_verified_csv(results, output_path)
            log.info(f"[VERIFY] saved: {output_path}")
            n_verified += 1

    return n_verified


def _is_market_hours_static() -> bool:
    """현재 시각이 장중(09:00~15:40)인지 (run_once 외부에서 사용)."""
    now = datetime.now()
    if now.hour < 9:
        return False
    if now.hour > 15 or (now.hour == 15 and now.minute >= 40):
        return False
    return True


def _print_triangle_stats(cache_dir: str):
    """모든 verified_*.csv를 합쳐서 누적 통계를 콘솔에 출력."""
    import csv as _csv

    if not os.path.exists(cache_dir):
        return

    files = sorted([f for f in os.listdir(cache_dir)
                    if f.startswith("verified_") and f.endswith(".csv")])
    if not files:
        return

    all_records = []
    for fname in files:
        path = os.path.join(cache_dir, fname)
        try:
            with open(path, 'r', encoding='utf-8-sig') as f:
                for row in _csv.DictReader(f):
                    all_records.append(row)
        except Exception:
            continue

    if not all_records:
        return

    def _safe_float(v):
        if v is None or v == "" or v == "None":
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    def _safe_bool(v):
        return str(v).lower() in ("true", "1", "yes")

    n = len(all_records)
    n_hit_2x = sum(1 for r in all_records if _safe_bool(r.get('hit_2x')))
    n_hit_15x = sum(1 for r in all_records if _safe_bool(r.get('hit_1_5x')))

    log.info("=" * 78)
    log.info(f"[STATS] 매수타점 누적 통계 ({len(files)}일치, 총 {n}건)")
    log.info("=" * 78)
    log.info(f"  1.5x 적중: {n_hit_15x} ({n_hit_15x/n*100:.1f}%)")
    log.info(f"  2.0x 적중: {n_hit_2x} ({n_hit_2x/n*100:.1f}%)")

    # ── NO_WAVE 비율 (summary jsonl 로드) ──
    summary_files = sorted([f for f in os.listdir(cache_dir)
                            if f.startswith("triangle_summary_")
                            and f.endswith(".jsonl")])
    if summary_files:
        sum_total = 0
        sum_ok = 0
        sum_nowave = 0
        sum_insuf = 0
        for sf in summary_files:
            try:
                with open(os.path.join(cache_dir, sf), 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        rec = json.loads(line)
                        sum_total += rec.get("n_total", 0)
                        sum_ok += rec.get("n_ok", 0)
                        sum_nowave += rec.get("n_nowave", 0)
                        sum_insuf += rec.get("n_insuf", 0)
            except Exception:
                continue
        if sum_total > 0:
            log.info(f"  패턴 형성률: OK={sum_ok/sum_total*100:.1f}% "
                     f"NO_WAVE={sum_nowave/sum_total*100:.1f}% "
                     f"INSUF={sum_insuf/sum_total*100:.1f}% "
                     f"(누적 {sum_total:,}건)")

    # ── 시간대별 반등 (2x와 1.5x 나란히) ──
    log.info("")
    log.info("  적중 후 시간대별 평균 반등률 (양수=상승)")
    log.info(f"  {'시점':>5} | {'2x평균':>9} {'2x>0%':>7} {'2x표본':>7} | "
             f"{'1.5x평균':>10} {'1.5x>0%':>8} {'1.5x표본':>9}")
    log.info("  " + "-" * 74)
    for o in REBOUND_OFFSETS:
        # 2x
        col2 = f"rebound_{o}m"
        vals2 = [_safe_float(r.get(col2)) for r in all_records
                 if _safe_bool(r.get('hit_2x'))]
        vals2 = [v for v in vals2 if v is not None]
        if vals2:
            avg2 = sum(vals2) / len(vals2)
            pos2 = sum(1 for v in vals2 if v > 0) / len(vals2) * 100
            avg2_s = f"{avg2:+.3f}%"
            pos2_s = f"{pos2:.1f}%"
            n2_s = f"{len(vals2)}"
        else:
            avg2_s, pos2_s, n2_s = "-", "-", "0"

        # 1.5x
        col15 = f"rebound_{o}m_15x"
        vals15 = [_safe_float(r.get(col15)) for r in all_records
                  if _safe_bool(r.get('hit_1_5x'))]
        vals15 = [v for v in vals15 if v is not None]
        if vals15:
            avg15 = sum(vals15) / len(vals15)
            pos15 = sum(1 for v in vals15 if v > 0) / len(vals15) * 100
            avg15_s = f"{avg15:+.3f}%"
            pos15_s = f"{pos15:.1f}%"
            n15_s = f"{len(vals15)}"
        else:
            avg15_s, pos15_s, n15_s = "-", "-", "0"

        log.info(f"  {o:>3}분 | {avg2_s:>9} {pos2_s:>7} {n2_s:>7} | "
                 f"{avg15_s:>10} {pos15_s:>8} {n15_s:>9}")

    # ── 시간 예측 정확도 ──
    log.info("")
    err_vals = [_safe_float(r.get('time_err_2x')) for r in all_records]
    err_vals = [v for v in err_vals if v is not None]
    if err_vals:
        avg_err = sum(err_vals) / len(err_vals)
        avg_abs_err = sum(abs(v) for v in err_vals) / len(err_vals)
        log.info(f"  시간 예측 오차 (2x): 평균 {avg_err:+.1f}분, "
                 f"|평균| {avg_abs_err:.1f}분 (n={len(err_vals)})")
        log.info(f"    음수=실제가 더 빨리 도달 / 양수=이론보다 늦게")

    # ── 매수비율 구간별 통계 ──
    log.info("")
    log.info("  매수비율 구간별 적중률·30분반등 (수급 강도와의 관계)")
    log.info(f"  {'구간':>10} | {'표본':>7} | {'2x적중':>8} | {'1.5x적중':>10} | "
             f"{'2x 30분반등':>14}")
    log.info("  " + "-" * 64)
    br_bins = [(0, 30), (30, 40), (40, 50), (50, 60), (60, 70), (70, 100)]
    for lo, hi in br_bins:
        bucket = []
        for r in all_records:
            br = _safe_float(r.get("buy_ratio"))
            if br is None:
                continue
            if lo <= br < hi or (hi == 100 and br <= 100 and br >= lo):
                bucket.append(r)
        if not bucket:
            continue
        n_b = len(bucket)
        h2 = sum(1 for r in bucket if _safe_bool(r.get('hit_2x')))
        h15 = sum(1 for r in bucket if _safe_bool(r.get('hit_1_5x')))
        rebs = [_safe_float(r.get('rebound_30m')) for r in bucket
                if _safe_bool(r.get('hit_2x'))]
        rebs = [v for v in rebs if v is not None]
        avg_reb = sum(rebs) / len(rebs) if rebs else None
        avg_reb_s = f"{avg_reb:+.3f}%" if avg_reb is not None else "-"
        log.info(f"  {lo:>3}~{hi:<3}% | {n_b:>7} | "
                 f"{h2/n_b*100:>6.1f}% | "
                 f"{h15/n_b*100:>8.1f}% | "
                 f"{avg_reb_s:>14}")

    log.info("=" * 78)


# ============================================================================
#  종목 분류 + 포맷 헬퍼
# ============================================================================

def classify_stock_type(code: str, name: str) -> str:
    """
    종목 유형 분류: STOCK / ETF / ETN / ELW

    [v18.2] _SECTION_KIND_CACHE 참조 (get_all_stock_codes에서 이미 조회됨)
    - 캐시 miss 시 COM fallback → 종목명 fallback
    """
    code_a = ensure_a_prefix(code)

    # 1차: 캐시 참조 (COM 재호출 없음)
    kind = _SECTION_KIND_CACHE.get(code_a)
    if kind is not None:
        if kind in (10, 12):
            return "ETF"
        if kind == 17:
            return "ETN"
        if kind == 9:
            return "ELW"
        return "STOCK"

    # 2차: COM 직접 조회 (캐시 miss — 거의 없음)
    try:
        code_mgr = win32com.client.Dispatch("CpUtil.CpCodeMgr")
        kind = code_mgr.GetStockSectionKind(code_a)
        if kind in (10, 12):
            return "ETF"
        if kind == 17:
            return "ETN"
        if kind == 9:
            return "ELW"
        return "STOCK"
    except Exception:
        pass

    # 3차: 종목명 fallback
    name_upper = name.upper()
    if "ETN" in name_upper:
        return "ETN"
    if "ETF" in name_upper:
        return "ETF"
    etf_brands = ["KODEX", "TIGER", "KOSEF", "KBSTAR", "HANARO",
                  "SOL ", "ACE ", "ARIRANG", "TIMEFOLIO", "RISE "]
    for kw in etf_brands:
        if kw in name or kw in name_upper:
            return "ETF"
    return "STOCK"


def format_amount(amount: int) -> str:
    """금액 포맷 (1억 → '1억', 15000만 → '1.5억')"""
    if abs(amount) >= 1_0000_0000:
        return f"{amount / 1_0000_0000:,.0f}억"
    elif abs(amount) >= 1_0000:
        return f"{amount / 1_0000:,.0f}만"
    else:
        return f"{amount:,}"


def fmt_억(amount: int) -> str:
    """금액을 억 단위로 표시 (HTML 테이블용)"""
    if amount == 0:
        return "0"
    sign = "" if amount >= 0 else "-"
    abs_amt = abs(amount)
    if abs_amt >= 1_0000_0000:
        val = abs_amt / 1_0000_0000
        if val >= 10:
            return f"{sign}{val:,.0f}"
        else:
            return f"{sign}{val:,.1f}"
    elif abs_amt >= 1000_0000:
        return f"{sign}{abs_amt / 1_0000_0000:.2f}"
    else:
        return f"{sign}{abs_amt / 1_0000_0000:.3f}"


def dir_kr(direction: str) -> str:
    """방향성 영→한 변환"""
    _map = {
        "Strong Buy": "강한매수",
        "Buy": "매수우위",
        "Neutral": "균형",
        "Sell": "매도우위",
        "Strong Sell": "강한매도",
        "-": "-",
    }
    return _map.get(direction, direction)



# ============================================================================
#  LLM 테마 분류 (GPT-4o-mini)
# ============================================================================


# 테마명 정규화 맵 (세분류 → 대분류)
# LLM이 세분류로 응답하더라도 대분류로 통일
_THEME_NORMALIZE_MAP = {
    # ── 반도체 ──
    "반도체 장비": "반도체",
    "반도체 재료": "반도체",
    "반도체 재료/부품": "반도체",
    "반도체 소재": "반도체",
    "반도체 소부장": "반도체",
    "반도체 패키징": "반도체",
    "반도체 설계": "반도체",
    "반도체 테스트": "반도체",
    "반도체 공정": "반도체",
    "반도체 부품": "반도체",
    "시스템반도체": "반도체",
    "메모리반도체": "반도체",
    "파운드리": "반도체",
    "HBM": "반도체",
    # ── AI ──
    "AI 반도체": "AI",
    "AI 소프트웨어": "AI",
    "AI 서비스": "AI",
    "AI 플랫폼": "AI",
    "AI 데이터": "AI",
    "AI 에이전트": "AI",
    "생성형 AI": "AI",
    "인공지능": "AI",
    # ── 바이오 ──
    "바이오 신약": "바이오",
    "바이오시밀러": "바이오",
    "바이오 의약품": "바이오",
    "바이오 진단": "바이오",
    "바이오 CMO": "바이오",
    "바이오 CDMO": "바이오",
    "제약/바이오": "바이오",
    "셀트리온": "바이오",
    "신약개발": "바이오",
    "항암제": "바이오",
    "줄기세포": "바이오",
    "유전자 치료": "바이오",
    "진단키트": "바이오",
    # ── 2차전지 ──
    "2차전지 소재": "2차전지",
    "2차전지 장비": "2차전지",
    "2차전지 부품": "2차전지",
    "배터리 소재": "2차전지",
    "배터리 장비": "2차전지",
    "배터리 부품": "2차전지",
    "리튬": "2차전지",
    "양극재": "2차전지",
    "음극재": "2차전지",
    "전해질": "2차전지",
    "분리막": "2차전지",
    "전고체 배터리": "2차전지",
    "전고체": "2차전지",
    "ESS": "2차전지",
    # ── 전기차 ──
    "전기차 충전": "전기차",
    "전기차 부품": "전기차",
    "전기차 소재": "전기차",
    "EV 충전": "전기차",
    "자율주행": "전기차",
    # ── 로봇 ──
    "로봇 부품": "로봇",
    "로봇 소프트웨어": "로봇",
    "산업용 로봇": "로봇",
    "서비스 로봇": "로봇",
    "협동로봇": "로봇",
    "휴머노이드": "로봇",
    "로봇 자동화": "로봇",
    # ── 방산 ──
    "방산 부품": "방산",
    "방산 장비": "방산",
    "방위산업": "방산",
    "군수": "방산",
    "K-방산": "방산",
    # ── 조선 ──
    "조선 기자재": "조선",
    "조선 부품": "조선",
    "선박 부품": "조선",
    "LNG선": "조선",
    "해양플랜트": "조선",
    # ── 원전 ──
    "원전 부품": "원전",
    "원전 장비": "원전",
    "원전 소재": "원전",
    "원자력 발전": "원전",
    "SMR": "원전",
    "소형모듈원자로": "원전",
    # ── 우주항공 ──
    "우주항공 부품": "우주항공",
    "항공우주": "우주항공",
    "위성": "우주항공",
    "위성통신": "우주항공",
    "우주 발사체": "우주항공",
    "UAM": "우주항공",
    "드론": "우주항공",
    # ── 양자컴퓨터 ──
    "양자 기술": "양자컴퓨터",
    "양자통신": "양자컴퓨터",
    "양자암호": "양자컴퓨터",
    # ── 클라우드 ──
    "클라우드 서비스": "클라우드",
    "클라우드 컴퓨팅": "클라우드",
    "클라우드 인프라": "클라우드",
    "데이터센터": "클라우드",
    "IDC": "클라우드",
    # ── 화장품 ──
    "K-뷰티": "화장품",
    "뷰티": "화장품",
    "화장품 ODM": "화장품",
    "화장품 OEM": "화장품",
    "스킨케어": "화장품",
    # ── 엔터/미디어 ──
    "K-팝": "엔터",
    "엔터테인먼트": "엔터",
    "K-콘텐츠": "엔터",
    "게임 개발": "게임",
    "모바일 게임": "게임",
    "온라인 게임": "게임",
    # ── 금융 ──
    "핀테크": "금융",
    "가상자산": "금융",
    "블록체인": "금융",
    "증권": "금융",
    "보험": "금융",
    "은행": "금융",
    # ── 식품 ──
    "식품 가공": "식품",
    "음식료": "식품",
    "주류": "식품",
    "건강기능식품": "식품",
    # ── 건설 ──
    "건설 자재": "건설",
    "시멘트": "건설",
    "건축 소재": "건설",
    "인테리어": "건설",
    # ── 태양광/풍력 → 신재생 ──
    "태양광": "신재생",
    "풍력": "신재생",
    "태양광 부품": "신재생",
    "풍력 부품": "신재생",
    "신재생에너지": "신재생",
    "수소에너지": "수소",
    "수소 연료전지": "수소",
    "수소 저장": "수소",
    # ── 기타 ──
    "전력 설비": "전력",
    "전력 반도체": "전력",
    "전력기기": "전력",
    "초전도체": "전력",
    "자동차 부품": "자동차",
    "자동차 소재": "자동차",
    "통신 장비": "통신",
    "통신 서비스": "통신",
    "5G": "통신",
    "6G": "통신",
    "보안 솔루션": "보안",
    "사이버 보안": "보안",
    "정보보안": "보안",
    "철강 주요종목": "철강",
    "철강 중소형": "철강",
    "디스플레이 장비": "디스플레이",
    "디스플레이 소재": "디스플레이",
    "OLED 소재": "디스플레이",
    "OLED 장비": "디스플레이",
    "OLED": "디스플레이",
    "플렉서블 디스플레이": "디스플레이",
    # ── 밸류업 (괄호·지수명 자동 제거) ──
    "밸류업": "밸류업",
    "코리아 밸류업 지수": "밸류업",
    # ── 2차전지 추가 ──
    "고체산화물 연료전지": "2차전지",
    "SOFC": "2차전지",
    "연료전지": "2차전지",
    "2차전지(나트륨이온)": "2차전지",
    "2차전지(소재/부품)": "2차전지",
    "2차전지(장비)": "2차전지",
    "2차전지(생산)": "2차전지",
    "폐배터리": "2차전지",
    # ── 석유화학 계열 ──
    "LNG": "석유화학",
    "LPG": "석유화학",
    "정유": "석유화학",
    "셰일가스": "석유화학",
    "윤활유": "석유화학",
    # ── 자동차 계열 ──
    "전기차": "자동차",
    "자동차부품": "자동차",
    "전기차(충전소/충전기)": "자동차",
    "스마트카": "자동차",
    "타이어": "자동차",
    "전기자전거": "자동차",
    "리비안": "자동차",
    # ── 반도체 계열 (스마트폰 하드웨어 포함) ──
    "CXL": "반도체",
    "MLCC": "반도체",
    "뉴로모픽 반도체": "반도체",
    "PCB": "반도체",
    "카메라모듈": "반도체",
    "갤럭시 부품주": "반도체",
    "아이폰": "반도체",
    "폴더블폰": "반도체",
    "삼성페이": "반도체",
    "애플페이": "반도체",
    "무선충전기술": "반도체",
    "모바일솔루션": "반도체",
    "스마트폰": "반도체",
    # ── 원자력 (독립 카테고리) ──
    "원자력발전소 해체": "원자력",
    "원자력발전": "원자력",
    "원전": "원자력",
    # ── 전력 계열 ──
    "전력설비": "전력",
    "스마트그리드": "전력",
    "핵융합에너지": "전력",
    # ── 디스플레이 추가 ──
    "마이크로 LED": "디스플레이",
    # ── AI 계열 ──
    "온디바이스 AI": "AI",
    "의료AI": "AI",
    "음성인식": "AI",
    # ── IT 계열 ──
    "클라우드": "IT",
    "SI": "IT",
    "스마트팩토리": "IT",
    "3D 프린터": "IT",
    "IT/SW": "IT",
    "키오스크": "IT",
    "재택근무": "IT",
    "마이데이터": "IT",
    # ── 통신 추가 ──
    "통신장비": "통신",
    # ── 보안 추가 ──
    "보안주": "보안",
    "CCTV": "보안",
    # ── 바이오 계열 ──
    "제약업체": "바이오",
    "의료기기": "바이오",
    "코로나19": "바이오",
    "치매": "바이오",
    "비만치료제": "바이오",
    "mRNA": "바이오",
    "보톡스": "바이오",
    "백신": "바이오",
    "모더나": "바이오",
    "탈모 치료": "바이오",
    "마이크로바이옴": "바이오",
    "제대혈": "바이오",
    "원격진료": "바이오",
    "비대면진료": "바이오",
    "마리화나": "바이오",
    "치아 치료": "바이오",
    "낙태": "바이오",
    "마스크": "바이오",
    # ── 화장품 추가 ──
    "미용기기": "화장품",
    # ── 방산 추가 ──
    "재난/안전": "방산",
    # ── 우주항공 (독립 카테고리) ──
    "우주항공": "우주항공",
    "항공기부품": "우주항공",
    "스페이스X": "우주항공",
    # ── 게임/엔터 (모바일게임·콘텐츠 분리 - 정확매칭 우선) ──
    "모바일게임(스마트폰)": "게임",
    "모바일콘텐츠(스마트폰/태블릿PC)": "엔터",
    "모바일게임": "게임",
    "모바일콘텐츠": "엔터",
    "음원/음반": "엔터",
    "영상콘텐츠": "엔터",
    "캐릭터상품": "엔터",
    "웹툰": "엔터",
    # ── 금융 계열 ──
    "카카오뱅크": "금융",
    "토스": "금융",
    "전자결제": "금융",
    "화폐/금융자동화기기": "금융",
    "가상화폐": "금융",
    "두나무": "금융",
    "창투사": "금융",
    # ── 지주사 추가 ──
    "지주": "지주사",
    # ── 남북경협 계열 ──
    "DMZ 평화공원": "남북경협",
    # ── 건설 계열 ──
    "건설기계": "건설",
    "모듈러주택": "건설",
    "아스콘": "건설",
    "해저터널": "건설",
    "서울고속버스터미널": "건설",
    # ── 철도 추가 ──
    "GTX": "철도",
    # ── 조선 추가 ──
    "조선기자재": "조선",
    # ── 유통 (신규 카테고리) ──
    "소매유통": "유통",
    "편의점": "유통",
    "백화점": "유통",
    "홈쇼핑": "유통",
    "면세점": "유통",
    "쿠팡": "유통",
    "마켓컬리": "유통",
    "콜드체인": "유통",
    # ── 관광/레저 (신규 카테고리) ──
    "호텔/리조트": "관광/레저",
    "카지노": "관광/레저",
    "여행": "관광/레저",
    "야놀자": "관광/레저",
    "항공/저가 항공사": "관광/레저",
    "테마파크": "관광/레저",
    # ── 패션/의류 ──
    "패션/의류": "패션/의류",
    # ── 소재 (신규 카테고리) ──
    "희귀금속": "소재",
    "비철금속": "소재",
    "그래핀": "소재",
    "탄소나노튜브": "소재",
    "페라이트": "소재",
    # ── 환경 (신규 카테고리) ──
    "수자원": "환경",
    "황사/미세먼지": "환경",
    "공기청정기": "환경",
    "폐기물처리": "환경",
    "온실가스": "환경",
    # ── 메타버스 추가 ──
    "메타버스": "메타버스",
    "증강현실": "메타버스",
    "가상현실": "메타버스",
    "바이오인식": "메타버스",
    # ── 자원개발 ──
    "자원개발": "자원개발",
}

# 접미어 패턴: "XX 소재/장비/부품/서비스" → "XX"로 자동 축약
_THEME_SUFFIX_STRIP = [
    " 소재", " 장비", " 부품", " 서비스", " 솔루션",
    " 소부장", " 기자재", " 인프라", " 플랫폼",
    " 관련주", " 관련", " 대장주", " 수혜주", " 테마",
]


# 대분류 키워드 집합 (접두어 매칭용)
_THEME_PARENT_KEYWORDS = {
    "반도체", "AI", "바이오", "2차전지", "전기차", "로봇", "방산", "조선",
    "원전", "우주항공", "양자컴퓨터", "클라우드", "화장품", "엔터", "게임",
    "금융", "식품", "건설", "신재생", "수소", "전력", "자동차", "통신",
    "보안", "디스플레이", "의료기기",
}


def normalize_theme(theme: str) -> str:
    """테마명 세분류 → 대분류 정규화"""
    if not theme:
        return theme
    # 1) 정확 매칭
    if theme in _THEME_NORMALIZE_MAP:
        return _THEME_NORMALIZE_MAP[theme]
    # 2) 부분 매칭 (맵의 key가 theme 안에 포함)
    for sub, parent in _THEME_NORMALIZE_MAP.items():
        if sub in theme:
            return parent
    # 3) 접미어 자동 제거 후 재귀
    for suffix in _THEME_SUFFIX_STRIP:
        if theme.endswith(suffix):
            stripped = theme[:-len(suffix)].strip()
            if stripped and stripped != theme:
                return normalize_theme(stripped)  # 재귀: "반도체 테스트" → 다시 정규화
    # 4) 접두어 매칭: 첫 단어가 대분류 키워드이면 그것으로 통일
    #    예: "반도체 테스트" → "반도체", "바이오 CDMO 위탁" → "바이오"
    first_word = theme.split()[0] if " " in theme else ""
    if first_word in _THEME_PARENT_KEYWORDS:
        return first_word
    return theme


def classify_themes_via_llm(missing_stocks: List[dict],
                            existing_themes: List[str]) -> Dict[str, str]:
    """
    OpenAI GPT-4o-mini API를 사용하여 미분류 종목들의 시장 테마를 분류.
    환경변수: api11.txt (스크립트 폴더) 또는 OPENAI_API_KEY 환경변수

    [v18.1 수정] 50종목씩 배치 분할 + max_tokens 4096
    - 이전: 전체 종목을 한번에 → max_tokens 1024로 응답 잘림 → 70%+ 미분류
    - 수정: 50개 배치 × 여러 호출 → 안정적 분류
    """
    api_key = ""
    _api_file = os.path.join(SCRIPT_DIR, "api11.txt")
    if os.path.exists(_api_file):
        try:
            raw = open(_api_file, "r", encoding="utf-8-sig").read()
            api_key = raw.strip().split("\n")[0].strip()
            if api_key:
                log.info(f"[LLM] API key loaded from api11.txt "
                         f"(len={len(api_key)}, starts={api_key[:12]}...)")
        except Exception as e:
            log.warning(f"[LLM] api11.txt read error: {e}")
    if not api_key:
        api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        log.warning("[LLM] API key not found -> LLM classification skipped")
        log.warning("[LLM]   Create api11.txt in script folder (key only, one line)")
        return {}

    theme_hint = ""
    if existing_themes:
        unique_themes = sorted(set(existing_themes))[:30]
        theme_hint = f"\n\nExisting themes: {', '.join(unique_themes)}"

    # 50종목씩 배치 분할
    BATCH_SIZE = 50
    all_results = {}
    total_in_tok = 0
    total_out_tok = 0

    for batch_idx in range(0, len(missing_stocks), BATCH_SIZE):
        batch = missing_stocks[batch_idx:batch_idx + BATCH_SIZE]
        batch_num = batch_idx // BATCH_SIZE + 1
        total_batches = (len(missing_stocks) + BATCH_SIZE - 1) // BATCH_SIZE

        stock_lines = []
        for s in batch:
            chg = s.get("change_rate", 0)
            chg_str = f"+{chg:.1f}%" if chg > 0 else f"{chg:.1f}%"
            stock_lines.append(f"- {s['code']} {s['name']} ({chg_str})")
        stock_list_text = "\n".join(stock_lines)

        user_prompt = f"""한국 주식시장 대량체결 분석 리포트에서 아래 종목들의 **시장 테마**를 분류해주세요.

## 규칙
1. 테마명은 한국 주식시장에서 통용되는 시장 테마로 작성 (예: "로봇", "반도체", "양자컴퓨터", "우주항공", "방산", "2차전지", "AI", "바이오", "원전", "조선", "이재명", "경영권분쟁", "통신", "금융", "자동차", "화장품" 등)
2. **업종명이 아닌 테마명** 으로 분류 (예: "전기전자" X -> "반도체" O)
3. **세분류 금지, 대분류 테마만 사용**: "반도체 장비" X → "반도체" O, "반도체 재료/부품" X → "반도체" O, "바이오 신약" X → "바이오" O, "2차전지 소재" X → "2차전지" O, "AI 반도체" X → "AI" O
4. 대기업/유명기업은 반드시 해당 업종 테마로 분류 (예: KT->통신, 삼성전자->반도체, 현대차->자동차, LG에너지솔루션->2차전지)
5. 최근 경영권분쟁/적대적M&A 이슈 종목은 "경영권분쟁" 테마 적용
6. 하나의 종목에 테마 1개만 배정
7. 정말로 어떤 테마에도 해당하지 않는 경우만 "-" 사용 (가능한 한 "-"를 최소화)
8. 같은 테마에 속하는 종목들은 동일한 테마명 사용
{theme_hint}

## 분류할 종목
{stock_list_text}

## 응답 형식
반드시 JSON만 응답하세요. 설명이나 마크다운 없이 순수 JSON만:
{{"종목코드": "테마명", "종목코드": "테마명"}}"""

        try:
            log.info(f"[LLM] Batch {batch_num}/{total_batches}: "
                     f"{len(batch)} stocks...")

            resp = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "gpt-4o-mini",
                    "max_tokens": 4096,
                    "temperature": 0.2,
                    "messages": [
                        {
                            "role": "system",
                            "content": "You are a Korean stock market theme classifier. "
                                       "Respond ONLY with valid JSON. No explanation, no markdown. "
                                       "Classify EVERY stock - minimize unclassified ('-') results."
                        },
                        {"role": "user", "content": user_prompt},
                    ],
                },
                timeout=60,
            )

            if resp.status_code != 200:
                err_text = resp.text[:400]
                log.warning(f"[LLM] Batch {batch_num} API error "
                            f"{resp.status_code}: {err_text}")
                if resp.status_code == 401:
                    log.warning("[LLM] -> API key invalid/expired. Stopping.")
                    break
                elif resp.status_code == 429:
                    log.warning("[LLM] -> Rate limit. Waiting 10s...")
                    time.sleep(10)
                continue

            data = resp.json()
            choices = data.get("choices", [])
            if not choices:
                log.warning(f"[LLM] Batch {batch_num}: empty choices")
                continue
            text = choices[0].get("message", {}).get("content", "")

            text = text.strip()
            if text.startswith("```"):
                text = re.sub(r'^```(?:json)?\s*', '', text)
                text = re.sub(r'\s*```$', '', text)

            llm_result = json.loads(text)
            if not isinstance(llm_result, dict):
                log.warning(f"[LLM] Batch {batch_num}: "
                            f"unexpected type {type(llm_result)}")
                continue

            valid_codes = {s["code"] for s in batch}
            batch_cleaned = 0
            for code, theme in llm_result.items():
                code = str(code).strip().lstrip("A").zfill(6)
                if code in valid_codes and isinstance(theme, str) and theme.strip():
                    all_results[code] = normalize_theme(theme.strip())
                    batch_cleaned += 1

            log.info(f"[LLM] Batch {batch_num}: "
                     f"{batch_cleaned}/{len(batch)} classified")

            usage = data.get("usage", {})
            if usage:
                in_tok = usage.get("prompt_tokens", 0)
                out_tok = usage.get("completion_tokens", 0)
                total_in_tok += in_tok if isinstance(in_tok, int) else 0
                total_out_tok += out_tok if isinstance(out_tok, int) else 0

            # 배치 간 딜레이 (rate limit 방지)
            if batch_idx + BATCH_SIZE < len(missing_stocks):
                time.sleep(1)

        except json.JSONDecodeError as e:
            log.warning(f"[LLM] Batch {batch_num} JSON parse error: {e}")
            continue
        except requests.exceptions.Timeout:
            log.warning(f"[LLM] Batch {batch_num} timeout (60s)")
            continue
        except Exception as e:
            log.warning(f"[LLM] Batch {batch_num} error: {e}")
            continue

    # 전체 결과 로그
    log.info(f"[LLM] Total: {len(all_results)}/{len(missing_stocks)} classified")
    if total_in_tok or total_out_tok:
        cost = (total_in_tok * 0.15 + total_out_tok * 0.60) / 1_000_000
        log.info(f"[LLM] Tokens: in={total_in_tok} out={total_out_tok} "
                 f"cost=${cost:.6f}")

    # 샘플 로그
    for code, theme in list(all_results.items())[:5]:
        name = next((s["name"] for s in missing_stocks if s["code"] == code), code)
        log.info(f"[LLM]   {code} {name} -> {theme}")

    return all_results


# ============================================================================
#  네이버 증권 테마 크롤링 (자동 테마 수집)
# ============================================================================
def fetch_naver_theme_map(max_theme_pages: int = 30,
                          delay: float = 0.12) -> Dict[str, List[str]]:
    """
    네이버 증권 테마 페이지를 크롤링하여 종목코드→테마명 역매핑을 구축.
    
    흐름:
      1) 테마 목록 페이지(paginated) → 테마ID + 테마명 수집
      2) 각 테마 상세 페이지 → 소속 종목코드 추출
      3) 역매핑: {종목코드: [테마1, 테마2, ...]}
      4) 일별 캐시 (하루 1회만 크롤링)
    
    Returns:
        Dict[str, List[str]]: {6자리코드: ["테마명1", "테마명2", ...]}
        실패 시 빈 dict 반환
    """
    # 캐시 확인 (당일 파일 존재하면 재사용)
    cache_file = os.path.join(
        CACHE_DIR, f"naver_themes_{date.today().strftime('%Y%m%d')}.json")
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cached = json.load(f)
            log.info(f"[NAVER-THEME] Loaded cache: {len(cached)} stocks, "
                     f"{sum(len(v) for v in cached.values())} theme-links")
            return cached
        except Exception as e:
            log.warning(f"[NAVER-THEME] Cache load failed: {e}")

    headers = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/131.0.0.0 Safari/537.36"),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8",
        "Accept-Encoding": "gzip, deflate",
        "Referer": "https://finance.naver.com/sise/theme.naver",
    }

    # regex 패턴: 테마 목록에서 테마 ID+이름 추출
    re_theme_link = re.compile(
        r'sise_group_detail\.naver\?type=theme&(?:amp;)?no=(\d+)"[^>]*>'
        r'\s*([^<]+?)\s*</a>', re.IGNORECASE)

    # regex 패턴: 테마 상세에서 종목코드 추출
    re_stock_code = re.compile(
        r'(?:main\.naver|main\.nhn)\?code=(\d{6})', re.IGNORECASE)

    # -----------------------------------------------------------
    # Step 1: 테마 목록 수집 (paginated)
    # -----------------------------------------------------------
    themes: List[Tuple[str, str]] = []  # [(theme_id, theme_name), ...]
    log.info(f"[NAVER-THEME] Fetching theme list (max {max_theme_pages} pages)...")

    for page in range(1, max_theme_pages + 1):
        try:
            url = f"https://finance.naver.com/sise/theme.naver?&page={page}"
            resp = requests.get(url, headers=headers, timeout=10)
            resp.encoding = "euc-kr"

            if resp.status_code != 200:
                log.warning(f"[NAVER-THEME] Theme list page {page}: "
                            f"HTTP {resp.status_code}")
                if resp.status_code == 403:
                    log.warning("[NAVER-THEME] 403 Forbidden - "
                                "Naver may be blocking this IP.")
                break

            page_themes = re_theme_link.findall(resp.text)
            if not page_themes:
                log.debug(f"[NAVER-THEME] Page {page}: no themes found (last page)")
                break

            for tid, tname in page_themes:
                themes.append((tid, tname.strip()))

            log.debug(f"[NAVER-THEME] Page {page}: {len(page_themes)} themes")
            time.sleep(delay)

        except requests.exceptions.Timeout:
            log.warning(f"[NAVER-THEME] Page {page} timeout")
            break
        except Exception as e:
            log.warning(f"[NAVER-THEME] Page {page} error: {e}")
            break

    if not themes:
        log.warning("[NAVER-THEME] No themes fetched. "
                    "Falling back to LLM/keyword only.")
        return {}

    # 중복 제거 (동일 테마가 여러 페이지에 나올 수 있음)
    seen = set()
    unique_themes = []
    for tid, tname in themes:
        if tid not in seen:
            seen.add(tid)
            unique_themes.append((tid, tname))
    themes = unique_themes

    log.info(f"[NAVER-THEME] Found {len(themes)} unique themes. "
             f"Fetching member stocks...")

    # -----------------------------------------------------------
    # Step 2: 각 테마별 소속 종목코드 추출
    # -----------------------------------------------------------
    reverse_map: Dict[str, List[str]] = {}  # code -> [theme_name, ...]
    fetched = 0
    failed = 0

    for idx, (tid, tname) in enumerate(themes):
        try:
            url = (f"https://finance.naver.com/sise/"
                   f"sise_group_detail.naver?type=theme&no={tid}")
            resp = requests.get(url, headers=headers, timeout=10)
            resp.encoding = "euc-kr"

            if resp.status_code != 200:
                failed += 1
                continue

            codes_found = re_stock_code.findall(resp.text)
            # 중복 제거
            codes_found = list(dict.fromkeys(codes_found))

            for code in codes_found:
                if code not in reverse_map:
                    reverse_map[code] = []
                if tname not in reverse_map[code]:
                    reverse_map[code].append(tname)

            fetched += 1

            # 진행률 (50개마다)
            if (idx + 1) % 50 == 0:
                log.info(f"[NAVER-THEME] Progress: {idx+1}/{len(themes)} themes, "
                         f"{len(reverse_map)} stocks mapped")

            time.sleep(delay)

        except requests.exceptions.Timeout:
            failed += 1
            continue
        except Exception as e:
            failed += 1
            log.debug(f"[NAVER-THEME] Theme '{tname}'({tid}) error: {e}")
            continue

    log.info(f"[NAVER-THEME] Done: {fetched}/{len(themes)} themes fetched "
             f"({failed} failed), {len(reverse_map)} stocks mapped")

    # 샘플 출력
    sample_items = list(reverse_map.items())[:5]
    for code, theme_list in sample_items:
        log.info(f"[NAVER-THEME]   {code} -> {theme_list}")

    # -----------------------------------------------------------
    # Step 3: 캐시 저장
    # -----------------------------------------------------------
    if reverse_map:
        try:
            os.makedirs(CACHE_DIR, exist_ok=True)
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(reverse_map, f, ensure_ascii=False, indent=1)
            log.info(f"[NAVER-THEME] Cache saved: {cache_file}")
        except Exception as e:
            log.warning(f"[NAVER-THEME] Cache save failed: {e}")

    return reverse_map


# 종목명 키워드 → 테마 매핑 (LLM fallback)
# [v18.1] 대기업 정확 매칭을 상단에 배치 (부분 매칭보다 우선)
_NAME_EXACT_MAP = {
    # 대기업 / 유명 종목 직접 매핑
    "삼성전자": "반도체", "SK하이닉스": "반도체", "삼성SDI": "2차전지",
    "LG에너지솔루션": "2차전지", "LG화학": "화학", "LG전자": "전자",
    "현대차": "자동차", "기아": "자동차", "현대모비스": "자동차부품",
    "KT": "통신", "SK텔레콤": "통신", "LG유플러스": "통신",
    "NAVER": "IT/플랫폼", "네이버": "IT/플랫폼",
    "카카오": "IT/플랫폼", "카카오뱅크": "인터넷은행",
    "카카오페이": "핀테크", "삼성바이오로직스": "바이오",
    "셀트리온": "바이오", "SK이노베이션": "에너지",
    "포스코홀딩스": "철강", "HD현대중공업": "조선",
    "HD현대에너지솔루션": "태양광", "현대오토에버": "IT/SW",
    "대한항공": "항공", "한화에어로스페이스": "방산",
    "한화오션": "조선", "HD한국조선해양": "조선",
    "하이브": "엔터", "JYP Ent.": "엔터", "SM": "엔터",
    "우리금융지주": "금융", "BNK금융지주": "금융",
    "KB금융": "금융", "신한지주": "금융", "하나금융지주": "금융",
    "메리츠금융지주": "금융", "한국전력": "전력",
    "두산퓨얼셀": "수소", "두산에너빌리티": "원전",
    "DL이앤씨": "건설", "SK바이오팜": "바이오",
    "KT&G": "담배/기타", "S-Oil": "에너지/정유",
    "SK": "지주", "LG": "지주", "삼성물산": "지주",
    "HD현대": "지주", "한화": "방산",
    "솔브레인": "반도체소재", "대덕전자": "반도체/PCB",
}

_NAME_THEME_MAP = [
    ("인베스트", "창투"), ("벤처", "창투"), ("파트너스", "창투"),
    ("캐피탈", "창투"), ("에셋", "자산운용"),
    ("증권", "증권"), ("금융지주", "금융"), ("금융", "금융"),
    ("보험", "보험"), ("은행", "은행"), ("카드", "카드"),
    ("리츠", "부동산"), ("부동산", "부동산"),
    ("로봇", "로봇"), ("자율주행", "자율주행"), ("드론", "드론"),
    ("건설기계", "건설기계"), ("건설", "건설"), ("기계", "기계"),
    ("조선", "조선"), ("해운", "해운"), ("물류", "물류"), ("운송", "운송"),
    ("철강", "철강"), ("금속", "금속"), ("알루미", "금속"),
    ("자동차", "자동차"), ("모터", "모터"), ("부품", "자동차부품"),
    ("반도체", "반도체"), ("칩", "반도체"), ("링크", "반도체"),
    ("디스플레이", "디스플레이"), ("패널", "디스플레이"),
    ("소프트", "IT/SW"), ("솔루션", "IT/SW"), ("클라우드", "IT"),
    ("시스템", "IT"), ("테크", "IT"), ("데이터", "IT"),
    ("AI", "AI"), ("인공지능", "AI"),
    ("전자", "전자"), ("전기", "전기"), ("전력", "전력기기"),
    ("통신", "통신"), ("텔레", "통신"), ("네트워크", "통신"),
    ("게임", "게임"), ("엔터", "엔터"),
    ("바이오", "바이오"), ("제약", "바이오"), ("약품", "바이오"),
    ("메디", "의료기기"), ("의료", "의료기기"), ("헬스", "헬스케어"),
    ("셀", "바이오"), ("진단", "진단"), ("유전", "바이오"),
    ("2차전지", "2차전지"), ("배터리", "2차전지"), ("전지", "2차전지"),
    ("리튬", "2차전지"), ("양극", "2차전지"), ("음극", "2차전지"),
    ("에너지", "에너지"), ("태양", "태양광"), ("솔라", "태양광"),
    ("풍력", "풍력"), ("수소", "수소"),
    ("화학", "화학"), ("소재", "소재"),
    ("레이저", "장비"), ("장비", "장비"),
    ("식품", "식품"), ("음료", "식품"), ("농업", "농업"),
    ("유통", "유통"), ("마트", "유통"), ("쇼핑", "유통"),
    ("백화점", "유통"), ("패션", "패션"), ("화장품", "화장품"),
    ("방산", "방산"), ("항공", "항공"), ("우주", "우주항공"),
    ("미사일", "방산"), ("방위", "방산"),
    ("원전", "원전"), ("원자력", "원전"), ("핵", "원전"),
    ("HBM", "반도체"), ("파운드리", "반도체"),
    ("양자", "양자컴퓨터"), ("SMR", "원전"),
]


def fill_missing_themes(results: List[dict],
                        theme_map: Dict[str, List[str]]) -> None:
    """
    테마가 없는 종목에 대해 4단계로 분류:
      -1차: 네이버 증권 테마 크롤링 (자동, 정치테마 포함 전체 커버)
       0차: 정확 매칭 (대기업/유명 종목 직접 매핑)
       1차: OpenAI GPT-4o-mini로 시장 테마 분류
       2차: 종목명 키워드 매칭 fallback
    theme_map은 in-place 수정됨.
    """
    total_all = sum(1 for r in results if r["total_block"] > 0)

    # -1차: 네이버 증권 테마 크롤링 (최우선)
    # [v18.14] 매 거래일마다 네이버 테마로 강제 동기화 (REPLACE)
    #   기존: `if not theme_map.get(code)` 게이트 → 한 번 분류된 종목은 영원히 갱신 안 됨
    #   문제: 신규상장주(예: 더즌)가 상장 직후 일부 테마만 가진 채 캐시되면, 이후
    #         네이버에 새 테마(예: 스테이블코인)가 추가돼도 영영 반영 안 되던 버그
    #   변경: 게이트 제거. 네이버에 종목이 있으면 무조건 전체 교체 (sync 우선)
    #         네이버에 없는 종목만 LLM/키워드 fallback 단계로 진입
    naver_map = fetch_naver_theme_map()

    # ─────────────────────────────────────────────────────────────────────
    # [v18.15] 사전 동기화 단계: results와 무관하게 naver_map 전체를 theme_map에 등록
    #   배경: v18.14의 sync 로직은 `for r in results:` 루프 안에 있어서, 블록딜 분석
    #         입구 필터를 통과하지 못한 종목(예: 더즌 462860 — 거래대금 미달)은
    #         theme_map에 영원히 등록되지 않았음. 진단 결과 네이버에는 2,344개가 있으나
    #         results는 ~490개로 약 1,800개 종목이 분류 누락 상태였음.
    #   변경: results 루프 진입 전에 naver_map 전체를 theme_map에 사전 등록.
    #         이렇게 하면 어떤 종목이 나중에 results에 들어오더라도 즉시 정확한
    #         분류가 적용되며, results에 영영 안 들어와도 분류 자체는 유지됨.
    #   영향: theme_map 종목수 ~490 → ~2,344 (약 5배), cache 파일 크기 약 2배 증가
    # ─────────────────────────────────────────────────────────────────────
    presync_added = 0    # 신규 등록 (이전에 없던 종목)
    presync_changed = 0  # 기존 등록 + 변경 (이전과 다른 테마)
    presync_same = 0     # 변경 없음
    if naver_map:
        log.info(f"[THEME-PRESYNC] 네이버 전체 사전 동기화 시작 "
                 f"(naver={len(naver_map)}종목, 기존 theme_map={len(theme_map)}종목)")
        for clean_code, naver_themes_raw in naver_map.items():
            if not naver_themes_raw:
                continue
            # CYBOS 형식 (A + 6자리)으로 통일
            code_with_a = "A" + clean_code if not clean_code.startswith("A") else clean_code
            normalized = list(dict.fromkeys(
                normalize_theme(t) for t in naver_themes_raw if t
            ))
            if not normalized:
                continue
            prev = theme_map.get(code_with_a, [])
            if not prev:
                theme_map[code_with_a] = normalized
                presync_added += 1
            elif prev != normalized:
                theme_map[code_with_a] = normalized
                presync_changed += 1
            else:
                presync_same += 1

        log.info(f"[THEME-PRESYNC] 완료: 신규 {presync_added}, "
                 f"변경 {presync_changed}, 동일 {presync_same} | "
                 f"theme_map 총 {len(theme_map)}종목")

        # 이상 징후 자동 경고
        if presync_added == 0 and presync_changed == 0:
            log.warning(f"[THEME-PRESYNC] ⚠️ 신규/변경 0건 — 이미 모두 동기화된 상태이거나 "
                        f"naver_map이 비어있을 가능성")
        if len(naver_map) < 1000:
            log.warning(f"[THEME-PRESYNC] ⚠️ naver_map 종목수 {len(naver_map)} < 1000 — "
                        f"네이버 크롤링이 일부만 성공했을 가능성. max_theme_pages 점검 필요")

    naver_filled = 0
    naver_changed = 0  # [v18.14] 실제로 변경된 종목 수 (audit용)
    if naver_map:
        for r in results:
            code = r["code"]
            if r["total_block"] > 0:
                # 네이버 코드는 6자리, CYBOS 코드는 A+6자리 (예: A005930)
                clean_code = code[1:] if code.startswith("A") else code
                if clean_code in naver_map:
                    # [v18.14] 네이버를 권위 있는 소스로 간주하고 전체 교체
                    # [v18.15] 사전 동기화 단계에서 이미 처리됐을 가능성이 높지만,
                    #          results의 code 형식이 다를 수 있어 안전하게 한 번 더 수행
                    naver_themes = [normalize_theme(t) for t in naver_map[clean_code]]
                    # 중복 제거하면서 순서 유지
                    naver_themes = list(dict.fromkeys(naver_themes))
                    prev = theme_map.get(code, [])
                    if naver_themes != prev:
                        theme_map[code] = naver_themes
                        naver_changed += 1
                        if prev:
                            log.info(f"[THEME-SYNC] {code} {r.get('name','')}: "
                                     f"{prev} → {naver_themes}")
                    else:
                        theme_map[code] = naver_themes  # 동일해도 보장
                    naver_filled += 1
        if naver_filled:
            log.info(f"[THEME-FILL] Naver theme (results 매칭): {naver_filled}/{total_all} stocks "
                     f"(changed={naver_changed})")
    else:
        log.info("[THEME-FILL] Naver theme unavailable, skipping")

    # 0차: 정확 매칭 (LLM 호출 전에 먼저 처리)
    exact_filled = 0
    for r in results:
        code = r["code"]
        name = r["name"]
        if not theme_map.get(code):
            # 종목명에서 소스태그 제거 (예: "KT 거래상위" → "KT")
            clean_name = name.split(" ")[0] if " " in name else name
            if clean_name in _NAME_EXACT_MAP:
                theme_map[code] = [normalize_theme(_NAME_EXACT_MAP[clean_name])]
                exact_filled += 1
    if exact_filled:
        log.info(f"[THEME-FILL] Exact match: {exact_filled} stocks")

    missing = [r for r in results
               if r["total_block"] > 0 and not theme_map.get(r["code"])]

    if not missing:
        log.info("[THEME-FILL] All stocks have themes - no lookup needed")
        return

    log.info(f"[THEME-FILL] {len(missing)} stocks without theme")

    # 1차: LLM
    existing_themes = []
    for themes_list in theme_map.values():
        existing_themes.extend(themes_list)

    llm_result = classify_themes_via_llm(missing, existing_themes)

    llm_filled = 0
    still_missing = []
    for r in missing:
        code = r["code"]
        if code in llm_result and llm_result[code] != "-":
            theme_map[code] = [llm_result[code]]
            llm_filled += 1
        else:
            still_missing.append(r)

    if llm_filled > 0:
        log.info(f"[THEME-FILL] LLM classified: {llm_filled}/{len(missing)}")

    # 2차: 키워드
    keyword_filled = 0
    if still_missing:
        log.info(f"[THEME-FILL] {len(still_missing)} remaining -> keyword heuristic")
        for r in still_missing:
            code = r["code"]
            name = r["name"]
            matched = None
            for keyword, theme in _NAME_THEME_MAP:
                if keyword in name:
                    matched = theme
                    break
            if matched:
                theme_map[code] = [normalize_theme(matched)]
                keyword_filled += 1
                log.info(f"[THEME-FILL] {code} {name} -> [{matched}] (keyword)")
        log.info(f"[THEME-FILL] keyword: {keyword_filled}/{len(still_missing)}")

    total_filled = naver_filled + exact_filled + llm_filled + keyword_filled
    log.info(f"[THEME-FILL] Total: {total_filled}/{total_all} filled "
             f"(naver:{naver_filled}, exact:{exact_filled}, "
             f"LLM:{llm_filled}, keyword:{keyword_filled})")



# ============================================================================
#  HTML 회차 네비게이션 (이전/다음 리포트 이동)
# ============================================================================

def _scan_report_files(output_dir: str) -> List[str]:
    """output_block 폴더에서 block_trade_YYYYMMDD_HHMM.html 파일 목록을 시간순 정렬 반환."""
    import re
    pattern = re.compile(r'^block_trade_\d{8}_\d{4}\.html$')
    files = []
    if os.path.isdir(output_dir):
        for f in os.listdir(output_dir):
            if pattern.match(f):
                files.append(f)
    files.sort()  # 파일명이 YYYYMMDD_HHMM 이라 문자열 정렬 = 시간순
    return files


def _build_nav_html(prev_file: str = None, next_file: str = None) -> tuple:
    """이전/다음 파일명으로 네비게이션 HTML 슬롯 생성. (prev_slot, next_slot) 반환."""
    if prev_file:
        prev_slot = f'<a class="nav-arrow" href="{prev_file}">◀ 이전</a>'
    else:
        prev_slot = '<span class="nav-placeholder">◀ 이전</span>'

    if next_file:
        next_slot = f'<a class="nav-arrow" href="{next_file}">다음 ▶</a>'
    else:
        next_slot = '<span class="nav-placeholder">다음 ▶</span>'

    return prev_slot, next_slot


def _inject_nav(html: str, prev_file: str = None, next_file: str = None,
                nav_index: int = 0, nav_total: int = 0) -> str:
    """HTML 문자열의 네비게이션 플레이스홀더를 실제 링크로 교체."""
    prev_slot, next_slot = _build_nav_html(prev_file, next_file)
    html = html.replace('__NAV_PREV_SLOT__', prev_slot)
    html = html.replace('__NAV_NEXT_SLOT__', next_slot)
    if nav_index > 0 and nav_total > 0:
        html = html.replace('__NAV_INDEX_SLOT__', f'{nav_index}/{nav_total}')
    else:
        html = html.replace('__NAV_INDEX_SLOT__', '')
    return html


def _update_prev_file_next_link(output_dir: str, prev_filename: str, current_filename: str):
    """직전 회차 HTML 파일을 열어서 '다음' 링크를 현재 파일로 업데이트."""
    prev_path = os.path.join(output_dir, prev_filename)
    if not os.path.exists(prev_path):
        return
    try:
        with open(prev_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # 아직 next가 placeholder인 경우 교체
        old_next = '<span class="nav-placeholder">다음 ▶</span>'
        new_next = f'<a class="nav-arrow" href="{current_filename}">다음 ▶</a>'

        if old_next in content:
            content = content.replace(old_next, new_next)
            with open(prev_path, 'w', encoding='utf-8') as f:
                f.write(content)
            log.debug(f"[NAV] Updated prev file next link: {prev_filename} -> {current_filename}")
    except Exception as e:
        log.warning(f"[NAV] Failed to update prev file: {e}")


# ============================================================================
#  [v18.9 패치] Hot/Cold 테마 순위 계산 (최근 10거래일 MCWAR 기반)
# ============================================================================
#  사용자 요구사항:
#    - 최근 10거래일의 Hot BEST 10 / Cold WORST 10 테마 순위
#    - 종목수 ≥ 5인 테마만 순위 대상
#    - 코드는 과거 HTML 파일(OUTPUT_DIR 내) + 현재 스냅샷을 합쳐 계산
#  설계 주의사항:
#    - 과거 HTML 파서는 v17/v18 구버전/신버전 모두 호환 (data 속성 유연 추출)
#    - 일자별 '마지막 스냅샷'만 사용 (장중 스냅샷 노이즈 배제)
#    - 상한가/하한가(±29.5%) 종목 제외
#    - coldness = days_since_last_top3 / n_days (Top3 는 min_stocks 만족 테마 중)
# ============================================================================

def _parse_past_html_for_themes(html_path: str, min_mcap: int = 2000) -> Dict[str, dict]:
    """
    과거 HTML 파일에서 테마별 MCWAR + 종목수 계산.
    버전 무관 파서: data-change, data-mcap, data-theme 만 필요.
    Returns: {theme_normalized: {'mcwar': float, 'n_stocks': int}}

    [v18.10] 내부적으로는 _parse_past_html_data 를 호출하고 theme_agg 부분만 반환.
    """
    return _parse_past_html_data(html_path, min_mcap)['theme_agg']


def _parse_past_html_data(html_path: str, min_mcap: int = 2000) -> dict:
    """
    [v18.10] 단일 패스 파서 - 과거 HTML에서 (종목 리스트 + 테마 집계) 모두 반환.

    Returns:
        {
            'stocks': [{'code', 'change_pct', 'mcap'}, ...],
            'theme_agg': {theme_normalized: {'mcwar', 'n_stocks'}}
        }
    """
    empty = {'stocks': [], 'theme_agg': {}}
    try:
        with open(html_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception:
        return empty

    # 개별주 섹션만 추출 (ETF 섹션 제외)
    m = re.search(r'id="section-individual"[^>]*>(.*?)(?=id="section-etf"|$)',
                  content, re.DOTALL)
    section = m.group(1) if m else content

    tr_re = re.compile(r'<tr\s+data-[^>]+>.*?</tr>', re.DOTALL)
    attr_re = re.compile(r'data-([a-z-]+)="([^"]*?)"')
    code_re = re.compile(r'class="code-cell">(\d{6})</td>')

    stocks_list: List[dict] = []
    theme_stocks: Dict[str, List[Tuple[float, int]]] = defaultdict(list)

    for tr_block in tr_re.findall(section):
        attrs = dict(attr_re.findall(tr_block))
        if 'change' not in attrs or 'mcap' not in attrs:
            continue
        code_m = code_re.search(tr_block)
        if not code_m:
            continue
        code = code_m.group(1)
        try:
            change = float(attrs.get('change') or 0)
            mcap = int(attrs.get('mcap') or 0)
        except (ValueError, TypeError):
            continue
        if mcap < min_mcap:
            continue
        if abs(change) >= 29.5:  # 상한가/하한가 제외
            continue

        # 종목 리스트에 추가 (테마 유무 상관없이 - 스코어 랭킹용)
        stocks_list.append({
            'code': code,
            'change_pct': change,
            'mcap': mcap,
        })

        # 테마 집계용
        theme_raw = attrs.get('theme', '') or ''
        themes = [t.strip() for t in theme_raw.split(',') if t.strip()]
        if not themes:
            continue
        primary = normalize_theme(themes[0])
        if not primary or len(primary) < 2 or primary in ('-', '—', '–'):
            continue
        theme_stocks[primary].append((change, mcap))

    theme_agg = {}
    for theme, items in theme_stocks.items():
        total_mcap = sum(m for _, m in items)
        if total_mcap > 0:
            mcwar = sum(c * m / total_mcap for c, m in items)
            theme_agg[theme] = {'mcwar': round(mcwar, 4), 'n_stocks': len(items)}

    return {'stocks': stocks_list, 'theme_agg': theme_agg}


def _get_last_snapshot_per_day(output_dir: str, max_days: int = 9,
                                exclude_today: Optional[str] = None) -> List[Tuple[str, str]]:
    """
    output_dir 내의 block_trade_YYYYMMDD_HHMM.html 파일들을 일자별로 그룹화하여,
    각 일자의 '가장 늦은 시각' 파일 경로를 반환.

    Args:
        output_dir: HTML 파일 디렉토리
        max_days: 최대 반환 일수 (최근순)
        exclude_today: 제외할 날짜 YYYYMMDD (현재 실행 중인 당일은 아직 파일이 없거나
                       있어도 부분 스냅샷이라 별도 처리)

    Returns: [(date_str, filepath), ...] 오래된 날짜 → 최근 날짜 순
    """
    pattern = re.compile(r'^block_trade_(\d{8})_(\d{4})\.html$')
    by_date: Dict[str, List[str]] = defaultdict(list)
    if not os.path.isdir(output_dir):
        return []
    for fname in os.listdir(output_dir):
        m = pattern.match(fname)
        if not m:
            continue
        date_str = m.group(1)
        if exclude_today and date_str == exclude_today:
            continue
        by_date[date_str].append(fname)

    dates_sorted = sorted(by_date.keys())
    result = []
    for d in dates_sorted:
        files = sorted(by_date[d])  # 시각 기준 정렬
        last_file = files[-1]
        result.append((d, os.path.join(output_dir, last_file)))
    return result[-max_days:]  # 최근 max_days 만


def _compute_stock_coldness_map(daily_stocks_list: List[List[dict]],
                                  top_n_per_day: int = 100) -> Dict[str, dict]:
    """
    [v18.10] 개별종목 coldness 계산 (Option B: change × log10(mcap) 스코어 기반)

    각 날짜에 대해:
        score = change_pct × log10(max(mcap_억, 10))
    점수 상위 top_n_per_day 개 종목을 해당일의 '핫 종목'으로 판정.
    종목별 coldness = 마지막 핫일 이후 경과일 / 전체 일수.

    Args:
        daily_stocks_list: 각 날짜의 종목 리스트들 (오래된 날짜 → 최근 날짜 순)
                          각 종목: {'code', 'change_pct', 'mcap'}
        top_n_per_day: 각 날 핫 종목으로 판정할 상위 N개

    Returns:
        {code: {'coldness', 'days_since_hot', 'hot_count', 'appearances'}}
    """
    import math
    n_days = len(daily_stocks_list)
    if n_days == 0:
        return {}

    # 일별 핫 종목 셋 + 일별 등장 종목 셋 (O(1) 조회용)
    # [v18.11] 종목별 누적 스코어도 함께 집계 (tiebreak 용)
    daily_hot_sets: List[set] = []
    daily_code_sets: List[set] = []
    cum_scores: Dict[str, float] = defaultdict(float)
    for day_stocks in daily_stocks_list:
        # 스코어 계산 (log10으로 시총 편향 완화)
        scored = []
        for s in day_stocks:
            mcap = max(s.get('mcap', 0), 10)  # log(0) 방지
            score = s.get('change_pct', 0.0) * math.log10(mcap)
            scored.append((s['code'], score))
            cum_scores[s['code']] += score  # [v18.11] 누적 스코어
        # 상위 top_n_per_day 추출
        scored.sort(key=lambda x: -x[1])
        daily_hot_sets.append(set(code for code, _ in scored[:top_n_per_day]))
        daily_code_sets.append(set(s['code'] for s in day_stocks))

    # 기간 내 등장한 전체 종목 코드
    all_codes = set()
    for code_set in daily_code_sets:
        all_codes.update(code_set)

    result = {}
    for code in all_codes:
        # 등장 일수
        appearances = sum(1 for cs in daily_code_sets if code in cs)

        # 마지막 핫일 역탐색 (오늘 = 마지막 index)
        last_hot_idx = None
        hot_count = 0
        for i in range(n_days - 1, -1, -1):
            if code in daily_hot_sets[i]:
                if last_hot_idx is None:
                    last_hot_idx = n_days - 1 - i  # 오늘부터의 경과일
                hot_count += 1

        if last_hot_idx is None:
            days_since_hot = n_days  # 한 번도 핫 아님
            coldness = 1.0
        else:
            days_since_hot = last_hot_idx
            coldness = days_since_hot / n_days

        result[code] = {
            'coldness': round(coldness, 3),
            'days_since_hot': days_since_hot,
            'hot_count': hot_count,
            'appearances': appearances,
            'cum_score': round(cum_scores[code], 2),  # [v18.11] 누적 스코어 (tiebreak 용)
        }
    return result


def _compute_hot_cold_themes(current_stocks: List[dict],
                              theme_map: Dict[str, List[str]],
                              output_dir: str,
                              lookback: int = 10,
                              min_stocks: int = 5,
                              stock_top_n: int = 100) -> Tuple[List[dict], List[dict], int, Dict[str, list]]:
    """
    현재 스냅샷 + 과거 (lookback-1)일 마지막 스냅샷을 합쳐 테마 Hot/Cold 순위 계산.

    Hot BEST 10: 최근 Top3 진입이 잦고 평균 MCWAR 높은 순 (coldness 낮음)
    Cold WORST 10: Top3 진입이 없거나 오래됐고 평균 MCWAR 낮은 순 (coldness 높음)

    [v18.10] 추가: 개별종목 coldness + drilldown_data 계산
        drilldown_data: {theme_name: [{code, name, change_pct, coldness, ...}]}
        → 사용자가 테마명 클릭 시 소속 종목 리스트를 표시하기 위함

    Returns: (hot_best, cold_worst, n_days, drilldown_data)
    """
    # 1) 오늘 날짜 (현재 실행 시점)
    today_str = date.today().strftime("%Y%m%d")

    # 2) 과거 (lookback-1)일의 마지막 스냅샷 수집
    past_days = _get_last_snapshot_per_day(
        output_dir, max_days=lookback - 1, exclude_today=today_str
    )

    # 3) 일별 데이터 파싱 (통합 파서로 stocks + theme_agg 한 번에)
    #    daily_data: List[Tuple[date_str, theme_agg_dict, stocks_list]]
    daily_data: List[Tuple[str, Dict[str, dict], List[dict]]] = []
    for date_str, filepath in past_days:
        parsed = _parse_past_html_data(filepath, min_mcap=2000)
        if parsed['theme_agg'] or parsed['stocks']:
            daily_data.append((date_str, parsed['theme_agg'], parsed['stocks']))

    # 4) 현재(today) 테마 집계 + 종목 리스트 생성
    today_theme_stocks: Dict[str, List[Tuple[float, int]]] = defaultdict(list)
    today_stocks_for_ranking: List[dict] = []
    for s in current_stocks:
        code = s.get("code", "")
        if not code:
            continue
        mcap = s.get("market_cap", 0)  # 억원
        change = s.get("change_rate", 0.0)
        if mcap < 2000:
            continue
        if abs(change) >= 29.5:
            continue
        # 종목 랭킹용 리스트 (테마 유무와 무관)
        today_stocks_for_ranking.append({
            'code': code,
            'change_pct': change,
            'mcap': mcap,
        })
        # 테마 집계
        themes = theme_map.get(code, [])
        if not themes:
            continue
        primary = normalize_theme(themes[0])
        if not primary or len(primary) < 2 or primary in ('-', '—', '–'):
            continue
        today_theme_stocks[primary].append((change, mcap))

    today_themes: Dict[str, dict] = {}
    for theme, items in today_theme_stocks.items():
        total_mcap = sum(m for _, m in items)
        if total_mcap > 0:
            mcwar = sum(c * m / total_mcap for c, m in items)
            today_themes[theme] = {'mcwar': round(mcwar, 4), 'n_stocks': len(items)}

    if today_themes or today_stocks_for_ranking:
        daily_data.append((today_str, today_themes, today_stocks_for_ranking))

    n_days = len(daily_data)
    if n_days < 2:
        return [], [], n_days, {}

    # 5) 유효 테마: 기간 내 한 번이라도 min_stocks 이상
    all_themes = set()
    for _, day_themes, _ in daily_data:
        for theme, info in day_themes.items():
            if info['n_stocks'] >= min_stocks:
                all_themes.add(theme)

    # 6) 각 날짜의 Top3 (min_stocks 만족 테마들 중 MCWAR 상위 3개)
    daily_top3: List[set] = []
    for _, day_themes, _ in daily_data:
        qualified = [
            (t, info['mcwar']) for t, info in day_themes.items()
            if info['n_stocks'] >= min_stocks
        ]
        qualified.sort(key=lambda x: -x[1])
        daily_top3.append(set(t for t, _ in qualified[:3]))

    # 7) 테마별 metrics 계산
    results_list = []
    for theme in all_themes:
        mcwar_values = []
        n_stocks_values = []
        for _, day_themes, _ in daily_data:
            info = day_themes.get(theme)
            if info and info['n_stocks'] >= min_stocks:
                mcwar_values.append(info['mcwar'])
                n_stocks_values.append(info['n_stocks'])

        if not mcwar_values:
            continue

        active_days = len(mcwar_values)
        cum_mcwar = sum(mcwar_values)
        avg_mcwar = cum_mcwar / active_days

        last_hot_idx = None
        hot_count = 0
        for i in range(n_days - 1, -1, -1):
            if theme in daily_top3[i]:
                if last_hot_idx is None:
                    last_hot_idx = n_days - 1 - i
                hot_count += 1

        if last_hot_idx is None:
            days_since_hot = n_days
            coldness = 1.0
        else:
            days_since_hot = last_hot_idx
            coldness = days_since_hot / n_days

        avg_n_stocks = sum(n_stocks_values) / active_days
        today_info = today_themes.get(theme, {})
        today_mcwar = today_info.get('mcwar') if today_info else None

        results_list.append({
            'theme': theme,
            'coldness': round(coldness, 3),
            'days_since_hot': days_since_hot,
            'hot_count': hot_count,
            'cum_mcwar': round(cum_mcwar, 2),
            'avg_mcwar': round(avg_mcwar, 2),
            'active_days': active_days,
            'avg_n_stocks': round(avg_n_stocks, 1),
            'today_mcwar': today_mcwar,
        })

    # [v18.11] 정렬키 변경: (coldness, hot_count, cum_mcwar) 3단 tiebreak
    # → 동일 coldness 시 hot_count 많은 테마 우선, 그 다음 cum_mcwar 우수 테마 우선
    hot_best = sorted(
        results_list,
        key=lambda x: (x['coldness'], -x['hot_count'], -x['cum_mcwar'])
    )[:10]
    cold_worst = sorted(
        results_list,
        key=lambda x: (-x['coldness'], x['hot_count'], x['cum_mcwar'])
    )[:10]

    # ================================================================
    # [v18.10] 종목 coldness 계산 + drilldown_data 빌드
    # ================================================================
    # 일별 종목 리스트를 추출하여 _compute_stock_coldness_map 호출
    daily_stocks_only = [stocks for _, _, stocks in daily_data]
    stock_coldness_map = _compute_stock_coldness_map(
        daily_stocks_only, top_n_per_day=stock_top_n
    )

    # 오늘 스냅샷의 code → (name, change_rate, market_cap) 매핑
    today_code_info: Dict[str, dict] = {}
    for s in current_stocks:
        code = s.get("code", "")
        if code:
            today_code_info[code] = {
                'name': s.get('name', code),
                'change_pct': s.get('change_rate', 0.0),
                'mcap': s.get('market_cap', 0),
            }

    # drilldown_data: Hot/Cold 10+10 테마에 한해서만 빌드 (최대 20개 테마)
    drilldown_themes = set()
    for t in hot_best:
        drilldown_themes.add(t['theme'])
    for t in cold_worst:
        drilldown_themes.add(t['theme'])

    # [v18.12] drilldown_data: themes[0] 단일 매칭 → 전체 정규화 테마 목록 포함 매칭으로 교체
    # 이유: 종목의 첫 번째 테마가 아닌 2차 이하 테마가 대표 테마와 일치하는 경우도 드릴다운에 포함해야 함
    # 예: 건설 테마 9종목 중 2개만 표시되던 문제 → 전체 테마 목록 순회로 수정
    drilldown_data: Dict[str, list] = {}
    for theme in drilldown_themes:
        stocks_in_theme = []
        seen_codes: set = set()
        for s in current_stocks:
            code = s.get("code", "")
            if not code or code in seen_codes:
                continue
            themes = theme_map.get(code, [])
            if not themes:
                continue
            # [v18.12] 다중 테마 포함 매칭: 종목의 모든 테마를 정규화한 뒤 theme 포함 여부 확인
            normalized_themes = []
            for t in themes:
                nt = normalize_theme(t)
                if nt and nt not in normalized_themes:
                    normalized_themes.append(nt)
            if theme not in normalized_themes:
                continue
            mcap = s.get("market_cap", 0)
            change = s.get("change_rate", 0.0)
            if mcap < 2000:
                continue
            if abs(change) >= 29.5:
                continue

            cold_info = stock_coldness_map.get(code, {})
            stocks_in_theme.append({
                'code': code,
                'name': s.get('name', code),
                'change_pct': round(change, 2),
                'mcap': mcap,
                'coldness': cold_info.get('coldness', None),
                'days_since_hot': cold_info.get('days_since_hot', None),
                'hot_count': cold_info.get('hot_count', 0),
                'appearances': cold_info.get('appearances', 0),
                'cum_score': cold_info.get('cum_score', 0.0),
            })
            seen_codes.add(code)

        # [v18.12] 정렬 보강: coldness 없는 종목 맨 뒤, 그 다음 coldness 오름차순,
        # appearances 내림 → hot_count 내림 → cum_score 내림 → mcap 내림 → 이름 오름
        stocks_in_theme.sort(
            key=lambda x: (
                x['coldness'] is None,                                    # coldness 없는 종목 맨 뒤
                x['coldness'] if x['coldness'] is not None else 999,      # coldness 낮을수록 앞
                -x['appearances'],                                        # 자주 등장한 종목 우선
                -x['hot_count'],                                          # hot 횟수 많은 종목 우선
                -x['cum_score'],                                          # 누적 스코어 높은 종목 우선
                -x['mcap'],                                               # 시총 큰 종목 우선
                x['name'],                                                # 이름 오름차순 (tie-break)
            )
        )
        log.info(
            f"[DRILLDOWN] {theme}: total={len(stocks_in_theme)}, "
            f"has_coldness={sum(1 for x in stocks_in_theme if x['coldness'] is not None)}, "
            f"no_coldness={sum(1 for x in stocks_in_theme if x['coldness'] is None)}"
        )
        drilldown_data[theme] = stocks_in_theme

    # [v18.12] current_theme_counts: display_nstocks 계산 (변수명 오류 수정)
    current_theme_counts: Dict[str, int] = defaultdict(int)
    for s in current_stocks:
        code = s.get('code', '')
        if not code:
            continue
        themes = theme_map.get(code, [])
        if not themes:
            continue
        primary = normalize_theme(themes[0])
        if not primary or len(primary) < 2 or primary in ('-', '—', '–'):
            continue
        mcap = s.get('market_cap', 0)
        change = s.get('change_rate', 0.0)
        if mcap < 2000:
            continue
        if abs(change) >= 29.5:
            continue
        current_theme_counts[primary] += 1

    for item in hot_best:
        item['display_nstocks'] = current_theme_counts.get(item['theme'], round(item.get('avg_n_stocks', 0)))
    for item in cold_worst:
        item['display_nstocks'] = current_theme_counts.get(item['theme'], round(item.get('avg_n_stocks', 0)))

    return hot_best, cold_worst, n_days, drilldown_data


def _build_hot_cold_themes_html(hot_best: List[dict], cold_worst: List[dict],
                                 n_days: int, min_stocks: int,
                                 drilldown_data: Dict[str, list]) -> str:
    """
    Hot/Cold 테마 순위 HTML 블록 생성.
    [v18.10] 테마명 클릭 → 드릴다운 영역에 종목 리스트 + 개별종목 coldness 표시
    """
    if n_days < 2:
        return (
            '<div class="info-box" style="border-left-color: #ff5722;">'
            '<strong>🔥 테마 순환 순위</strong><br>'
            f'<span style="color:#888;font-size:11px;">'
            f'과거 HTML 파일이 부족하여 순위 계산 불가 (필요: 최소 2일, 현재: {n_days}일). '
            f'스크립트를 며칠간 실행하여 데이터가 축적되면 표시됩니다.</span>'
            '</div>'
        )

    if not hot_best and not cold_worst:
        return ""

    import json
    import html as html_module

    def _row(idx: int, item: dict) -> str:
        avg = item['avg_mcwar']
        avg_color = "#4caf50" if avg >= 0 else "#f44336"
        cum = item['cum_mcwar']
        cum_color = "#4caf50" if cum >= 0 else "#f44336"
        today = item.get('today_mcwar')
        if today is None:
            today_str_val = '—'
            today_color = "#666"
        else:
            today_str_val = f"{today:+.2f}%"
            today_color = "#4caf50" if today >= 0 else "#f44336"
        theme_name = item["theme"]
        theme_esc = html_module.escape(theme_name, quote=True)
        has_drilldown = theme_name in drilldown_data and len(drilldown_data[theme_name]) > 0
        if has_drilldown:
            theme_cell = (
                f'<td class="hot-cold-theme-cell" data-theme="{theme_esc}" '
                f'style="color:#bb86fc;font-weight:bold;padding:3px 6px;'
                f'cursor:pointer;text-decoration:underline dotted;" '
                f'title="클릭하면 소속 종목과 coldness를 표시합니다">{theme_esc}</td>'
            )
        else:
            theme_cell = (
                f'<td style="color:#bb86fc;font-weight:bold;padding:3px 6px;">{theme_esc}</td>'
            )
        return (
            '<tr>'
            f'<td style="text-align:center;color:#888;padding:3px 6px;">{idx}</td>'
            + theme_cell +
            f'<td style="text-align:right;color:{avg_color};padding:3px 6px;">{avg:+.2f}%</td>'
            f'<td style="text-align:right;color:{cum_color};padding:3px 6px;font-weight:bold;">{cum:+.2f}%</td>'
            f'<td style="text-align:right;color:{today_color};padding:3px 6px;">{today_str_val}</td>'
            f'<td style="text-align:center;color:#aaa;padding:3px 6px;">{item["hot_count"]}</td>'
            f'<td style="text-align:center;color:#aaa;padding:3px 6px;">{item["days_since_hot"]}</td>'
            f'<td style="text-align:center;color:#aaa;padding:3px 6px;">{item.get("display_nstocks", item["avg_n_stocks"]):.0f}</td>'
            '</tr>'
        )

    th_css = (
        "padding:4px 6px;border-bottom:1px solid #333;color:#888;"
        "text-align:center;font-weight:normal;font-size:10px;"
    )
    header = (
        '<tr>'
        f'<th style="{th_css}">#</th>'
        f'<th style="{th_css}">테마</th>'
        f'<th style="{th_css}" title="{n_days}일 평균 MCWAR">평균</th>'
        f'<th style="{th_css}" title="{n_days}일 누적 MCWAR (정렬 tiebreak 기준)">누적</th>'
        f'<th style="{th_css}" title="오늘 MCWAR">오늘</th>'
        f'<th style="{th_css}" title="기간 내 일별 Top3 진입 횟수">핫↑</th>'
        f'<th style="{th_css}" title="마지막 Top3 진입 이후 경과일">휴지</th>'
        f'<th style="{th_css}" title="평균 종목수">종목</th>'
        '</tr>'
    )
    table_css = (
        "width:100%;border-collapse:collapse;font-size:11px;"
        "background:#1a1a1a;border:1px solid #2a2a2a;"
    )

    hot_rows = ''.join(_row(i + 1, item) for i, item in enumerate(hot_best))
    cold_rows = ''.join(_row(i + 1, item) for i, item in enumerate(cold_worst))

    # JSON 직렬화 (한글 보존 + </script> 회피)
    drilldown_json = json.dumps(drilldown_data, ensure_ascii=False)
    drilldown_json = drilldown_json.replace('</', '<\\/')

    # 상단 테이블 HTML (f-string)
    tables_html = f'''
<!-- [v18.10] Hot/Cold Theme Ranking + Drilldown -->
<div class="info-box" style="border-left-color: #ff5722;">
    <strong>🔥 테마 순환 순위</strong>
    <span style="font-size:11px;color:#888;margin-left:8px;">
        최근 {n_days}거래일 | 테마 종목수 ≥ {min_stocks} | 테마명 클릭 → 소속 종목 드릴다운
    </span>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-top:8px;">
        <div>
            <div style="color:#ff7043;font-weight:bold;font-size:12px;margin-bottom:4px;">
                🔥 HOT BEST {len(hot_best)}
                <span style="color:#888;font-weight:normal;font-size:10px;">최근 주도 테마</span>
            </div>
            <table style="{table_css}">
                <thead>{header}</thead>
                <tbody>{hot_rows}</tbody>
            </table>
        </div>
        <div>
            <div style="color:#42a5f5;font-weight:bold;font-size:12px;margin-bottom:4px;">
                🧊 COLD WORST {len(cold_worst)}
                <span style="color:#888;font-weight:normal;font-size:10px;">오래 소외된 테마</span>
            </div>
            <table style="{table_css}">
                <thead>{header}</thead>
                <tbody>{cold_rows}</tbody>
            </table>
        </div>
    </div>
    <div id="hot-cold-drilldown" style="display:none;margin-top:12px;padding:10px 12px;background:#1a1a1a;border:1px solid #333;border-radius:4px;"></div>
    <div style="font-size:10px;color:#666;margin-top:6px;line-height:1.5;">
        평균 = 기간 내 일별 시총가중평균등락률의 평균 &nbsp;|&nbsp;
        <b style="color:#aaa;">누적 = 기간 내 일별 MCWAR 합 (동점 시 정렬 tiebreak)</b> &nbsp;|&nbsp;
        핫↑ = 기간 내 일별 MCWAR Top3 진입 횟수 &nbsp;|&nbsp;
        휴지 = 마지막 Top3 진입 이후 경과일 (오늘=0) &nbsp;|&nbsp;
        종목 = 기간 내 min_stocks 충족 일수의 평균 종목수 &nbsp;|&nbsp;
        <span style="color:#bb86fc;">개별종목 coldness</span> = (등락률×log10(시총)) 랭킹 상위 100 진입 이력 기반 &nbsp;|&nbsp;
        <b style="color:#aaa;">종목 동점 시 hot_count → 누적스코어 순 tiebreak</b>
    </div>
</div>
'''

    # JS 블록 (non-f-string + placeholder 치환으로 중괄호 이스케이프 회피)
    drilldown_js = '''
<script>
(function() {
    window.hotColdDrilldownData = __DRILLDOWN_DATA_JSON__;
    window.showHotColdDrilldown = function(themeName) {
        var area = document.getElementById('hot-cold-drilldown');
        if (!area) return;
        var data = window.hotColdDrilldownData[themeName];
        if (!data || data.length === 0) {
            area.style.display = 'none';
            area.dataset.currentTheme = '';
            return;
        }
        // 같은 테마 재클릭 시 닫기
        if (area.dataset.currentTheme === themeName && area.style.display !== 'none') {
            area.style.display = 'none';
            area.dataset.currentTheme = '';
            return;
        }
        var esc = function(s) {
            return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;')
                   .replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
        };
        var html = '<div style="margin-bottom:6px;color:#ff7043;font-weight:bold;font-size:12px;">';
        html += '📌 ' + esc(themeName) + ' 현재 전체 종목 (' + data.length + '개)';
        html += ' <span style="font-weight:normal;color:#888;font-size:10px;">';
        html += '(coldness 계산값은 있으면 표시, 없으면 —)</span>';
        html += ' <span style="float:right;cursor:pointer;color:#666;" onclick="showHotColdDrilldown(\\'' + esc(themeName).replace(/'/g, "\\\\'") + '\\')">✕ 닫기</span></div>';
        html += '<table style="width:100%;border-collapse:collapse;font-size:11px;">';
        html += '<thead><tr>';
        var headers = ['#', '종목명', '코드', '오늘%', 'coldness', '핫↑', '누적', '휴지', '등장'];
        var tips = ['', '', '', '당일 등락률', '0=오늘 핫, 1=한 번도 핫 아님', '기간 내 Top100 진입 횟수', '기간 내 change×log10(mcap) 누적 (tiebreak 기준)', '마지막 핫일 이후 경과일', '기간 내 데이터 등장일수'];
        for (var k = 0; k < headers.length; k++) {
            html += '<th style="padding:3px 6px;border-bottom:1px solid #333;color:#888;font-weight:normal;font-size:10px;text-align:center;" title="' + tips[k] + '">' + headers[k] + '</th>';
        }
        html += '</tr></thead><tbody>';
        for (var i = 0; i < data.length; i++) {
            var s = data[i];
            var chg = s.change_pct;
            var chgColor = chg >= 0 ? '#4caf50' : '#f44336';
            var chgStr = (chg >= 0 ? '+' : '') + chg.toFixed(2) + '%';
            var coldStr, coldColor;
            if (s.coldness === null || s.coldness === undefined) {
                coldStr = '—';
                coldColor = '#666';
            } else {
                coldStr = s.coldness.toFixed(2);
                if (s.coldness <= 0.2) coldColor = '#ff7043';
                else if (s.coldness >= 0.8) coldColor = '#42a5f5';
                else coldColor = '#aaa';
            }
            var daysStr = (s.days_since_hot === null || s.days_since_hot === undefined) ? '—' : s.days_since_hot;
            var cumScore = (s.cum_score !== null && s.cum_score !== undefined) ? s.cum_score : 0;
            var cumColor = cumScore >= 0 ? '#4caf50' : '#f44336';
            var cumStr = (cumScore >= 0 ? '+' : '') + cumScore.toFixed(1);
            html += '<tr>';
            html += '<td style="text-align:center;color:#888;padding:3px 6px;">' + (i + 1) + '</td>';
            html += '<td style="color:#ddd;padding:3px 6px;">' + esc(s.name) + '</td>';
            html += '<td style="color:#8b8fa3;font-family:Consolas,monospace;font-size:11px;padding:3px 6px;">' + esc(s.code) + '</td>';
            html += '<td style="text-align:right;color:' + chgColor + ';padding:3px 6px;">' + chgStr + '</td>';
            html += '<td style="text-align:right;color:' + coldColor + ';padding:3px 6px;font-weight:bold;">' + coldStr + '</td>';
            html += '<td style="text-align:center;color:#aaa;padding:3px 6px;">' + s.hot_count + '</td>';
            html += '<td style="text-align:right;color:' + cumColor + ';padding:3px 6px;">' + cumStr + '</td>';
            html += '<td style="text-align:center;color:#aaa;padding:3px 6px;">' + daysStr + '</td>';
            html += '<td style="text-align:center;color:#aaa;padding:3px 6px;">' + s.appearances + '</td>';
            html += '</tr>';
        }
        html += '</tbody></table>';
        area.innerHTML = html;
        area.style.display = 'block';
        area.dataset.currentTheme = themeName;
    };
    // 이벤트 위임 (테마 셀 클릭)
    document.addEventListener('click', function(e) {
        var t = e.target;
        if (t && t.classList && t.classList.contains('hot-cold-theme-cell')) {
            var themeName = t.dataset.theme;
            if (themeName) window.showHotColdDrilldown(themeName);
        }
    });
})();
</script>
'''
    drilldown_js = drilldown_js.replace('__DRILLDOWN_DATA_JSON__', drilldown_json)

    return tables_html + drilldown_js


# ============================================================================
#  HTML 리포트 생성 (v17 다크테마 기반)
# ============================================================================

def _build_program_trade_payload(results: List[dict]) -> str:
    """
    [v18.20] 모달 팝업용 시계열 데이터 빌드 (gzip+base64).
    - results 중 program_trade 있는 STOCK 종목만 (ETF는 컬럼 자체가 없음)
    - 30초 단위 원본 그대로 (사용자 요구)
    - HTML <script>에 임베드되어 클릭 시 JS에서 디코딩
    형식: { "<code>": { "n": "종목명", "rows": [[tm,cur_prc,pred_pre,flu_rt,trde_qty,
                                                 prm_sell_qty,prm_buy_qty,prm_netprps_qty,
                                                 prm_netprps_qty_irds], ...] } }
    행 순서는 응답 그대로(최신→과거).
    """
    import base64, gzip, json as _json

    payload: Dict[str, dict] = {}
    n_stocks = 0
    n_total_rows = 0

    for r in results:
        pt = r.get("program_trade")
        if not pt or not pt.get("rows"):
            continue
        code = r.get("code", "")
        if not code:
            continue
        rows = pt["rows"]

        # 키움 음수 표기 정규화 + 컴팩트 배열 (필드 9개)
        compact_rows = []
        for row in rows:
            def _num(k):
                v = row.get(k, "")
                if v in (None, "", "0"):
                    return v if v != "" else "0"
                # "--14240" → "-14240", "+245500" → "245500"
                s = str(v).replace("--", "-").replace("+", "")
                return s

            compact_rows.append([
                str(row.get("tm", "")),
                _num("cur_prc"),
                _num("pred_pre"),
                _num("flu_rt"),
                _num("trde_qty"),
                _num("prm_sell_qty"),
                _num("prm_buy_qty"),
                _num("prm_netprps_qty"),
                _num("prm_netprps_qty_irds"),
            ])

        payload[code] = {
            "n": r.get("name", ""),
            "rows": compact_rows,
        }
        n_stocks += 1
        n_total_rows += len(compact_rows)

    if not payload:
        log.info("[KIWOOM_PT] 임베드할 페이로드 없음")
        return "{}"

    # gzip + base64
    raw = _json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    raw_bytes = raw.encode("utf-8")
    gz = gzip.compress(raw_bytes, compresslevel=9)
    b64 = base64.b64encode(gz).decode("ascii")

    log.info(f"[KIWOOM_PT] 페이로드: {n_stocks}종목 × 평균 {n_total_rows//max(n_stocks,1)}행 "
             f"| raw={len(raw_bytes)/1024:.1f}KB → gzip={len(gz)/1024:.1f}KB "
             f"→ base64={len(b64)/1024:.1f}KB")
    return b64


def generate_html_report(results: List[dict], theme_map: Dict[str, List[str]],
                         run_time: str, elapsed_sec: float) -> str:
    """HTML 리포트 생성 - 다크테마 풀 기능"""

    # ETN 제외, ETF/STOCK 분리 (대량체결 있는 것만)
    stocks = [r for r in results if r["type"] == "STOCK" and r["total_block"] > 0]
    etfs = [r for r in results if r["type"] == "ETF" and r["total_block"] > 0]

    # 순매수금액 기준 정렬
    stocks.sort(key=lambda x: x["net_amount"], reverse=True)
    etfs.sort(key=lambda x: x["net_amount"], reverse=True)

    total_stocks_analyzed = len(results)

    # ── 소스별 종목 수 ──
    source_counts = defaultdict(int)
    for r in results:
        src = r.get("source", "volume_top")
        if "surge" in src:
            source_counts["급등(≥5%)"] += 1
        if "volume" in src:
            source_counts["거래상위"] += 1
    source_info = " | ".join(f"{k} {v}" for k, v in sorted(source_counts.items()))

    # ── 요약 통계 ──
    stock_buy = sum(r["block_buy_amount"] for r in stocks)
    stock_sell = sum(r["block_sell_amount"] for r in stocks)
    stock_net = stock_buy - stock_sell
    etf_buy = sum(r["block_buy_amount"] for r in etfs)
    etf_sell = sum(r["block_sell_amount"] for r in etfs)
    etf_net = etf_buy - etf_sell
    all_buy = stock_buy + etf_buy
    all_sell = stock_sell + etf_sell
    all_net = all_buy - all_sell

    # 방향성 카운트
    dir_counts = {"Strong Buy": 0, "Buy": 0, "Neutral": 0, "Sell": 0, "Strong Sell": 0}
    for s in stocks:
        d = s["direction"]
        if d in dir_counts:
            dir_counts[d] += 1

    n_stocks = len(stocks)
    bullish_count = sum(1 for s in stocks if s["direction"] in ("Strong Buy", "Buy") and s.get("change_rate", 0) > 0)
    bearish_count = sum(1 for s in stocks if s["direction"] in ("Strong Sell", "Sell") and s.get("change_rate", 0) < 0)

    # ── 방향성 분포 바 ──
    def make_direction_bar(counts: dict, total: int) -> str:
        if total == 0:
            return ""
        segments = [
            ("seg-strong-buy", "강한매수", counts.get("Strong Buy", 0)),
            ("seg-buy", "매수우위", counts.get("Buy", 0)),
            ("seg-neutral", "균형", counts.get("Neutral", 0)),
            ("seg-sell", "매도우위", counts.get("Sell", 0)),
            ("seg-strong-sell", "강한매도", counts.get("Strong Sell", 0)),
        ]
        bar_html = '<div class="direction-bar">'
        for cls, label, cnt in segments:
            if cnt == 0:
                continue
            pct = cnt / total * 100
            bar_html += (
                f'<div class="seg {cls}" style="width:{pct:.1f}%;" '
                f'title="{label}: {cnt}종목 ({pct:.0f}%)">{cnt}</div>\n'
            )
        bar_html += '</div>'
        return bar_html

    direction_bar = make_direction_bar(dir_counts, n_stocks)

    # ── 테마별 분포 바 ──
    def make_theme_bar(stocks_list: List[dict], tmap: Dict[str, List[str]]) -> Tuple[str, str]:
        # [v18.13 버그수정] themes[0] 단일 매칭 → 전체 정규화 테마 매칭
        # 이유: 드릴다운(_compute_hot_cold_themes)은 v18.12에서 전체 테마 매칭으로 수정됐으나
        #       이 분포 바 함수는 동일 패치가 누락되어 라벨 숫자와 드릴다운 종목 수가 불일치했음
        # 효과: 한 종목이 보유한 모든 테마(중복제거)에 +1 카운트 → 드릴다운 종목 수와 정확히 일치
        # 퍼센트 분모는 종목 수(total)가 아닌 태그 총합(tag_total) 사용 → 바 너비 합 100% 유지
        theme_counter = Counter()
        no_theme = 0
        for s in stocks_list:
            themes = tmap.get(s["code"], [])
            if themes:
                normalized = set(normalize_theme(t) for t in themes if t)
                for nt in normalized:
                    theme_counter[nt] += 1
            else:
                no_theme += 1
        total = len(stocks_list)
        if total == 0:
            return "", ""
        if no_theme > 0:
            theme_counter["미분류"] = no_theme
        sorted_themes = theme_counter.most_common()
        tag_total = sum(theme_counter.values())  # [v18.13] 분모: 종목 수가 아닌 태그 총합
        palette = [
            "#e53935", "#1e88e5", "#43a047", "#fb8c00", "#8e24aa",
            "#00acc1", "#f4511e", "#3949ab", "#7cb342", "#c0ca33",
            "#6d4c41", "#546e7a", "#d81b60", "#00897b", "#fdd835",
            "#5e35b1", "#039be5", "#e65100", "#2e7d32", "#ad1457",
        ]
        bar_html = '<div class="direction-bar">'
        legend_items = []
        for idx, (theme_name, cnt) in enumerate(sorted_themes):
            pct = cnt / tag_total * 100  # [v18.13] 분모를 tag_total로 변경 (바 너비 합 100% 유지)
            color = palette[idx % len(palette)]
            display = f"{cnt}" if pct >= 5 else ""
            bar_html += (
                f'<div class="seg" style="width:{pct:.1f}%; background:{color}; '
                f'min-width:2px;" '
                f'title="{theme_name}: {cnt}종목 ({pct:.1f}%)">{display}</div>\n'
            )
            legend_items.append(
                f'<span class="theme-legend-item" '
                f'onclick="toggleThemeFilter(\'{theme_name}\')">'
                f'<span style="display:inline-block; width:10px; height:10px; '
                f'border-radius:50%; background:{color}; margin-right:4px;"></span>'
                f'{theme_name}({cnt}, {pct:.1f}%)</span>'
            )
        bar_html += '</div>'
        legend_html = '<div style="margin-top:6px; line-height:1.8;">' + "".join(legend_items) + '</div>'
        return bar_html, legend_html

    theme_bar_html, theme_legend_html = make_theme_bar(stocks, theme_map)

    # ── [v18.9/v18.10] Hot/Cold 테마 순환 순위 계산 + 종목 드릴다운 ──
    try:
        _hot_best, _cold_worst, _n_days, _drilldown = _compute_hot_cold_themes(
            stocks, theme_map, OUTPUT_DIR,
            lookback=10, min_stocks=5, stock_top_n=100
        )
        hot_cold_html = _build_hot_cold_themes_html(
            _hot_best, _cold_worst, _n_days, min_stocks=5,
            drilldown_data=_drilldown
        )
    except Exception as e:
        log.warning(f"[v18.10] Hot/Cold theme ranking failed: {e}")
        hot_cold_html = ""

    # ── [v18.6] ATR(7)% 셀 생성 헬퍼 ──
    def _program_trade_td(item: dict, table_id: str) -> str:
        """
        [v18.20] 키움 ka90008 기반 프로그램순매수% 셀 생성.
        - ETF 테이블에서는 빈 셀 (사용자 요구로 ETF 제외)
        - 데이터는 data-pt-payload 속성에 gzip+base64 임베드 → 클릭 시 모달
        """
        if table_id == "etfTable":
            return ""  # ETF는 컬럼 자체가 없으므로 빈 문자열
        pt = item.get("program_trade")
        if not pt or pt.get("latest_ratio") is None:
            return '<td class="pt-na" data-sort-val="-9999">-</td>\n'
        ratio = pt["latest_ratio"]
        # 색상 강조: 한국 관습 (양수=빨강, 음수=파랑) + |값| 단계별 진하기
        abs_r = abs(ratio)
        if abs_r >= 10.0:
            level = "very-strong"
        elif abs_r >= 5.0:
            level = "strong"
        else:
            level = "weak"
        sign = "pos" if ratio >= 0 else "neg"
        cls = f"pt-{sign}-{level}"
        val = f"{ratio:+.2f}%"
        # 데이터 임베드: 종목코드만 data-pt-code 로 (실제 페이로드는 별도 SCRIPT에 담음)
        code = item.get("code", "")
        return (f'<td class="pt-cell {cls}" data-pt-code="{code}" '
                f'data-sort-val="{ratio:.4f}" '
                f'onclick="showProgramTradeModal(this)" '
                f'title="클릭하여 시간대별 추이 보기 (현재 {val})">{val}</td>\n')

    def _atr7_td(atr_pct: float) -> str:
        if 0.5 <= atr_pct <= 1.0:
            cls = "atr-sweet"
        elif atr_pct > 1.0:
            cls = "atr-high"
        elif atr_pct > 0:
            cls = "atr-low"
        else:
            cls = ""
        val = f"{atr_pct:.2f}%" if atr_pct > 0 else "-"
        return f'<td class="{cls}">{val}</td>\n'

    # ── [v18.8] 삼각형 매수타점 셀 생성 헬퍼 ──
    def _triangle_tds(item: dict) -> str:
        """타점(2x) + 남은시간 2개 셀 생성. 1.5x는 tooltip."""
        tri = item.get("_triangle", {})
        if tri.get("status") != "OK":
            return ('<td class="tri-na col-hidden" data-sort-val="999999">-</td>\n'
                    '<td class="tri-na col-hidden" data-sort-val="999999">-</td>\n')

        t2x = tri["target_2x"]
        t15x = tri["target_1_5x"]
        tr2 = tri.get("time_remain_2x", -1)
        tr15 = tri.get("time_remain_1_5x", -1)
        peak = tri.get("peak", 0)
        center = tri.get("center", 0)
        x_val = tri.get("x", 0)
        cur = tri.get("current_price", 0)

        # 타점(2x) 셀 — tooltip에 1.5x, P, C, X 정보 포함
        tip = (f"1.5x 타점: {t15x:,}원&#10;"
               f"P(고점): {peak:,}원&#10;"
               f"C(중심): {center:,}원&#10;"
               f"X(거리): {x_val:,}원&#10;"
               f"현재가: {cur:,}원")
        target_td = (f'<td class="amount-cell col-hidden" title="{tip}" '
                     f'data-sort-val="{t2x}">{t2x:,}</td>\n')

        # 남은시간 셀
        if tr2 is None:
            time_cls = "tri-na"
            time_str = "?"
            sort_val = 999998
        elif tr2 <= 0:
            time_cls = "tri-reached"
            time_str = "도달"
            sort_val = 0
        elif tr2 <= 5:
            time_cls = "tri-imminent"
            time_str = f"~{tr2:.0f}분"
            sort_val = tr2
        elif tr2 <= 15:
            time_cls = "tri-near"
            time_str = f"~{tr2:.0f}분"
            sort_val = tr2
        else:
            time_cls = ""
            time_str = f"~{tr2:.0f}분"
            sort_val = tr2

        # 1.5x 남은시간 tooltip
        if tr15 is None:
            time_tip = "1.5x: ATR없음"
        elif tr15 <= 0:
            time_tip = "1.5x: 도달"
        else:
            time_tip = f"1.5x: ~{tr15:.0f}분"
        time_td = (f'<td class="{time_cls} col-hidden" title="{time_tip}" '
                   f'data-sort-val="{sort_val:.1f}">{time_str}</td>\n')

        return target_td + time_td

    # ── [v18.9] 거래대금 회귀 스크리닝 셀 생성 헬퍼 ──
    def _tv_reversion_tds(item: dict) -> str:
        """이탈도(σ) + 회복예상 2개 셀 생성 (v18.9 테스트용)"""
        itatdo = item.get("tv_itatdo")
        rec_mins = item.get("tv_recovery_mins")

        if itatdo is None:
            return ('<td class="est-na col-hidden" data-sort-val="9999">-</td>\n'
                    '<td class="est-na col-hidden" data-sort-val="9999">-</td>\n')

        # 이탈도 셀 CSS
        if itatdo <= 0:
            itd_cls = "tv-sufficient"
        elif itatdo < 1.0:
            itd_cls = "tv-low"
        elif itatdo < 2.0:
            itd_cls = "tv-mid"
        else:
            itd_cls = "tv-high"

        itd_str = f"{itatdo:+.2f}σ"
        itd_sort = f"{itatdo:.4f}"

        # 회복예상 셀
        rec_str = fmt_recovery_mins(rec_mins)
        if rec_mins is None:
            rec_cls, rec_sort = "est-na", "9999"
        elif rec_mins <= 0:
            rec_cls, rec_sort = "tv-sufficient", "0"
        elif rec_mins <= 390:
            rec_cls, rec_sort = "tv-today", f"{rec_mins:.1f}"
        elif rec_mins <= 780:
            rec_cls, rec_sort = "tv-2days", f"{rec_mins:.1f}"
        else:
            rec_cls, rec_sort = "tv-long", f"{rec_mins:.1f}"

        mu_v = item.get("_tv_mu_v", 0)
        theta = item.get("_tv_theta") or 0.0
        # [v18.9] U자형 보조값 (검증 비교용)
        itatdo_u = item.get("tv_itatdo_u")
        rec_mins_u = item.get("tv_recovery_mins_u")
        est_eod = item.get("_est_eod", 0)
        est_eod_u = item.get("_est_eod_u", 0)

        tip = ""
        if mu_v > 0:
            # U자형 비교 줄 (값이 있을 때만)
            u_compare = ""
            if itatdo_u is not None:
                u_rec_str = fmt_recovery_mins(rec_mins_u) if rec_mins_u is not None else "N/A"
                u_compare = (
                    f'──────────&#10;'
                    f'[U자형 보정 비교]&#10;'
                    f'예상마감(단순): {est_eod/1e8:.0f}억&#10;'
                    f'예상마감(U자형): {est_eod_u/1e8:.0f}억&#10;'
                    f'이탈도(U자형): {itatdo_u:+.2f}σ&#10;'
                    f'회복예상(U자형): {u_rec_str}&#10;'
                )
            tip = (f'title="기준대금(μ): {mu_v/1e8:.0f}억&#10;'
                   f'θ≈{theta:.3f} (AR(1) 1차 근사, 엄밀값=−ln(β))&#10;'
                   f'이탈도(메인): {itatdo:.2f}σ&#10;'
                   f'{u_compare}'
                   f'[테스트용 — 백테스팅 미검증, OU 휴리스틱]" ')

        return (f'<td class="{itd_cls} col-hidden" data-sort-val="{itd_sort}" {tip}>{itd_str}</td>\n'
                f'<td class="{rec_cls} col-hidden" data-sort-val="{rec_sort}">{rec_str}</td>\n')

    # ── [v18.6] 예상마감대금 비교 + 20일고가대비 셀 생성 헬퍼 ──
    def _est_eod_cls(pct: float) -> str:
        """예상마감대금 비교/20일고가대비 CSS 클래스"""
        if pct == 0.0:
            return "est-na"
        if abs(pct) >= 100:
            return "est-strong-pos" if pct > 0 else "est-strong-neg"
        if pct > 0:
            return "est-positive"
        return "est-negative"

    def _est_eod_tds(item: dict) -> str:
        """전일동시간대비(%), 시총대비예상마감(%), 예상대비D-1, D-2, 20일고가대비 5개 셀 생성
        [v18.18] 전일동시간대비 셀을 맨 앞에 추가
        """
        d1 = item.get("est_eod_vs_d1", 0.0)
        d2 = item.get("est_eod_vs_d2", 0.0)
        h20 = item.get("close_vs_20d_high", 0.0)
        est_eod = item.get("est_eod", 0)
        mcap = item.get("market_cap", 0)  # 억원 단위

        # [v18.18] 전일동시간대비 셀 산산
        sync_pct = item.get("prev_day_sync_pct", 0.0)
        if sync_pct > 0.0:
            sync_str = f"{sync_pct:.1f}%"
            sync_cls = "ratio-strong-buy" if sync_pct >= 100.0 else "ratio-buy"
            sync_sort = f"{sync_pct:.4f}"
            sync_tooltip = (f"전일 동시각 누적대비 당일 누적\n"
                            f"≥100%: 전일 보다 수급 강함 (빨강)\n"
                            f"<100%: 전일 보다 수급 약함 (파랑)\n"
                            f"PineScript 동일 로직: barTradeVal=(H+L)/2×V")
            sync_td = (f'<td class="{sync_cls}" '
                       f'data-sort-val="{sync_sort}" '
                       f'title="{sync_tooltip}">{sync_str}</td>\n')
        else:
            sync_td = '<td class="est-na">-</td>\n'

        # 예상마감대금 = 0이면 데이터 없음
        if est_eod <= 0:
            return (sync_td +
                    '<td class="est-na">-</td>\n'
                    '<td class="est-na col-hidden">-</td>\n'
                    '<td class="est-na col-hidden">-</td>\n'
                    '<td class="est-na">-</td>\n')

        # 시총대비예상마감(%) = (예상마감대금 / 시총) × 100
        # est_eod: 원 단위 / mcap: 억원 단위 → 단위 맞춤
        est_eod_억 = est_eod / 1e8
        if mcap > 0:
            est_vs_mcap = est_eod_억 / mcap * 100  # (억/억)*100
        else:
            est_vs_mcap = 0.0

        # 시총대비 CSS: 높을수록 강조 (홍인기 원칙상 거래대금 폭발 종목)
        if est_vs_mcap >= 5.0:
            evm_cls = "ratio-strong-buy"   # 시총 5% 이상 — 매우 강한 수급
        elif est_vs_mcap >= 2.0:
            evm_cls = "ratio-buy"          # 2~5%
        elif est_vs_mcap >= 0.5:
            evm_cls = ""                   # 0.5~2% 보통
        else:
            evm_cls = "est-na"             # 0.5% 미만 — 수급 미미

        evm_str = f"{est_vs_mcap:.2f}%"

        # D-1
        d1_cls = _est_eod_cls(d1)
        d1_str = f"{d1:+.0f}%" if d1 != 0 else "-"

        # D-2
        d2_cls = _est_eod_cls(d2)
        d2_str = f"{d2:+.0f}%" if d2 != 0 else "-"

        # 20일고가대비 (항상 0 이하)
        h20_cls = _est_eod_cls(h20)
        h20_str = f"{h20:.1f}%" if h20 != 0 else "0.0%"

        # tooltip: 예상마감대금 절대값(억) + 시총(억) 표시
        tooltip = f"예상마감대금: {est_eod_억:,.0f}억&#10;시총: {mcap:,.0f}억&#10;= 시총의 {est_vs_mcap:.2f}%"

        # [v18.18] 전일동시간대비 셀을 맨 앞에 함께 반환 (5개 셀)
        return (sync_td +
                f'<td class="{evm_cls}" data-sort-val="{est_vs_mcap:.4f}" title="{tooltip}">{evm_str}</td>\n'
                f'<td class="{d1_cls} col-hidden">{d1_str}</td>\n'
                f'<td class="{d2_cls} col-hidden">{d2_str}</td>\n'
                f'<td class="{h20_cls}">{h20_str}</td>\n')

    # ── 테이블 행 생성 ──
    def make_table_rows(items: List[dict], table_id: str) -> str:
        rows = ""
        for i, item in enumerate(items, 1):
            code = item["code"]
            themes = theme_map.get(code, [])
            # 중복 제거 (순서 유지)
            if themes:
                seen_t = set()
                deduped = []
                for t in themes:
                    nt = normalize_theme(t)
                    if nt and nt not in seen_t:
                        seen_t.add(nt)
                        deduped.append(nt)
                theme_str = ", ".join(deduped) if deduped else "-"
            else:
                theme_str = "-"

            chg = item.get("change_rate", 0)
            buy_amt = item["block_buy_amount"]
            sell_amt = item["block_sell_amount"]
            net = item["net_amount"]
            daily_vol = item.get("daily_volume", 0)
            mcap = item.get("market_cap", 0)  # 억원 단위
            total_cnt = item["block_buy_count"] + item["block_sell_count"]  # [v18.1] = 매수우세봉 + 매도우세봉
            br = item["buy_ratio"]
            direction = item["direction"]
            dir_label = dir_kr(direction)

            # 소스 배지
            source = item.get("source", "")
            source_badge = ""
            if "surge" in source and "volume" in source:
                source_badge = ' <span class="source-badge surge">급등+거래</span>'
            elif "surge" in source:
                source_badge = ' <span class="source-badge surge">급등</span>'
            elif "volume" in source:
                source_badge = ' <span class="source-badge vol-top">거래상위</span>'

            # 등락률 CSS
            if chg > 0:
                chg_cls = "return-positive"
            elif chg < 0:
                chg_cls = "return-negative"
            else:
                chg_cls = ""
            chg_str = f"+{chg:.1f}%" if chg > 0 else f"{chg:.1f}%"

            # 매수비율 CSS (0~100%, 50% 중립)
            if br >= 60.0:
                ratio_cls = "ratio-strong-buy"
            elif br >= 55.0:
                ratio_cls = "ratio-buy"
            elif br <= 40.0:
                ratio_cls = "ratio-strong-sell"
            elif br <= 45.0:
                ratio_cls = "ratio-sell"
            else:
                ratio_cls = ""

            # 방향성 CSS
            dir_cls_map = {
                "Strong Buy": "dir-strong-buy",
                "Buy": "dir-buy",
                "Neutral": "dir-neutral",
                "Sell": "dir-sell",
                "Strong Sell": "dir-strong-sell",
            }
            dir_cls = dir_cls_map.get(direction, "dir-neutral")

            # 순매수 CSS
            net_cls = "net-positive" if net >= 0 else "net-negative"

            # [v18.3] 시총대비 순매수비율 = (순매수금액 / 시총) × 100
            if mcap > 0:
                mcap_ratio = net / (mcap * 1e8) * 100  # net:원, mcap:억원
            else:
                mcap_ratio = 0.0
            # CSS: ±0.5% 이상이면 강조
            if mcap_ratio >= 0.5:
                mcap_ratio_cls = "ratio-buy"
            elif mcap_ratio <= -0.5:
                mcap_ratio_cls = "ratio-sell"
            else:
                mcap_ratio_cls = ""

            # [v18.5] 호가/시총 비율
            ob_ratio = item.get("ob_ratio", 0.0)
            ask_remain = item.get("ask_remain", 0)
            bid_remain = item.get("bid_remain", 0)
            ob_amount_억 = ob_ratio / 100 * mcap if mcap > 0 else 0

            # 호가/시총 표시
            if ob_ratio > 0:
                ob_ratio_str = f"{ob_ratio:.3f}%"
            else:
                ob_ratio_str = "-"

            # 호가/시총 CSS (높을수록 관심 많은 종목)
            if ob_ratio >= 0.5:
                ob_ratio_cls = "ob-high"
            elif ob_ratio >= 0.1:
                ob_ratio_cls = "ob-mid"
            elif ob_ratio > 0:
                ob_ratio_cls = "ob-low"
            else:
                ob_ratio_cls = "ob-none"

            # [v18.1] 체결유형 배지 주석처리 (틱 모드 전용, 원복 시 해제)
            # if item["block_buy_count"] > 0 and item["block_sell_count"] == 0:
            #     presence = '<span class="buy-only-badge">매수Only</span>'
            #     presence_text = "매수Only"
            # elif item["block_sell_count"] > 0 and item["block_buy_count"] == 0:
            #     presence = '<span class="sell-only-badge">매도Only</span>'
            #     presence_text = "매도Only"
            # else:
            #     presence = '<span class="both-badge">양쪽</span>'
            #     presence_text = "양쪽"

            rows += (
                f'<tr data-ratio="{br:.2f}" data-change="{chg:.2f}" '
                f'data-net="{net}" data-direction="{dir_label}" '
                f'data-total-amt="{daily_vol}" data-mcap="{mcap}" '
                f'data-mcap-ratio="{mcap_ratio:.4f}" '
                f'data-ob-ratio="{ob_ratio:.4f}" '
                f'data-atr7pct="{item.get("atr7_pct", 0.0):.4f}" '
                f'data-h20="{item.get("close_vs_20d_high", 0.0):.1f}" '
                f'data-theme="{theme_str}">\n'
                f'<td>{i}</td>\n'
                f'<td class="code-cell">{code}</td>\n'
                f'<td class="name-cell">{item["name"]}{source_badge}</td>\n'
                f'<td class="amount-cell">{mcap:,}</td>\n'
                f'<td class="{chg_cls}">{chg_str}</td>\n'
                f'<td class="theme-cell">{theme_str}</td>\n'
                f'<td class="amount-cell col-hidden">{fmt_억(buy_amt)}</td>\n'
                f'<td class="amount-cell col-hidden">{fmt_억(sell_amt)}</td>\n'
                f'<td class="amount-cell {net_cls}">{fmt_억(net)}</td>\n'
                f'<td class="{ratio_cls} col-hidden">{br:.2f}%</td>\n'
                f'<td class="{mcap_ratio_cls}">{mcap_ratio:+.3f}%</td>\n'
                f'<td class="{dir_cls} col-hidden">{dir_label}</td>\n'
                + _atr7_td(item.get("atr7_pct", 0.0))
                + _triangle_tds(item) +
                (f'<td class="{ob_ratio_cls}" title="매도잔량: {ask_remain:,}주&#10;매수잔량: {bid_remain:,}주&#10;합계: {ask_remain+bid_remain:,}주&#10;호가금액: {ob_amount_억:.1f}억">{ob_ratio_str}</td>\n'
                   if table_id in ("individualTable", "limitTable") else '') +
                f'<td class="amount-cell">{fmt_억(daily_vol)}</td>\n'
                + _program_trade_td(item, table_id)
                + _est_eod_tds(item)
                + _tv_reversion_tds(item) +
                f'<td>{total_cnt}</td>\n'
                # f'<td>{presence}</td>\n'  # [v18.1] 체결유형 주석처리
                f'</tr>\n'
            )
        return rows

    stock_rows = make_table_rows(stocks, "individualTable")
    etf_rows = make_table_rows(etfs, "etfTable")

    # [v18.7] 5일 내 상/하한가 마감 종목 (개별주에서 중복 추출)
    limit_stocks = [r for r in stocks if r.get("limit_close_5d", False)]
    limit_rows = make_table_rows(limit_stocks, "limitTable")
    n_limit = len(limit_stocks)

    # ── 요약카드 CSS 클래스 ──
    def val_cls(v: int) -> str:
        return "value-positive" if v > 0 else "value-negative" if v < 0 else "value-neutral"

    # ── HTML 본문 ──
    # threshold_str = format_amount(BLOCK_TRADE_THRESHOLD)  # [v18.1] 폐지


    html = f"""<!DOCTYPE html>
<html><head>
<meta charset="UTF-8">
<title>대량체결 교차분석 - {run_time}</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@400;500;700&display=swap');

* {{ box-sizing: border-box; margin: 0; padding: 0; }}

body {{
    font-family: 'Noto Sans KR', 'Malgun Gothic', sans-serif;
    margin: 0;
    background: #0f1117;
    color: #e0e0e0;
}}

.container {{
    max-width: 1800px;
    margin: 0 auto;
    padding: 24px;
}}

h1 {{
    font-size: 22px;
    font-weight: 700;
    color: #fff;
    margin-bottom: 4px;
}}

.subtitle {{
    color: #8b8fa3;
    font-size: 13px;
    margin-bottom: 20px;
}}

/* Summary Cards */
.summary-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 12px;
    margin-bottom: 20px;
}}

.summary-card {{
    background: #1a1d29;
    border: 1px solid #2a2d3a;
    border-radius: 8px;
    padding: 14px 16px;
}}

.summary-card .label {{
    font-size: 11px;
    color: #8b8fa3;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-bottom: 4px;
}}

.summary-card .value {{
    font-size: 20px;
    font-weight: 700;
}}

.summary-card .sub {{
    font-size: 11px;
    color: #8b8fa3;
    margin-top: 2px;
}}

.value-positive {{ color: #ef5350; }}
.value-negative {{ color: #42a5f5; }}
.value-neutral {{ color: #e0e0e0; }}

/* Filter Bar */
.filter-bar {{
    background: #1a1d29;
    border: 1px solid #2a2d3a;
    border-radius: 8px;
    padding: 12px 16px;
    margin-bottom: 16px;
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 10px;
}}

.filter-bar input[type="text"] {{
    background: #0f1117;
    border: 1px solid #3a3d4a;
    border-radius: 4px;
    color: #e0e0e0;
    padding: 6px 12px;
    font-size: 12px;
    width: 180px;
}}

.filter-bar input[type="text"]::placeholder {{
    color: #5a5d6a;
}}

.filter-bar label {{
    font-size: 11px;
    color: #b0b3c0;
    cursor: pointer;
    white-space: nowrap;
    display: flex;
    align-items: center;
    gap: 4px;
}}

.filter-bar label:hover {{ color: #fff; }}

.filter-bar input[type="checkbox"] {{
    accent-color: #5c6bc0;
}}

.filter-bar button {{
    background: #2a2d3a;
    border: 1px solid #3a3d4a;
    border-radius: 4px;
    color: #b0b3c0;
    padding: 5px 14px;
    font-size: 11px;
    cursor: pointer;
}}

.filter-bar button:hover {{
    background: #3a3d4a;
    color: #fff;
}}

.filter-sep {{
    width: 1px;
    height: 24px;
    background: #3a3d4a;
}}

/* Section Tabs */
.section-tabs {{
    display: flex;
    gap: 0;
    margin-bottom: 0;
}}

.section-tab {{
    padding: 10px 24px;
    font-size: 13px;
    font-weight: 500;
    color: #8b8fa3;
    background: #1a1d29;
    border: 1px solid #2a2d3a;
    border-bottom: none;
    border-radius: 8px 8px 0 0;
    cursor: pointer;
    transition: all 0.15s;
}}

.section-tab:hover {{ color: #fff; }}

.section-tab.active {{
    color: #fff;
    background: #22252f;
    border-color: #5c6bc0;
    border-bottom: 2px solid #22252f;
}}

.section-content {{
    background: #22252f;
    border: 1px solid #2a2d3a;
    border-radius: 0 8px 8px 8px;
    padding: 0;
    display: none;
}}

.section-content.active {{
    display: block;
}}

/* Table */
table {{
    border-collapse: collapse;
    width: 100%;
    font-size: 12px;
}}

thead th {{
    background: #1a1d29;
    color: #b0b3c0;
    font-weight: 500;
    font-size: 11px;
    padding: 8px 6px;
    text-align: center;
    border-bottom: 2px solid #5c6bc0;
    position: sticky;
    top: 0;
    cursor: pointer;
    user-select: none;
    white-space: nowrap;
    z-index: 10;
}}

thead th:hover {{
    color: #fff;
    background: #252838;
}}

th.sorted-asc::after {{ content: ' ▲'; color: #5c6bc0; }}
th.sorted-desc::after {{ content: ' ▼'; color: #5c6bc0; }}

tbody td {{
    padding: 6px 8px;
    text-align: center;
    border-bottom: 1px solid #2a2d3a;
}}

tbody tr {{
    transition: background 0.1s;
}}

tbody tr:hover {{
    background: #2a2d3a !important;
}}

tbody tr:nth-child(even) {{
    background: #1e2130;
}}

.name-cell {{
    text-align: left;
    font-weight: 500;
    color: #fff;
    max-width: 140px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}}

.code-cell {{
    font-family: 'Consolas', monospace;
    font-size: 11px;
    color: #8b8fa3;
}}

.amount-cell {{
    text-align: right;
    font-family: 'Consolas', monospace;
}}

.theme-cell {{
    text-align: left;
    color: #7ee787;
    font-size: 11px;
    max-width: 350px;
    white-space: normal;
    word-break: keep-all;
    line-height: 1.4;
}}

.return-positive {{ color: #ef5350; font-weight: 700; }}
.return-negative {{ color: #42a5f5; font-weight: 700; }}

.ratio-strong-buy {{ color: #ef5350; font-weight: 700; background: rgba(239,83,80,0.1); }}
.ratio-buy {{ color: #ef5350; }}
.ratio-strong-sell {{ color: #42a5f5; font-weight: 700; background: rgba(66,165,245,0.1); }}
.ratio-sell {{ color: #42a5f5; }}

.net-positive {{ color: #ef5350; }}
.net-negative {{ color: #42a5f5; }}

.dir-strong-buy {{ color: #ef5350; font-weight: 700; }}
.dir-buy {{ color: #ef5350; }}
.dir-strong-sell {{ color: #42a5f5; font-weight: 700; }}
.dir-sell {{ color: #42a5f5; }}
.dir-neutral {{ color: #8b8fa3; }}

/* [v18.5] 호가/시총 비율 */
.ob-high {{ color: #ffd740; font-weight: 700; background: rgba(255,215,64,0.1); }}
.ob-mid {{ color: #ef5350; }}
.ob-low {{ color: #8b8fa3; }}
.ob-none {{ color: #484f58; }}
.atr-sweet {{ color: #4caf50; font-weight: 700; background: rgba(76,175,80,0.1); }}
.atr-high {{ color: #ef5350; }}
.atr-low {{ color: #8b8fa3; }}
.tri-reached {{ color: #4caf50; font-weight: 700; background: rgba(76,175,80,0.15); }}
.tri-imminent {{ color: #66bb6a; font-weight: 600; background: rgba(102,187,106,0.1); }}
.tri-near {{ color: #ffa726; font-weight: 600; }}
.tri-na {{ color: #484f58; }}
.col-hidden {{ display: none !important; }}

/* [v18.8] 즉시 표시 커스텀 툴팁 */
#custom-tooltip {{
    position: absolute;
    display: none;
    background: rgba(20, 25, 35, 0.97);
    color: #e6edf3;
    border: 1px solid #4a5568;
    padding: 8px 12px;
    border-radius: 4px;
    font-size: 12px;
    line-height: 1.5;
    white-space: pre-line;
    pointer-events: none;
    z-index: 10000;
    max-width: 450px;
    box-shadow: 0 4px 12px rgba(0,0,0,0.6);
}}

/* [v18.6] 예상마감대금 비교 + 20일고가대비 */
.est-positive {{ color: #ef5350; }}
.est-negative {{ color: #42a5f5; }}
.est-strong-pos {{ color: #ef5350; font-weight: 700; background: rgba(239,83,80,0.08); }}
.est-strong-neg {{ color: #42a5f5; font-weight: 700; background: rgba(66,165,245,0.08); }}
.est-na {{ color: #484f58; }}
/* [v18.9] 거래대금 회귀 스크리닝 컬럼 */
.tv-sufficient {{ color: #69b578; }}
.tv-low        {{ color: #b0bec5; }}
.tv-mid        {{ color: #ffa726; font-weight: 600; }}
.tv-high       {{ color: #ef5350; font-weight: 700; background: rgba(239,83,80,0.07); }}
.tv-today      {{ color: #69b578; }}
.tv-2days      {{ color: #ffa726; }}
.tv-long       {{ color: #b0bec5; }}

.sell-only-badge {{
    background: #1a237e;
    color: #64b5f6;
    padding: 2px 6px;
    border-radius: 3px;
    font-size: 10px;
    font-weight: 500;
}}

.buy-only-badge {{
    background: #4a1212;
    color: #ef9a9a;
    padding: 2px 6px;
    border-radius: 3px;
    font-size: 10px;
    font-weight: 500;
}}

.both-badge {{
    background: #2a2d3a;
    color: #b0b3c0;
    padding: 2px 6px;
    border-radius: 3px;
    font-size: 10px;
}}

.source-badge {{
    padding: 1px 4px;
    border-radius: 3px;
    font-size: 9px;
    font-weight: 600;
    margin-left: 4px;
    vertical-align: middle;
}}
.source-badge.surge {{
    background: #ff6f00;
    color: #fff;
}}
.source-badge.vol-top {{
    background: #2e7d32;
    color: #c8e6c9;
}}

.info-box {{
    background: #1a1d29;
    border: 1px solid #2a2d3a;
    border-left: 3px solid #5c6bc0;
    border-radius: 4px;
    padding: 12px 16px;
    margin-bottom: 16px;
    font-size: 12px;
    color: #b0b3c0;
    line-height: 1.6;
}}

.info-box strong {{
    color: #e0e0e0;
}}

.direction-bar {{
    display: flex;
    height: 28px;
    border-radius: 4px;
    overflow: hidden;
    margin: 8px 0;
}}

.direction-bar .seg {{
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 10px;
    font-weight: 500;
    color: #fff;
    min-width: 30px;
}}

.seg-strong-buy {{ background: #c62828; }}
.seg-buy {{ background: #ef5350; }}
.seg-neutral {{ background: #546e7a; }}
.seg-sell {{ background: #42a5f5; }}
.seg-strong-sell {{ background: #1565c0; }}

.theme-legend-item {{
    display: inline-flex;
    align-items: center;
    margin-right: 10px;
    font-size: 11px;
    white-space: nowrap;
    cursor: pointer;
    padding: 2px 6px;
    border-radius: 4px;
    transition: background 0.15s;
}}
.theme-legend-item:hover {{
    background: rgba(255,255,255,0.12);
}}
.theme-legend-item.active {{
    background: rgba(187,134,252,0.25);
    outline: 1.5px solid #bb86fc;
}}

.footer {{
    text-align: center;
    color: #484f58;
    font-size: 11px;
    margin-top: 20px;
    padding-top: 10px;
    border-top: 1px solid #2a2d3a;
}}

/* ── Report Navigation ── */
.report-nav {{
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 6px 12px;
    margin-bottom: 10px;
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 6px;
    font-size: 13px;
}}
.report-nav .nav-arrow {{
    color: #58a6ff;
    text-decoration: none;
    padding: 4px 10px;
    border-radius: 4px;
    transition: background 0.15s;
    user-select: none;
}}
.report-nav .nav-arrow:hover {{
    background: #21262d;
}}
.report-nav .nav-placeholder {{
    visibility: hidden;
    padding: 4px 10px;
}}
.report-nav .nav-current {{
    color: #8b949e;
    font-size: 12px;
}}

@media (max-width: 1200px) {{
    .container {{ padding: 12px; }}
    table {{ font-size: 11px; }}
}}

/* [v18.20] 프로그램순매수% 셀 색상 + 모달 */
.pt-cell {{ cursor: pointer; font-weight: 600; }}
.pt-cell:hover {{ filter: brightness(1.4); text-decoration: underline; }}
.pt-na {{ color: #484f58; }}
/* 양수(빨강 계열, 한국 관습 = 매수) */
.pt-pos-weak       {{ color: #f85149; background: rgba(248,81,73,0.08); }}
.pt-pos-strong     {{ color: #ff6b61; background: rgba(248,81,73,0.18); }}
.pt-pos-very-strong{{ color: #ffffff; background: rgba(248,81,73,0.55); }}
/* 음수(파랑 계열, 한국 관습 = 매도) */
.pt-neg-weak       {{ color: #58a6ff; background: rgba(88,166,255,0.08); }}
.pt-neg-strong     {{ color: #7cb9ff; background: rgba(88,166,255,0.18); }}
.pt-neg-very-strong{{ color: #ffffff; background: rgba(88,166,255,0.55); }}

/* 모달 */
.pt-modal-backdrop {{
    display: none;
    position: fixed;
    inset: 0;
    background: rgba(0,0,0,0.78);
    z-index: 9999;
    align-items: center;
    justify-content: center;
    padding: 20px;
}}
.pt-modal-backdrop.show {{ display: flex; }}
.pt-modal {{
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 8px;
    width: 100%;
    max-width: 1200px;
    max-height: 90vh;
    display: flex;
    flex-direction: column;
    overflow: hidden;
    box-shadow: 0 8px 40px rgba(0,0,0,0.6);
}}
.pt-modal-header {{
    padding: 14px 20px;
    border-bottom: 1px solid #30363d;
    display: flex;
    justify-content: space-between;
    align-items: center;
    flex-shrink: 0;
}}
.pt-modal-title {{ font-size: 16px; font-weight: 600; color: #c9d1d9; }}
.pt-modal-meta  {{ font-size: 12px; color: #8b949e; margin-top: 4px; }}
.pt-modal-close {{
    background: none;
    border: none;
    color: #8b949e;
    font-size: 24px;
    cursor: pointer;
    padding: 0 8px;
    line-height: 1;
}}
.pt-modal-close:hover {{ color: #f85149; }}

.pt-modal-body {{
    overflow-y: auto;
    overflow-x: hidden;
    padding: 14px 20px;
    flex: 1;
}}
.pt-chart-wrap {{
    height: 320px;
    margin-bottom: 6px;
    background: #0d1117;
    border: 1px solid #30363d;
    border-radius: 4px;
    padding: 8px;
}}
.pt-scroll-wrap {{
    display: none;           /* JS가 줌 상태일 때만 flex로 변경 */
    align-items: center;
    gap: 8px;
    margin-bottom: 10px;
    padding: 0 8px;
}}
.pt-scroll-label {{
    font-size: 11px;
    color: #8b949e;
    white-space: nowrap;
    flex-shrink: 0;
}}
.pt-scroll-range {{
    flex: 1;
    height: 6px;
    -webkit-appearance: none;
    appearance: none;
    background: #21262d;
    border-radius: 3px;
    outline: none;
    cursor: pointer;
}}
.pt-scroll-range::-webkit-slider-thumb {{
    -webkit-appearance: none;
    width: 16px;
    height: 16px;
    background: #bb86fc;
    border-radius: 50%;
    cursor: grab;
}}
.pt-scroll-range::-moz-range-thumb {{
    width: 16px;
    height: 16px;
    background: #bb86fc;
    border-radius: 50%;
    border: none;
    cursor: grab;
}}
.pt-table-wrap {{
    overflow-x: auto;
    overflow-y: auto;
    max-height: 360px;
    border: 1px solid #30363d;
    border-radius: 4px;
}}
.pt-table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 12px;
    color: #c9d1d9;
}}
.pt-table th {{
    background: #21262d;
    padding: 6px 10px;
    text-align: right;
    border-bottom: 1px solid #30363d;
    position: sticky;
    top: 0;
    white-space: nowrap;
    font-weight: 600;
}}
.pt-table th:first-child, .pt-table td:first-child {{ text-align: center; }}
.pt-table td {{
    padding: 4px 10px;
    text-align: right;
    border-bottom: 1px solid #21262d;
    white-space: nowrap;
    font-variant-numeric: tabular-nums;
}}
.pt-table tr:hover td {{ background: #1c2128; }}
.pt-num-pos {{ color: #f85149; }}
.pt-num-neg {{ color: #58a6ff; }}
.pt-modal-footer {{
    padding: 8px 20px;
    border-top: 1px solid #30363d;
    color: #6e7681;
    font-size: 11px;
    flex-shrink: 0;
}}
</style>
</head>
<body>
<div class="container">

<!-- Report Navigation -->
<div class="report-nav">
    __NAV_PREV_SLOT__
    <span class="nav-current">__NAV_INDEX_SLOT__ · {run_time}</span>
    __NAV_NEXT_SLOT__
</div>

<h1>📊 대량체결 교차분석</h1>
<div class="subtitle">
    분석 시각: {run_time} &nbsp;|&nbsp;
    CYBOS Plus 1분봉 (호가비교 방식) &nbsp;|&nbsp;
    분석모드: 1분봉 전체합산 &nbsp;|&nbsp;
    분석: {total_stocks_analyzed}종목 ({source_info}) &nbsp;|&nbsp;
    소요: {elapsed_sec:.1f}초
</div>

<!-- Summary Cards -->
<div class="summary-grid">
    <div class="summary-card">
        <div class="label">개별주 순매수</div>
        <div class="value {val_cls(stock_net)}">{fmt_억(stock_net)}억</div>
        <div class="sub">매수 {fmt_억(stock_buy)}억 / 매도 {fmt_억(stock_sell)}억</div>
    </div>
    <div class="summary-card">
        <div class="label">ETF 순매수</div>
        <div class="value {val_cls(etf_net)}">{fmt_억(etf_net)}억</div>
        <div class="sub">매수 {fmt_억(etf_buy)}억 / 매도 {fmt_억(etf_sell)}억</div>
    </div>
    <div class="summary-card">
        <div class="label">전체 순매수</div>
        <div class="value {val_cls(all_net)}">{fmt_억(all_net)}억</div>
        <div class="sub">매수 {fmt_억(all_buy)}억 / 매도 {fmt_억(all_sell)}억</div>
    </div>
    <div class="summary-card">
        <div class="label">개별주 종목 수</div>
        <div class="value value-neutral">{n_stocks}</div>
        <div class="sub">상승매수 {bullish_count} / 하락매도 {bearish_count}</div>
    </div>
    <div class="summary-card">
        <div class="label">ETF/ETN 종목 수</div>
        <div class="value value-neutral">{len(etfs)}</div>
        <div class="sub">레버리지/인버스 수급 방향 확인</div>
    </div>
</div>

<!-- Direction Distribution -->
<div class="info-box">
    <strong>방향성 분포 (개별주 {n_stocks}종목):</strong>
    {direction_bar}
    <span style="font-size:11px;">
        🟢강한매수(70%+) &nbsp; 🔵매수우위(60~70%) &nbsp; ⚪균형(40~60%) &nbsp; 🟠매도우위(30~40%) &nbsp; 🔴강한매도(~30%)
    </span>
</div>

<!-- Theme Distribution -->
<div class="info-box" style="border-left-color: #7c4dff;">
    <strong>테마별 분포 (개별주 {n_stocks}종목):</strong>
    {theme_bar_html}
    {theme_legend_html}
</div>

{hot_cold_html}

<!-- Usage Guide -->
<div class="info-box" style="border-left-color: #ff9800;">
    <strong>📖 읽는 법:</strong><br>
    <strong>매수/매도 구분</strong> = 호가비교 방식 (체결가=매도호가 → 매수체결, 체결가=매수호가 → 매도체결)<br>
    <strong>분석방식</strong> = 1분봉 전체 합산 (개별 건 threshold 없음, 전체 수급 흐름 파악)<br>
    <strong>ATR(7)%</strong> = 1분봉 ATR(7) / 종가 × 100. 스위트스팟 0.5~1.0% (녹색 표시)<br>
    <strong>타점(2x)</strong> = 삼각형 패턴 P(당일고점) - 2×X. X = P - C(∩ 왼쪽 저점). 마우스 올리면 1.5x 정보 표시<br>
    <strong>남은시간</strong> = 현재가→타점(2x) 이론 도달시간 = (거리%/ATR7%)² 분. "도달" = 이미 타점 이하<br>
    <strong>거래대금대비 매수비율</strong> = 매수금액 / (매수금액+매도금액) × 100<br>
    <strong>시총대비 순매수비율</strong> = 당일 누적순매수금액 / 시가총액 × 100 (시총 대비 수급 강도)<br>
    <strong>매수비율 60%+</strong> = 매수우세 &nbsp;|&nbsp;
    <strong>매수비율 40%-</strong> = 매도우세<br>
    <strong>순매수 양수</strong> = 순수 자금 유입 &nbsp;|&nbsp;
    <strong>순매수 음수</strong> = 순수 자금 이탈<br>
    <strong>호가/시총</strong> = (매도10호가잔량 + 매수10호가잔량) / 상장주식수 × 100 (시총 대비 호가창 관심도)
</div>

<!-- Filter Bar -->
<div class="filter-bar">
    <input type="text" id="searchInput" placeholder="종목명/코드 검색..." onkeyup="filterTable()">
    <div class="filter-sep"></div>
    <label><input type="checkbox" id="fStrongBuy" onchange="filterTable()"> 🟢강한매수</label>
    <label><input type="checkbox" id="fBuy" onchange="filterTable()"> 🔵매수우위+</label>
    <label><input type="checkbox" id="fStrongSell" onchange="filterTable()"> 🔴강한매도</label>
    <label><input type="checkbox" id="fSell" onchange="filterTable()"> 🟠매도우위+</label>
    <div class="filter-sep"></div>
    <label><input type="checkbox" id="fAtrSweet" onchange="filterTable()" title="ATR(7)% 0.5~1.0 스위트스팟 종목만 표시"> ATR(7)% 0.5~1</label>
    <div class="filter-sep"></div>
    <!-- [v18.1] 체결유형 필터 주석처리 (틱 모드 전용)
    <label><input type="checkbox" id="fBuyOnly" onchange="filterTable()"> 매수Only</label>
    <label><input type="checkbox" id="fSellOnly" onchange="filterTable()"> 매도Only</label>
    <label><input type="checkbox" id="fBoth" onchange="filterTable()"> 양쪽활발</label>
    -->
    <div class="filter-sep"></div>
    <label><input type="checkbox" id="fMin5" onchange="filterTable()"> 거래대금5억+</label>
    <label><input type="checkbox" id="fMin10" onchange="filterTable()"> 거래대금10억+</label>
    <div class="filter-sep"></div>
    <label><input type="checkbox" id="fMcap2000" onchange="filterTable()"> 시총2000억+</label>
    <div class="filter-sep"></div>
    <label><input type="checkbox" id="fObHigh" onchange="filterTable()" title="호가/시총 비율 0.1% 이상 종목만 표시"> 호가/시총 상위</label>
    <div class="filter-sep"></div>
    <label><input type="checkbox" id="fH20_5" onchange="filterTable()" title="20일 고가 대비 -5% 이내 종목만 표시"> 20일고가 -5%이내</label>
    <label><input type="checkbox" id="fH20_10" onchange="filterTable()" title="20일 고가 대비 -10% 이내 종목만 표시"> -10%이내</label>
    <label><input type="checkbox" id="fH20_15" onchange="filterTable()" title="20일 고가 대비 -15% 이내 종목만 표시"> -15%이내</label>
    <div class="filter-sep"></div>
    <span id="themeFilterLabel" style="font-size:11px; color:#bb86fc; cursor:pointer; display:none;" onclick="clearThemeFilter()">🏷️ <span id="themeFilterName"></span> ✕</span>
    <div class="filter-sep"></div>
    <button onclick="resetFilter()">초기화</button>
    <span id="rowCountDisplay" style="margin-left:auto; font-size:11px; color:#8b8fa3;"></span>
</div>

<!-- Section Tabs -->
<div class="section-tabs">
    <div class="section-tab active" onclick="switchTab('limit', this)">
        🔥 5일내상/하한가 ({n_limit})
    </div>
    <div class="section-tab" onclick="switchTab('individual', this)">
        📋 개별주 ({n_stocks})
    </div>
    <div class="section-tab" onclick="switchTab('etf', this)">
        📦 ETF/ETN ({len(etfs)})
    </div>
</div>

<!-- Individual Stocks Section -->
<div id="section-individual" class="section-content">
<table id="individualTable" class="data-table">
<thead><tr>
<th onclick="sortTable('individualTable', 0)">순위</th>
<th onclick="sortTable('individualTable', 1)">코드</th>
<th onclick="sortTable('individualTable', 2)">종목명</th>
<th onclick="sortTable('individualTable', 3)">시총<br>(억)</th>
<th onclick="sortTable('individualTable', 4)">등락률</th>
<th onclick="sortTable('individualTable', 5)">테마</th>
<th onclick="sortTable('individualTable', 6)" class="col-hidden">매수<br>금액(억)</th>
<th onclick="sortTable('individualTable', 7)" class="col-hidden">매도<br>금액(억)</th>
<th onclick="sortTable('individualTable', 8)">순매수<br>(억)</th>
<th onclick="sortTable('individualTable', 9)" class="col-hidden">거래대금대비<br>매수비율</th>
<th onclick="sortTable('individualTable', 10)">시총대비<br>순매수비율</th>
<th onclick="sortTable('individualTable', 11)" class="col-hidden">방향성</th>
<th onclick="sortTable('individualTable', 12)" title="1분봉 ATR(7) / 종가 × 100&#10;스위트스팟: 0.5~1.0%">ATR(7)%</th>
<th onclick="sortTable('individualTable', 13)" title="삼각형 패턴 2x 매수타점 가격&#10;P(고점) - 2×X&#10;X = P - C(중심점)&#10;&#10;마우스 올리면 1.5x 타점, P, C, X 표시" class="col-hidden">타점<br>(2x)</th>
<th onclick="sortTable('individualTable', 14)" title="현재가에서 2x 타점까지&#10;이론상 도달 소요 시간 (분)&#10;= (거리% / ATR7%)² 분&#10;&#10;마우스 올리면 1.5x 남은시간 표시" class="col-hidden">남은<br>시간</th>
<th onclick="sortTable('individualTable', 15)" title="(총매도호가잔량 + 총매수호가잔량) / 총상장주식수 × 100&#10;시가총액 대비 현재 호가창에 걸린 금액 비율&#10;높을수록 시장의 관심이 많은 종목&#10;&#10;(마우스 올리면 매도/매수 잔량 표시)">호가<br>/시총</th>
<th onclick="sortTable('individualTable', 16)">당일거래<br>대금(억)</th>
<th onclick="sortTable('individualTable', 17)" title="키움 ka90008 기준&#10;(프로그램순매수수량 / 누적거래량) × 100&#10;&#10;양수(빨강): 프로그램 순매수 우위&#10;음수(파랑): 프로그램 순매도 우위&#10;|값|≥10%: 매우 강함&#10;|값|≥5%: 강함&#10;|값|<5%: 보통&#10;-: 데이터 없음&#10;&#10;⚡ 클릭하면 시간대별 추이 차트 팝업">프로그램<br>순매수%</th>
<th onclick="sortTable('individualTable', 18)" title="전일 동시각까지의 누적거래대금 vs 당일 누적거래대금&#10;barTradeVal = (H+L)/2 × V 기준 (PineScript 동일 로직)&#10;&#10;≥100%: 전일보다 수급 강함 (빨강)&#10;&lt;100%: 전일보다 수급 약함 (파랑)&#10;데이터 없음: -">전일<br>동시간대비</th>
<th onclick="sortTable('individualTable', 19)" title="(예상마감대금 ÷ 시가총액) × 100&#10;삼성전자 등 대형주 왜곡 없이 비교 가능&#10;마우스 올리면 예상마감대금(억) 표시&#10;&#10;≥5%: 매우 강한 수급 (빨강)&#10;2~5%: 강한 수급 (파랑)&#10;0.5~2%: 보통&#10;<0.5%: 수급 미미 (회색)">시총대비<br>예상마감</th>
<th onclick="sortTable('individualTable', 20)" title="예상마감대금 = 누적거래대금 × (390분 / 경과분)&#10;전일 총 거래대금 대비 몇 % 차이인지 표시" class="col-hidden">예상대비<br>D-1</th>
<th onclick="sortTable('individualTable', 21)" title="예상마감대금 vs 전전일 총 거래대금" class="col-hidden">예상대비<br>D-2</th>
<th onclick="sortTable('individualTable', 22)" title="현재가 / 최근 20거래일 최고 고가 - 1&#10;0%면 20일 신고가, 마이너스면 고점 대비 하락">20일고가<br>대비</th>
<th onclick="sortTable('individualTable', 23)" title="[테스트용] 거래대금 이탈도&#10;= (20일 기준대금 - 예상마감대금) / σ&#10;클수록 평균 대비 거래대금 회복 여지 큼&#10;기준대금 = max(20일 중앙값, 20일 평균)&#10;&#10;색상: 회색=충분, 연두=&lt;1σ, 주황=1~2σ, 빨강=&gt;2σ" class="col-hidden">이탈도<br>(σ) ⚗</th>
<th onclick="sortTable('individualTable', 24)" title="[테스트용] OU 직관 차용 휴리스틱 — 거래대금 회복 예상 시간&#10;= (이탈도 / θ) × 390분&#10;θ ≈ 1 - β (AR(1) 자기상관계수의 1차 근사)&#10;⚠️ 엄밀한 OU 도출식 아님 — β가 낮을수록 오차 커짐&#10;&#10;백테스팅 미검증 — 참고용만" class="col-hidden">회복<br>예상 ⚗</th>
<th onclick="sortTable('individualTable', 25)">총봉수</th>
</tr></thead>
<tbody>
{stock_rows if stock_rows else '<tr><td colspan="26" style="color:#484f58;">대량체결 내역이 없습니다</td></tr>'}
</tbody>
</table>
</div>

<!-- ETF Section -->
<div id="section-etf" class="section-content">
<table id="etfTable" class="data-table">
<thead><tr>
<th onclick="sortTable('etfTable', 0)">순위</th>
<th onclick="sortTable('etfTable', 1)">코드</th>
<th onclick="sortTable('etfTable', 2)">종목명</th>
<th onclick="sortTable('etfTable', 3)">시총<br>(억)</th>
<th onclick="sortTable('etfTable', 4)">등락률</th>
<th onclick="sortTable('etfTable', 5)">테마</th>
<th onclick="sortTable('etfTable', 6)" class="col-hidden">매수<br>금액(억)</th>
<th onclick="sortTable('etfTable', 7)" class="col-hidden">매도<br>금액(억)</th>
<th onclick="sortTable('etfTable', 8)">순매수<br>(억)</th>
<th onclick="sortTable('etfTable', 9)" class="col-hidden">거래대금대비<br>매수비율</th>
<th onclick="sortTable('etfTable', 10)">시총대비<br>순매수비율</th>
<th onclick="sortTable('etfTable', 11)" class="col-hidden">방향성</th>
<th onclick="sortTable('etfTable', 12)" title="1분봉 ATR(7) / 종가 × 100">ATR(7)%</th>
<th onclick="sortTable('etfTable', 13)" title="삼각형 패턴 2x 매수타점 가격" class="col-hidden">타점<br>(2x)</th>
<th onclick="sortTable('etfTable', 14)" title="현재가에서 2x 타점까지 이론상 소요 시간" class="col-hidden">남은<br>시간</th>
<th onclick="sortTable('etfTable', 15)">당일거래<br>대금(억)</th>
<th onclick="sortTable('etfTable', 16)" title="전일 동시각까지의 누적거래대금 vs 당일 누적거래대금&#10;barTradeVal = (H+L)/2 × V 기준 (PineScript 동일 로직)&#10;&#10;≥100%: 전일보다 수급 강함 (빨강)&#10;&lt;100%: 전일보다 수급 약함 (파랑)&#10;데이터 없음: -">전일<br>동시간대비</th>
<th onclick="sortTable('etfTable', 17)" title="예상마감대금 vs 전일 총 거래대금" class="col-hidden">예상대비<br>D-1</th>
<th onclick="sortTable('etfTable', 18)" title="예상마감대금 vs 전전일 총 거래대금" class="col-hidden">예상대비<br>D-2</th>
<th onclick="sortTable('etfTable', 19)" title="현재가 / 최근 20거래일 최고 고가 - 1">20일고가<br>대비</th>
<th onclick="sortTable('etfTable', 20)" title="[테스트용] 거래대금 이탈도 (20일 기준대금 대비)" class="col-hidden">이탈도<br>(σ) ⚗</th>
<th onclick="sortTable('etfTable', 21)" title="[테스트용] OU 직관 차용 휴리스틱 — 거래대금 회복 예상 시간 (백테스팅 미검증)" class="col-hidden">회복<br>예상 ⚗</th>
<th onclick="sortTable('etfTable', 22)">총봉수</th>
</tr></thead>
<tbody>
{etf_rows if etf_rows else '<tr><td colspan="23" style="color:#484f58;">대량체결 내역이 없습니다</td></tr>'}
</tbody>
</table>
</div>

<!-- Limit Close Section (5일내 상/하한가) -->
<div id="section-limit" class="section-content active">
<table id="limitTable" class="data-table">
<thead><tr>
<th onclick="sortTable('limitTable', 0)">순위</th>
<th onclick="sortTable('limitTable', 1)">코드</th>
<th onclick="sortTable('limitTable', 2)">종목명</th>
<th onclick="sortTable('limitTable', 3)">시총<br>(억)</th>
<th onclick="sortTable('limitTable', 4)">등락률</th>
<th onclick="sortTable('limitTable', 5)">테마</th>
<th onclick="sortTable('limitTable', 6)" class="col-hidden">매수<br>금액(억)</th>
<th onclick="sortTable('limitTable', 7)" class="col-hidden">매도<br>금액(억)</th>
<th onclick="sortTable('limitTable', 8)">순매수<br>(억)</th>
<th onclick="sortTable('limitTable', 9)" class="col-hidden">거래대금대비<br>매수비율</th>
<th onclick="sortTable('limitTable', 10)">시총대비<br>순매수비율</th>
<th onclick="sortTable('limitTable', 11)" class="col-hidden">방향성</th>
<th onclick="sortTable('limitTable', 12)" title="1분봉 ATR(7) / 종가 × 100&#10;스위트스팟: 0.5~1.0%">ATR(7)%</th>
<th onclick="sortTable('limitTable', 13)" title="삼각형 패턴 2x 매수타점 가격" class="col-hidden">타점<br>(2x)</th>
<th onclick="sortTable('limitTable', 14)" title="현재가에서 2x 타점까지 이론상 소요 시간" class="col-hidden">남은<br>시간</th>
<th onclick="sortTable('limitTable', 15)" title="(총매도호가잔량 + 총매수호가잔량) / 총상장주식수 × 100&#10;시가총액 대비 현재 호가창에 걸린 금액 비율&#10;높을수록 시장의 관심이 많은 종목&#10;&#10;(마우스 올리면 매도/매수 잔량 표시)">호가<br>/시총</th>
<th onclick="sortTable('limitTable', 16)">당일거래<br>대금(억)</th>
<th onclick="sortTable('limitTable', 17)" title="키움 ka90008 기준&#10;(프로그램순매수수량 / 누적거래량) × 100&#10;&#10;양수(빨강): 프로그램 순매수 우위&#10;음수(파랑): 프로그램 순매도 우위&#10;|값|≥10%: 매우 강함&#10;|값|≥5%: 강함&#10;|값|<5%: 보통&#10;-: 데이터 없음&#10;&#10;⚡ 클릭하면 시간대별 추이 차트 팝업">프로그램<br>순매수%</th>
<th onclick="sortTable('limitTable', 18)" title="전일 동시각까지의 누적거래대금 vs 당일 누적거래대금&#10;barTradeVal = (H+L)/2 × V 기준 (PineScript 동일 로직)&#10;&#10;≥100%: 전일보다 수급 강함 (빨강)&#10;&lt;100%: 전일보다 수급 약함 (파랑)&#10;데이터 없음: -">전일<br>동시간대비</th>
<th onclick="sortTable('limitTable', 19)" title="(예상마감대금 ÷ 시가총액) × 100&#10;삼성전자 등 대형주 왜곡 없이 비교 가능&#10;마우스 올리면 예상마감대금(억) 표시&#10;&#10;≥5%: 매우 강한 수급 (빨강)&#10;2~5%: 강한 수급 (파랑)&#10;0.5~2%: 보통&#10;<0.5%: 수급 미미 (회색)">시총대비<br>예상마감</th>
<th onclick="sortTable('limitTable', 20)" title="예상마감대금 = 누적거래대금 × (390분 / 경과분)&#10;전일 총 거래대금 대비 몇 % 차이인지 표시" class="col-hidden">예상대비<br>D-1</th>
<th onclick="sortTable('limitTable', 21)" title="예상마감대금 vs 전전일 총 거래대금" class="col-hidden">예상대비<br>D-2</th>
<th onclick="sortTable('limitTable', 22)" title="현재가 / 최근 20거래일 최고 고가 - 1&#10;0%면 20일 신고가, 마이너스면 고점 대비 하락">20일고가<br>대비</th>
<th onclick="sortTable('limitTable', 23)" title="[테스트용] 거래대금 이탈도 (20일 기준대금 대비)" class="col-hidden">이탈도<br>(σ) ⚗</th>
<th onclick="sortTable('limitTable', 24)" title="[테스트용] OU 직관 차용 휴리스틱 — 거래대금 회복 예상 시간 (백테스팅 미검증)" class="col-hidden">회복<br>예상 ⚗</th>
<th onclick="sortTable('limitTable', 25)">총봉수</th>
</tr></thead>
<tbody>
{limit_rows if limit_rows else '<tr><td colspan="26" style="color:#484f58;">5거래일 내 상/하한가 마감 종목이 없습니다</td></tr>'}
</tbody>
</table>
</div>

<div class="footer">
    수급 교차분석 v18.20.1 (CYBOS Plus 1분봉 호가비교 + 키움 프로그램매매) |
    분석모드: 1분봉 전체합산 |
    Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
</div>

</div><!-- container -->

<script>
// [v18.8] 즉시 표시 커스텀 툴팁 (브라우저 기본 툴팁 딜레이 우회)
(function() {{
    var tip = null;
    function ensureTip() {{
        if (!tip) {{
            tip = document.createElement('div');
            tip.id = 'custom-tooltip';
            document.body.appendChild(tip);
        }}
        return tip;
    }}
    document.addEventListener('mouseover', function(e) {{
        var target = e.target.closest('[title], [data-tooltip]');
        if (!target) return;
        // title 속성을 data-tooltip으로 즉석 이동 (브라우저 기본 툴팁 차단)
        if (target.hasAttribute('title')) {{
            var t = target.getAttribute('title');
            target.setAttribute('data-tooltip', t);
            target.removeAttribute('title');
        }}
        var txt = target.getAttribute('data-tooltip');
        if (!txt) return;
        var el = ensureTip();
        el.textContent = txt;
        el.style.display = 'block';
        el.style.left = (e.pageX + 14) + 'px';
        el.style.top = (e.pageY + 14) + 'px';
    }});
    document.addEventListener('mousemove', function(e) {{
        if (tip && tip.style.display === 'block') {{
            var tipW = tip.offsetWidth;
            var tipH = tip.offsetHeight;
            var vpRight = window.scrollX + window.innerWidth;
            var vpBottom = window.scrollY + window.innerHeight;
            var OFFSET = 14;
            var MARGIN = 10;

            // X: 오른쪽 공간이 충분하면 커서 오른쪽, 부족하면 커서 왼쪽
            var xRight = e.pageX + OFFSET;
            var xLeft  = e.pageX - OFFSET - tipW;
            var x = (xRight + tipW + MARGIN <= vpRight) ? xRight : Math.max(window.scrollX + MARGIN, xLeft);

            // Y: 아래 공간이 부족하면 커서 위로
            var yDown = e.pageY + OFFSET;
            var yUp   = e.pageY - OFFSET - tipH;
            var y = (yDown + tipH + MARGIN <= vpBottom) ? yDown : Math.max(window.scrollY + MARGIN, yUp);

            tip.style.left = x + 'px';
            tip.style.top  = y + 'px';
        }}
    }});
    document.addEventListener('mouseout', function(e) {{
        var target = e.target.closest('[data-tooltip]');
        if (target && tip) tip.style.display = 'none';
    }});
}})();

// Tab switching
function switchTab(tabId, el) {{
    document.querySelectorAll('.section-tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.section-content').forEach(s => s.classList.remove('active'));
    el.classList.add('active');
    document.getElementById('section-' + tabId).classList.add('active');
    updateRowCount();
}}

// Sorting
let sortStates = {{}};

function sortTable(tableId, colIdx) {{
    const table = document.getElementById(tableId);
    const tbody = table.querySelector('tbody');
    const rows = Array.from(tbody.querySelectorAll('tr'));
    const ths = table.querySelectorAll('thead th');

    const key = tableId + '_' + colIdx;
    const currentDir = sortStates[key] || 'none';
    const newDir = currentDir === 'asc' ? 'desc' : 'asc';

    ths.forEach(th => {{
        th.classList.remove('sorted-asc', 'sorted-desc');
    }});
    ths[colIdx].classList.add('sorted-' + newDir);

    Object.keys(sortStates).forEach(k => {{
        if (k.startsWith(tableId + '_') && k !== key) delete sortStates[k];
    }});
    sortStates[key] = newDir;

    rows.sort((a, b) => {{
        const cellA = a.cells[colIdx];
        const cellB = b.cells[colIdx];
        let aVal = cellA.getAttribute('data-sort-val') || cellA.textContent.trim();
        let bVal = cellB.getAttribute('data-sort-val') || cellB.textContent.trim();

        let aNum = parseFloat(aVal.replace(/[,%억+~분]/g, ''));
        let bNum = parseFloat(bVal.replace(/[,%억+~분]/g, ''));

        if (!isNaN(aNum) && !isNaN(bNum)) {{
            return newDir === 'asc' ? aNum - bNum : bNum - aNum;
        }}
        return newDir === 'asc' ? aVal.localeCompare(bVal, 'ko') : bVal.localeCompare(aVal, 'ko');
    }});

    rows.forEach(r => tbody.appendChild(r));
}}

// Filtering
let activeThemeFilter = '';

function toggleThemeFilter(themeName) {{
    // 범례 하이라이트 초기화
    document.querySelectorAll('.theme-legend-item').forEach(el => el.classList.remove('active'));

    if (activeThemeFilter === themeName) {{
        clearThemeFilter();
    }} else {{
        activeThemeFilter = themeName;
        document.getElementById('themeFilterLabel').style.display = 'inline';
        document.getElementById('themeFilterName').textContent = themeName;
        // 클릭한 범례 하이라이트
        document.querySelectorAll('.theme-legend-item').forEach(el => {{
            if (el.textContent.startsWith(themeName + '(')) el.classList.add('active');
        }});
        filterTable();
    }}
}}

function clearThemeFilter() {{
    activeThemeFilter = '';
    document.getElementById('themeFilterLabel').style.display = 'none';
    document.getElementById('themeFilterName').textContent = '';
    document.querySelectorAll('.theme-legend-item').forEach(el => el.classList.remove('active'));
    filterTable();
}}

function filterTable() {{
    const search = document.getElementById('searchInput').value.toLowerCase();
    const fStrongBuy = document.getElementById('fStrongBuy').checked;
    const fBuy = document.getElementById('fBuy').checked;
    const fStrongSell = document.getElementById('fStrongSell').checked;
    const fSell = document.getElementById('fSell').checked;
    const fAtrSweet = document.getElementById('fAtrSweet').checked;
    // [v18.1] 체결유형 필터 비활성화 (틱 모드 전용)
    const fBuyOnly = false;  // document.getElementById('fBuyOnly').checked;
    const fSellOnly = false; // document.getElementById('fSellOnly').checked;
    const fBoth = false;     // document.getElementById('fBoth').checked;
    const fMin5 = document.getElementById('fMin5').checked;
    const fMin10 = document.getElementById('fMin10').checked;
    const fMcap2000 = document.getElementById('fMcap2000').checked;
    const fObHigh = document.getElementById('fObHigh') ? document.getElementById('fObHigh').checked : false;
    // [v18.6] 20일고가대비 필터
    const fH20_5 = document.getElementById('fH20_5') ? document.getElementById('fH20_5').checked : false;
    const fH20_10 = document.getElementById('fH20_10') ? document.getElementById('fH20_10').checked : false;
    const fH20_15 = document.getElementById('fH20_15') ? document.getElementById('fH20_15').checked : false;

    document.querySelectorAll('.data-table tbody tr').forEach(row => {{
        const text = row.textContent.toLowerCase();
        const ratio = parseFloat(row.dataset.ratio);
        const change = parseFloat(row.dataset.change);
        const direction = row.dataset.direction;
        const totalAmt = parseFloat(row.dataset.totalAmt) / 100000000;
        const mcap = parseFloat(row.dataset.mcap) || 0;
        const rowTheme = row.dataset.theme || '';
        const obRatio = parseFloat(row.dataset.obRatio) || 0;
        const atr7pct = parseFloat(row.dataset.atr7pct) || 0;
        const h20 = parseFloat(row.dataset.h20) || 0;
        const presenceText = '';  // [v18.1] disabled

        let show = true;

        if (search && !text.includes(search)) show = false;

        if (fStrongBuy || fBuy || fStrongSell || fSell) {{
            let dirMatch = false;
            if (fStrongBuy && direction.includes('강한매수')) dirMatch = true;
            if (fBuy && ratio >= 60) dirMatch = true;
            if (fStrongSell && direction.includes('강한매도')) dirMatch = true;
            if (fSell && ratio <= 40) dirMatch = true;
            if (!dirMatch) show = false;
        }}

        if (fAtrSweet && (atr7pct < 0.5 || atr7pct > 1.0)) show = false;

        if (fBuyOnly || fSellOnly || fBoth) {{
            let presMatch = false;
            if (fBuyOnly && presenceText === '매수Only') presMatch = true;
            if (fSellOnly && presenceText === '매도Only') presMatch = true;
            if (fBoth && presenceText === '양쪽') presMatch = true;
            if (!presMatch) show = false;
        }}

        if (fMin5 && totalAmt < 5) show = false;
        if (fMin10 && totalAmt < 10) show = false;

        if (fMcap2000 && mcap < 2000) show = false;

        if (fObHigh && obRatio < 0.1) show = false;

        // [v18.6] 20일고가대비 필터 (체크된 것 중 가장 넓은 범위 적용)
        if (fH20_5 || fH20_10 || fH20_15) {{
            let h20Match = false;
            if (fH20_5 && h20 >= -5) h20Match = true;
            if (fH20_10 && h20 >= -10) h20Match = true;
            if (fH20_15 && h20 >= -15) h20Match = true;
            if (!h20Match) show = false;
        }}

        if (activeThemeFilter && !rowTheme.includes(activeThemeFilter)) show = false;

        row.style.display = show ? '' : 'none';
    }});

    updateRowCount();
}}

function resetFilter() {{
    document.getElementById('searchInput').value = '';
    document.querySelectorAll('.filter-bar input[type="checkbox"]').forEach(cb => cb.checked = false);
    clearThemeFilter();
    document.querySelectorAll('.data-table tbody tr').forEach(row => row.style.display = '');
    updateRowCount();
}}

function updateRowCount() {{
    const activeSection = document.querySelector('.section-content.active');
    if (!activeSection) return;
    const table = activeSection.querySelector('.data-table');
    if (!table) return;
    const total = table.querySelectorAll('tbody tr').length;
    const visible = Array.from(table.querySelectorAll('tbody tr')).filter(r => r.style.display !== 'none').length;
    document.getElementById('rowCountDisplay').textContent =
        visible === total ? total + '종목' : visible + '/' + total + '종목';
}}

updateRowCount();

// ── Navigation: date boundary confirmation ──
(function() {{
    var extractDate = function(filename) {{
        var m = filename.match(/block_trade_(\\d{{8}})_\\d{{4}}\\.html/);
        if (!m) return null;
        var d = m[1];
        return d.slice(0,4) + '-' + d.slice(4,6) + '-' + d.slice(6,8);
    }};
    var currentFile = window.location.pathname.split('/').pop() || window.location.href.split('/').pop();
    var currentDate = extractDate(currentFile);
    if (!currentDate) return;

    document.querySelectorAll('.report-nav .nav-arrow').forEach(function(el) {{
        el.addEventListener('click', function(e) {{
            var targetDate = extractDate(el.getAttribute('href'));
            if (targetDate && targetDate !== currentDate) {{
                if (!confirm('날짜 경계를 넘어갑니다.\\n(' + currentDate + ' → ' + targetDate + ')\\n\\n이동하시겠습니까?')) {{
                    e.preventDefault();
                }}
            }}
        }});
    }});
}})();
</script>

<!-- [v18.20] 프로그램매매 추이 모달 -->
<div id="ptModal" class="pt-modal-backdrop" onclick="if(event.target===this)closeProgramTradeModal()">
  <div class="pt-modal">
    <div class="pt-modal-header">
      <div>
        <div class="pt-modal-title" id="ptModalTitle">-</div>
        <div class="pt-modal-meta" id="ptModalMeta">-</div>
      </div>
      <button class="pt-modal-close" onclick="closeProgramTradeModal()">&times;</button>
    </div>
    <div class="pt-modal-body">
      <div class="pt-chart-wrap">
        <canvas id="ptChartCanvas"></canvas>
      </div>
      <div id="ptXScrollWrap" class="pt-scroll-wrap">
        <span class="pt-scroll-label">확대 상태 좌우 이동</span>
        <input id="ptXScroll" class="pt-scroll-range" type="range" min="0" max="0" step="1" value="0">
      </div>
      <div class="pt-table-wrap">
        <table class="pt-table" id="ptTable">
          <thead><tr>
            <th>시간</th><th>현재가</th><th>대비</th><th>등락률</th>
            <th>거래량</th><th>매도수량</th><th>매수수량</th>
            <th>순매수수량</th><th>순매수증감</th>
          </tr></thead>
          <tbody id="ptTableBody"></tbody>
        </table>
      </div>
    </div>
    <div class="pt-modal-footer">
      Source: 키움 REST API ka90008 (종목시간별프로그램매매추이) · 30초 단위 ·
      행 순서: 최신 → 과거 (응답 순서 그대로) · Shift+드래그로 좌우 이동
    </div>
  </div>
</div>

<!-- [v18.20] Chart.js (CDN) + hammer.js (zoom pan 필수) + zoom plugin + pako (gzip 디코더) -->
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/hammer.js/2.0.8/hammer.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/chartjs-plugin-zoom/2.0.1/chartjs-plugin-zoom.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/pako/2.1.0/pako.min.js"></script>

<!-- [v18.20] 프로그램매매 페이로드 (gzip+base64) -->
<script id="ptPayloadScript" type="application/octet-stream">__PT_PAYLOAD_B64__</script>

<script>
(function(){{
    // ── 페이로드 디코딩 (지연 실행: 첫 클릭 시 1회만) ──
    let _ptData = null;
    function _getPtData() {{
        if (_ptData !== null) return _ptData;
        try {{
            const b64 = (document.getElementById('ptPayloadScript').textContent || '').trim();
            if (!b64 || b64 === '{{}}') {{ _ptData = {{}}; return _ptData; }}
            // base64 → Uint8Array
            const binStr = atob(b64);
            const len = binStr.length;
            const bytes = new Uint8Array(len);
            for (let i = 0; i < len; i++) bytes[i] = binStr.charCodeAt(i);
            // gzip 해제
            const json = pako.ungzip(bytes, {{ to: 'string' }});
            _ptData = JSON.parse(json);
            console.log('[PT] 디코드 OK: ' + Object.keys(_ptData).length + '종목');
        }} catch (e) {{
            console.error('[PT] 디코드 실패:', e);
            _ptData = {{}};
        }}
        return _ptData;
    }}

    let _chart = null;

    // 시간 포맷: "153029" → "15:30"
    function _fmtTime(s) {{
        s = String(s || '');
        if (s.length >= 4) return s.substring(0,2) + ':' + s.substring(2,4);
        return s;
    }}
    // 부호 색상 클래스
    function _signCls(n) {{
        const v = parseFloat(n);
        if (isNaN(v) || v === 0) return '';
        return v > 0 ? 'pt-num-pos' : 'pt-num-neg';
    }}
    // 숫자 천단위 콤마
    function _fmtNum(n) {{
        const v = parseFloat(n);
        if (isNaN(v)) return '-';
        return v.toLocaleString('ko-KR');
    }}
    function _fmtSigned(n) {{
        const v = parseFloat(n);
        if (isNaN(v)) return '-';
        return (v > 0 ? '+' : '') + v.toLocaleString('ko-KR');
    }}

    window.showProgramTradeModal = function(td) {{
        const code = td.getAttribute('data-pt-code');
        if (!code) return;
        const data = _getPtData();
        const entry = data[code];
        if (!entry) {{
            alert('해당 종목의 프로그램매매 시계열 데이터가 없습니다.');
            return;
        }}
        const rows = entry.rows || [];
        const name = entry.n || '';

        document.getElementById('ptModalTitle').textContent =
            '[' + code + '] ' + name + ' — 시간대별 프로그램매매 추이';
        document.getElementById('ptModalMeta').textContent =
            '총 ' + rows.length + '행 · 키움 ka90008 (수량 기준) · 휠 확대 / 드래그 확대 가능';

        // 표 채우기 (응답 순서 그대로 = 최신부터)
        const tbody = document.getElementById('ptTableBody');
        let html = '';
        for (let i = 0; i < rows.length; i++) {{
            const r = rows[i];
            // r = [tm, cur_prc, pred_pre, flu_rt, trde_qty, sell_q, buy_q, net_q, net_irds]
            const pred = parseFloat(r[2]);
            const flu = parseFloat(r[3]);
            const net = parseFloat(r[7]);
            const irds = parseFloat(r[8]);
            html += '<tr>'
                + '<td>' + _fmtTime(r[0]) + '</td>'
                + '<td>' + _fmtNum(Math.abs(parseFloat(r[1]))) + '</td>'
                + '<td class="' + _signCls(pred) + '">' + _fmtSigned(pred) + '</td>'
                + '<td class="' + _signCls(flu) + '">'
                  + (isNaN(flu) ? '-' : (flu>0?'+':'') + flu.toFixed(2) + '%') + '</td>'
                + '<td>' + _fmtNum(r[4]) + '</td>'
                + '<td>' + _fmtNum(r[5]) + '</td>'
                + '<td>' + _fmtNum(r[6]) + '</td>'
                + '<td class="' + _signCls(net) + '">' + _fmtSigned(net) + '</td>'
                + '<td class="' + _signCls(irds) + '">' + _fmtSigned(irds) + '</td>'
                + '</tr>';
        }}
        tbody.innerHTML = html;

        // 차트 (가격/누적순매수는 line, 순매수증감은 bar)
        // 응답이 최신→과거이므로 차트는 reverse해서 과거→최신으로
        const reversed = rows.slice().reverse();
        const labels = reversed.map(r => _fmtTime(r[0]));
        const cumNet = reversed.map(r => parseFloat(r[7]));    // 누적 순매수수량
        const irdsArr = reversed.map(r => parseFloat(r[8]));   // 순매수증감 (막대)
        const prices = reversed.map(r => Math.abs(parseFloat(r[1])));  // 현재가

        function _finiteNums(arr) {{
            return (arr || []).map(v => Number(v)).filter(v => Number.isFinite(v));
        }}
        function _axisBounds(arr, padRatio, includeZero) {{
            const vals = _finiteNums(arr);
            if (!vals.length) return {{ min: 0, max: 1 }};
            let min = Math.min(...vals);
            let max = Math.max(...vals);
            if (includeZero) {{
                min = Math.min(min, 0);
                max = Math.max(max, 0);
            }}
            if (min === max) {{
                const base = Math.abs(max) || 1;
                const pad = Math.max(base * (padRatio || 0.1), 1);
                return {{ min: min - pad, max: max + pad }};
            }}
            const span = max - min;
            const pad = Math.max(span * (padRatio || 0.1), 1);
            return {{ min: min - pad, max: max + pad }};
        }}

        const priceBounds = _axisBounds(prices, 0.12, false);
        const cumBounds = _axisBounds(cumNet, 0.10, false);
        const flowAbsMax = Math.max(..._finiteNums(irdsArr).map(v => Math.abs(v)), 1);
        const flowBounds = {{
            min: -(flowAbsMax * 1.15),
            max:  (flowAbsMax * 1.15)
        }};

        if (_chart) {{ _chart.destroy(); _chart = null; }}
        const ctx = document.getElementById('ptChartCanvas').getContext('2d');
        _chart = new Chart(ctx, {{
            type: 'bar',
            data: {{
                labels: labels,
                datasets: [
                    {{
                        type: 'line',
                        label: '누적 순매수수량(주)',
                        data: cumNet,
                        borderColor: '#c9d1d9',
                        backgroundColor: 'rgba(201,209,217,0.15)',
                        yAxisID: 'yQty',
                        borderWidth: 2,
                        pointRadius: 0,
                        pointHitRadius: 8,
                        tension: 0.15,
                        order: 2,
                    }},
                    {{
                        type: 'bar',
                        label: '순매수증감(주)',
                        data: irdsArr,
                        backgroundColor: irdsArr.map(v => v >= 0 ? 'rgba(248,81,73,0.72)' : 'rgba(88,166,255,0.72)'),
                        borderWidth: 0,
                        yAxisID: 'yFlow',
                        barPercentage: 0.92,
                        categoryPercentage: 0.98,
                        order: 3,
                    }},
                    {{
                        type: 'line',
                        label: '현재가(원)',
                        data: prices,
                        borderColor: '#3fb950',
                        backgroundColor: 'transparent',
                        yAxisID: 'yPrice',
                        borderWidth: 2,
                        pointRadius: 0,
                        pointHitRadius: 8,
                        tension: 0.15,
                        order: 1,
                    }}
                ]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                interaction: {{ mode: 'index', intersect: false }},
                plugins: {{
                    legend: {{ labels: {{ color: '#c9d1d9', font: {{ size: 11 }} }} }},
                    tooltip: {{
                        callbacks: {{
                            label: function(ctx) {{
                                const v = ctx.parsed.y;
                                if (ctx.dataset.yAxisID === 'yPrice') {{
                                    return ctx.dataset.label + ': ' + v.toLocaleString('ko-KR') + '원';
                                }}
                                return ctx.dataset.label + ': ' + (v>0?'+':'') + v.toLocaleString('ko-KR') + '주';
                            }}
                        }}
                    }},
                    zoom: {{
                        zoom: {{
                            drag: {{
                                enabled: true,
                                threshold: 5,
                                backgroundColor: 'rgba(187,134,252,0.15)',
                                borderColor: 'rgba(187,134,252,0.6)',
                                borderWidth: 1,
                            }},
                            wheel: {{ enabled: true, speed: 0.08 }},
                            pinch: {{ enabled: true }},
                            mode: 'x',
                            onZoomComplete: function({{ chart }}) {{ _updateScrollbar(chart); }},
                        }},
                        pan: {{
                            enabled: true,
                            mode: 'x',
                            modifierKey: 'shift',
                            onPanComplete: function({{ chart }}) {{ _updateScrollbar(chart); }},
                        }},
                        limits: {{
                            x: {{ minRange: 5 }},
                        }},
                    }}
                }},
                scales: {{
                    x: {{
                        ticks: {{ color: '#8b949e', maxTicksLimit: 14, font: {{ size: 10 }} }},
                        grid: {{ color: 'rgba(255,255,255,0.04)' }},
                    }},
                    yQty: {{
                        type: 'linear', position: 'left',
                        min: cumBounds.min,
                        max: cumBounds.max,
                        ticks: {{ color: '#c9d1d9', font: {{ size: 10 }},
                                 callback: v => v.toLocaleString('ko-KR') }},
                        grid: {{ color: 'rgba(255,255,255,0.04)' }},
                        title: {{ display: true, text: '누적 순매수수량(주)', color: '#c9d1d9' }},
                    }},
                    yFlow: {{
                        type: 'linear',
                        position: 'left',
                        min: flowBounds.min,
                        max: flowBounds.max,
                        display: false,
                        grid: {{ drawOnChartArea: false, drawTicks: false }},
                    }},
                    yPrice: {{
                        type: 'linear', position: 'right',
                        min: priceBounds.min,
                        max: priceBounds.max,
                        ticks: {{ color: '#3fb950', font: {{ size: 10 }},
                                 callback: v => v.toLocaleString('ko-KR') }},
                        grid: {{ display: false }},
                        title: {{ display: true, text: '현재가(원)', color: '#3fb950' }},
                    }}
                }}
            }}
        }});

        // 스크롤바 초기 상태 설정
        _updateScrollbar(_chart);

        // [v18.x fix] 더블클릭 → 줌 리셋 (초기 상태 복귀)
        document.getElementById('ptChartCanvas').addEventListener('dblclick', function(e) {{
            e.preventDefault();
            // drag zoom mouseup 처리 완료 후 실행되도록 지연
            setTimeout(function() {{ resetProgramTradeZoom(); }}, 0);
        }});

        // 스크롤바 드래그 → 차트 pan 연동
        var scrollEl = document.getElementById('ptXScroll');
        scrollEl.oninput = function() {{
            if (!_chart) return;
            var xScale = _chart.scales.x;
            var totalLen = labels.length;
            var visLen = Math.round(xScale.max - xScale.min);
            var newMin = parseInt(scrollEl.value);
            var newMax = newMin + visLen;
            if (newMax > totalLen - 1) {{ newMax = totalLen - 1; newMin = Math.max(0, newMax - visLen); }}
            _chart.options.scales.x.min = newMin;
            _chart.options.scales.x.max = newMax;
            _chart.update('none');
        }};

        document.getElementById('ptModal').classList.add('show');
    }};

    // 줌/팬 후 스크롤바 위치·범위 동기화
    function _updateScrollbar(chart) {{
        if (!chart) return;
        var scrollEl = document.getElementById('ptXScroll');
        var wrapEl = document.getElementById('ptXScrollWrap');
        var xScale = chart.scales.x;
        var totalLen = chart.data.labels.length;
        var visMin = Math.round(xScale.min);
        var visMax = Math.round(xScale.max);
        var visLen = visMax - visMin;
        // 줌 상태가 아니면 (전체 보기) 스크롤바 숨김
        if (visLen >= totalLen - 2) {{
            wrapEl.style.display = 'none';
        }} else {{
            wrapEl.style.display = 'flex';
            scrollEl.min = 0;
            scrollEl.max = Math.max(0, totalLen - 1 - visLen);
            scrollEl.value = Math.max(0, visMin);
        }}
    }}

    window.resetProgramTradeZoom = function() {{
        if (!_chart) return;
        // 1) 스크롤바 oninput이 직접 설정한 x축 config 제거
        delete _chart.options.scales.x.min;
        delete _chart.options.scales.x.max;
        // 2) 줌 플러그인 내부 상태 초기화
        if (typeof _chart.resetZoom === 'function') {{
            _chart.resetZoom();
        }}
        // 3) resetZoom 내부 update 후에도 min/max가 남아있으면 강제 제거 + 재갱신
        if (_chart.options.scales.x.min !== undefined ||
            _chart.options.scales.x.max !== undefined) {{
            delete _chart.options.scales.x.min;
            delete _chart.options.scales.x.max;
            _chart.update();
        }}
        _updateScrollbar(_chart);
    }};

    window.closeProgramTradeModal = function() {{
        document.getElementById('ptModal').classList.remove('show');
        if (_chart) {{ _chart.destroy(); _chart = null; }}
    }};

    // ESC로 닫기
    document.addEventListener('keydown', function(e) {{
        if (e.key === 'Escape') closeProgramTradeModal();
    }});
}})();
</script>
</body></html>"""
    # [v18.20] 페이로드 슬롯 치환
    pt_payload = _build_program_trade_payload(results)
    html = html.replace("__PT_PAYLOAD_B64__", pt_payload)

    return html



# ============================================================================
#  메인 분석 실행
# ============================================================================

def run_analysis(conn: CybosConnection,
                 theme_map: Dict[str, List[str]],
                 tick_cache: Dict[str, dict],
                 target_date: str = "") -> Tuple[List[dict], Dict[str, dict]]:
    """
    1회 분석 실행
    tick_cache: {종목코드: {"ticks": [...], "last_time": "HHMM"}}
    target_date: 분석 대상 날짜 "YYYYMMDD" (빈 문자열이면 오늘)
    Returns: (results, updated_tick_cache)
    """
    start_time = time.time()
    if target_date:
        today_str = target_date
    else:
        today_str = datetime.now().strftime("%Y%m%d")

    # Step 1: 종목 선정 (MarketEye 전종목 일괄 조회)
    log.info("=" * 60)
    log.info("[STEP 1] Target selection via MarketEye...")
    top_stocks = select_target_stocks(conn)

    if not top_stocks:
        log.error("[STEP 1] No stocks selected!")
        return [], tick_cache

    log.info(f"[STEP 1] Got {len(top_stocks)} target stocks")

    # [v18.20] 키움 프로그램매매 백그라운드 호출 시작 (Step 2와 병렬 실행)
    _kiwoom_future = _start_kiwoom_program_trade_future(top_stocks, today_str)

    # Step 2: 종목별 틱 데이터 수집 (증분) + 대량체결 분석
    est_calls = len(top_stocks) * 2
    log.info(f"[STEP 2] Fetching tick data (incremental)...")
    log.info(f"[STEP 2] Estimated: ~{est_calls} API calls, "
             f"remain={conn.get_remain_count()}")

    results = []
    success = 0
    fail = 0
    skip_etn = 0
    total_tick_calls = 0

    for i, stock in enumerate(top_stocks):
        code_a = stock["code"]                # 'A005930' 형태
        code_clean = stock["code_clean"]       # '005930' 형태
        name = stock["name"]
        change_rate = stock["change_rate"]
        trading_amount = stock["trading_amount"]
        market_cap = stock.get("market_cap", 0)  # 억원 단위

        # [v18.2] 비대상 종목 제외 (안전장치 — 입구 필터 통과 못한 잔여)
        stock_type = classify_stock_type(code_clean, name)
        if stock_type not in ("STOCK", "ETF"):
            skip_etn += 1
            continue

        # 첫 5종목 상세 로깅
        if i < 5:
            log.info(f"[STEP 2] Stock #{i+1}: {code_clean} {name} "
                     f"chg={change_rate:+.1f}% amt={trading_amount/1e8:.0f}억")

        # 증분수집: 이전 캐시의 마지막 시간 이후부터
        cached = tick_cache.get(code_clean, {})
        # [v18.1] 1분봉은 항상 전체 재조회 (증분 불필요, 1회 호출로 당일 전체 커버)
        last_time = cached.get("last_time", "")
        prev_ticks = cached.get("ticks", [])

        try:
            # [v18.1] 1분봉 전체 조회 (증분수집 불필요)
            all_bars, api_calls = fetch_minute_data_daishin(
                conn, code_a, today_str
            )
            total_tick_calls += api_calls

            # 캐시 업데이트 (1분봉은 매번 전체 교체)
            # [v18.19] prev_day_bars 키 보존: 이미 캐시된 값이 있으면 유지
            # (전일봉은 당일 중 날짜가 바뀌지 않는 한 동일하므로 재사용 가능)
            _prev_cached = tick_cache.get(code_clean, {}).get("prev_day_bars", [])
            tick_cache[code_clean] = {
                "bars": all_bars,
                "last_time": all_bars[-1]["time"] if all_bars else "",
                "prev_day_bars": _prev_cached,  # [v18.19] 전일봉 캐시 보존
            }

            # 수급 분석
            analysis = analyze_minute_data(all_bars)
            total_block = analysis["buy_bars"] + analysis["sell_bars"]

            results.append({
                "code": code_clean,
                "name": name,
                "type": stock_type,
                "change_rate": change_rate,
                "source": stock.get("_source", "volume_top"),
                "block_buy_count": analysis["block_buy_count"],
                "block_buy_amount": analysis["block_buy_amount"],
                "block_sell_count": analysis["block_sell_count"],
                "block_sell_amount": analysis["block_sell_amount"],
                "net_amount": analysis["net_amount"],
                "direction": analysis["direction"],
                "total_block": total_block,
                "total_ticks": analysis["total_bars"],
                "daily_volume": trading_amount,
                "market_cap": market_cap,  # 억원 단위
                # [v18.2] buy_ratio: analyze_minute_data()에서 계산된 값 사용
                # = 매수금액 / (매수금액+매도금액) × 100  (0~100% 범위)
                "buy_ratio": analysis["buy_ratio"],
                # [v18.5] 호가잔량/시총 비율(%)
                "ob_ratio": stock.get("ob_ratio", 0.0),
                "ask_remain": stock.get("ask_remain", 0),
                "bid_remain": stock.get("bid_remain", 0),
                # [v18.6] ATR(7)% — 1분봉 기준 변동성
                "atr7_pct": analysis.get("atr7_pct", 0.0),
                # [v18.8] 삼각형 매수타점 (점대칭 2x)
                "_triangle": _calc_triangle_target(
                    all_bars, analysis.get("atr7_pct", 0.0)),
                # [v18.6] 현재가 (일봉 후처리에서 20일고가 계산용)
                "_price": stock.get("price", 0),
                # [v18.6] 예상마감대금 관련 (STEP 3에서 채움)
                "est_eod": 0,
                "est_eod_vs_d1": 0.0,
                "est_eod_vs_d2": 0.0,
                "close_vs_20d_high": 0.0,
                # [v18.9] 거래대금 회귀 스크리닝 (STEP 3에서 채움)
                "tv_itatdo": None,
                "tv_recovery_mins": None,
                "tv_itatdo_u": None,         # [v18.9] U자형 보조값
                "tv_recovery_mins_u": None,  # [v18.9] U자형 보조값
                # [v18.18] 전일동시간대비 (STEP 2에서 채움)
                "prev_day_sync_pct": 0.0,
            })

            # [v18.18] 전일동시간대비 계산
            # fetch_minute_data_prev_day: 2일치(고가·저가 포함) 조회 후 전일 bars만 반환
            # 장전 실행 시 today_str = 오늘, all_bars = 당일 0봉 → sync_ratio = 0.0 (정상)
            # [v18.19] 캐시 hit 시 API 호출 생략
            try:
                _cached_prev = tick_cache[code_clean].get("prev_day_bars", [])
                if _cached_prev:
                    # 캐시 hit: API 호출 없이 재사용
                    prev_day_bars = _cached_prev
                    log.info(f"[SYNC] {code_clean} {name} "
                             f"prev_day_bars cache hit ({len(prev_day_bars)}봉)")
                else:
                    # 캐시 miss: API 호출 후 tick_cache에 저장
                    prev_day_bars, prev_api_calls = fetch_minute_data_prev_day(
                        conn, code_a, today_str
                    )
                    total_tick_calls += prev_api_calls
                    tick_cache[code_clean]["prev_day_bars"] = prev_day_bars
                prev_sync_pct = calc_prev_day_sync_ratio(all_bars, prev_day_bars)
                results[-1]["prev_day_sync_pct"] = prev_sync_pct
                if i < 5:
                    log.info(f"[SYNC] {code_clean} {name} "
                             f"prev_day_bars={len(prev_day_bars)} "
                             f"sync_ratio={prev_sync_pct:.1f}%")
            except Exception as e_sync:
                log.warning(f"[SYNC] {code_clean} {name} "
                            f"전일동시간대비 계산 실패: {e_sync}")
                # results[-1]["prev_day_sync_pct"] 는 이미 0.0 초기값

            # [v18.8] 삼각형 매수타점 진단 로그 (첫 5종목)
            if i < 5:
                tri = results[-1].get("_triangle", {})
                bars_n = len(all_bars)
                bars_range = (f"{all_bars[0]['time']}~{all_bars[-1]['time']}"
                              if all_bars else "empty")
                if tri.get("status") == "OK":
                    log.info(f"[TRI] {code_clean} {name} | bars={bars_n} "
                             f"({bars_range}) | P={tri['peak']:,} "
                             f"C={tri['center']:,} X={tri['x']:,} "
                             f"2x={tri['target_2x']:,} "
                             f"cur={tri['current_price']:,} "
                             f"~{tri.get('time_remain_2x', 'N/A')}분 "
                             f"[{tri.get('method', '?')}]")
                else:
                    log.info(f"[TRI] {code_clean} {name} | bars={bars_n} "
                             f"({bars_range}) | status={tri.get('status', '?')} "
                             f"reason={tri.get('reason', '?')}")

            success += 1

            if (i + 1) % 50 == 0:
                elapsed = time.time() - start_time
                rate = success / elapsed if elapsed > 0 else 0
                remaining = len(top_stocks) - (i + 1)
                eta = remaining / rate / 60 if rate > 0 else 0
                log.info(
                    f"[STEP 2] {i+1}/{len(top_stocks)} | "
                    f"OK:{success} Fail:{fail} ETN:{skip_etn} | "
                    f"{rate:.1f}/sec | ETA:{eta:.1f}min | "
                    f"remain={conn.get_remain_count()}"
                )

        except Exception as e:
            fail += 1
            log.info(f"[STEP 2] {code_clean} {name} error: {e}")

    elapsed = time.time() - start_time
    log.info(f"[STEP 2] Done: {success} OK, {fail} fail, {skip_etn} ETN skipped "
             f"({elapsed:.1f}s, {total_tick_calls} tick API calls)")

    # 대량체결 있는 종목 수 요약
    with_block = sum(1 for r in results if r["total_block"] > 0)
    log.info(f"[STEP 2] Stocks with block trades: {with_block}/{len(results)}")

    # ── [v18.5] ob_ratio는 MarketEye에서 이미 수집됨 (추가 API 호출 없음) ──
    ob_valid = sum(1 for r in results if r.get("ob_ratio", 0) > 0)
    if ob_valid > 0:
        ob_vals = [r["ob_ratio"] for r in results if r["ob_ratio"] > 0]
        log.info(f"[OB_RATIO] {ob_valid}/{len(results)} stocks with ob_ratio | "
                 f"avg={sum(ob_vals)/len(ob_vals):.4f}% "
                 f"max={max(ob_vals):.4f}% min={min(ob_vals):.4f}%")
        ob_high = sum(1 for v in ob_vals if v >= 0.1)
        if ob_high > 0:
            log.info(f"[OB_RATIO] High interest (≥0.1%): {ob_high} stocks")
    else:
        log.info("[OB_RATIO] No ob_ratio data (market may be closed)")

    # ── [v18.6] STEP 3: 일봉 데이터 조회 (block trade 종목만) ──
    # 예상마감대금 비교 + 20일 고가대비 계산
    block_results = [r for r in results if r["total_block"] > 0]
    if block_results:
        log.info(f"[STEP 3] Fetching daily bars for {len(block_results)} stocks "
                 f"(block trade only)...")

        # 일봉 캐시 (같은 날 반복 실행 시 API 호출 생략)
        daily_cache_file = os.path.join(
            CACHE_DIR, f"daily_cache_{today_str}.pkl")
        daily_cache: Dict[str, List[dict]] = {}
        if os.path.exists(daily_cache_file):
            try:
                with open(daily_cache_file, 'rb') as f:
                    daily_cache = pickle.load(f)
                log.info(f"[STEP 3] Loaded daily cache: {len(daily_cache)} stocks")
            except Exception:
                daily_cache = {}

        daily_api_calls = 0
        daily_cache_hits = 0
        daily_success = 0

        for r in block_results:
            code_clean = r["code"]
            code_a = ensure_a_prefix(code_clean)

            # 캐시 확인
            if code_clean in daily_cache and daily_cache[code_clean]:
                daily_bars = daily_cache[code_clean]
                daily_cache_hits += 1
            else:
                daily_bars, calls = fetch_daily_ohlcv(conn, code_a, DAILY_BAR_COUNT)
                daily_api_calls += calls
                daily_cache[code_clean] = daily_bars

            if not daily_bars:
                r["est_eod"] = 0
                r["est_eod_vs_d1"] = 0.0
                r["est_eod_vs_d2"] = 0.0
                r["close_vs_20d_high"] = 0.0
                r["limit_close_5d"] = False
                r["limit_close_days"] = []
                r["tv_itatdo"] = None
                r["tv_recovery_mins"] = None
                r["tv_itatdo_u"] = None
                r["tv_recovery_mins_u"] = None
                continue

            # 예상마감대금 + 20일고가 계산
            metrics = calc_estimated_eod_metrics(
                daily_bars=daily_bars,
                current_price=r.get("_price", 0),
                current_trading_amount=r["daily_volume"],  # MarketEye 원 단위
                target_date_str=today_str,
            )
            r["est_eod"] = metrics["est_eod"]
            r["est_eod_vs_d1"] = metrics["est_eod_vs_d1"]
            r["est_eod_vs_d2"] = metrics["est_eod_vs_d2"]
            r["close_vs_20d_high"] = metrics["close_vs_20d_high"]

            # [v18.9] 거래대금 회귀 스크리닝 (메인: 단순 시간 비례)
            tv_rev = calc_turnover_reversion(
                daily_bars=daily_bars,
                est_eod=metrics["est_eod"],
                target_date_str=today_str,
            )
            r["tv_itatdo"] = tv_rev["itatdo"]
            r["tv_recovery_mins"] = tv_rev["recovery_mins"]
            r["_tv_mu_v"] = tv_rev["mu_v"]
            r["_tv_theta"] = tv_rev["theta"]
            r["_est_eod"] = metrics["est_eod"]

            # [v18.9] 보조: U자형 보정 버전 (검증 비교용, 툴팁 노출)
            tv_rev_u = calc_turnover_reversion(
                daily_bars=daily_bars,
                est_eod=metrics["est_eod_u"],
                target_date_str=today_str,
            )
            r["tv_itatdo_u"] = tv_rev_u["itatdo"]
            r["tv_recovery_mins_u"] = tv_rev_u["recovery_mins"]
            r["_est_eod_u"] = metrics["est_eod_u"]

            # [v18.7] 5거래일 내 상/하한가 마감 체크
            limit_info = check_limit_close_5d(daily_bars, today_str)
            r["limit_close_5d"] = limit_info["has_limit"]
            r["limit_close_days"] = limit_info["limit_days"]

            daily_success += 1

        # 일봉 캐시 저장
        try:
            with open(daily_cache_file, 'wb') as f:
                pickle.dump(daily_cache, f)
        except Exception as e:
            log.warning(f"[STEP 3] Daily cache save failed: {e}")

        log.info(f"[STEP 3] Done: {daily_success} OK | "
                 f"API calls: {daily_api_calls} | cache hits: {daily_cache_hits}")

        # [v18.7] 5거래일 내 상/하한가 마감 종목 요약
        limit_count = sum(1 for r in block_results if r.get("limit_close_5d", False))
        if limit_count > 0:
            limit_names = [f"{r['name']}({','.join(d['type'][0].upper()+str(d['date'])[-4:] for d in r['limit_close_days'])})"
                           for r in block_results if r.get("limit_close_5d", False)]
            log.info(f"[STEP 3] 5일내 상/하한가 마감: {limit_count}종목 | {', '.join(limit_names[:10])}"
                     + (f" ... 외 {limit_count-10}종목" if limit_count > 10 else ""))

        # 이상 징후 경고
        if daily_success == 0 and len(block_results) > 0:
            log.warning("[STEP 3] ⚠️ 모든 종목 일봉 조회 실패 — CYBOS 상태 확인 필요")
    else:
        log.info("[STEP 3] No block trade stocks — skipping daily bar fetch")

    # [v18.20] 키움 프로그램매매 결과 회수 + results에 주입
    # [v19] 타임아웃 시 디스크 캐시 폴백을 위해 today_str 전달
    _attach_kiwoom_results(results, _kiwoom_future, date_yyyymmdd=today_str)

    return results, tick_cache



# ============================================================================
#  Main
# ============================================================================

def main():
    global TOP_N_BY_VOLUME, CHANGE_RATE_THRESHOLD
    global TOP_N_ETF, MIN_REQUEST_COUNT

    parser = argparse.ArgumentParser(
        description="Block Trade Analyzer v18.11 (Daishin CYBOS Plus)")
    # [v18.1] threshold 폐지 (1분봉 전체합산 방식)
    # parser.add_argument("--threshold", type=int, default=BLOCK_TRADE_THRESHOLD,
    #                     help=f"Block trade threshold in KRW")
    parser.add_argument("--top-volume", type=int, default=TOP_N_BY_VOLUME,
                        help=f"Top N by trading amount (default: {TOP_N_BY_VOLUME})")
    parser.add_argument("--surge-rate", type=float, default=CHANGE_RATE_THRESHOLD,
                        help=f"Surge filter %% (default: {CHANGE_RATE_THRESHOLD})")
    parser.add_argument("--top-etf", type=int, default=TOP_N_ETF,
                        help=f"Top N ETFs (default: {TOP_N_ETF})")
    # [v18.1] 틱 모드 전용 옵션 주석처리
    # parser.add_argument("--tick-count", ...)
    # parser.add_argument("--max-pages", ...)
    parser.add_argument("--repeat", type=int, default=0,
                        help="Auto-repeat interval in minutes (0=single run)")
    parser.add_argument("--debug", action="store_true",
                        help="Enable debug logging")
    args = parser.parse_args()

    # BLOCK_TRADE_THRESHOLD = args.threshold  # [v18.1] 폐지
    TOP_N_BY_VOLUME = args.top_volume
    CHANGE_RATE_THRESHOLD = args.surge_rate
    TOP_N_ETF = args.top_etf
    # TICK_REQUEST_COUNT = args.tick_count  # [v18.1] 주석처리
    # MAX_TICK_PAGES = args.max_pages       # [v18.1] 주석처리

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        log.info("[CONFIG] Debug mode: ON")
    else:
        log.info("[CONFIG] Debug mode: OFF (use --debug for verbose)")

    # 디렉토리 생성
    os.makedirs(CACHE_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    log.info("=" * 60)
    log.info("  Block Trade Analyzer v18.11 - CYBOS Plus 1-min (Quote Comparison)")
    log.info("=" * 60)
    log.info(f"  Mode: 1-min bar aggregation (no threshold)")
    log.info(f"  Stock filter: SectionKind (STOCK+ETF only, no ETN/ELW)")
    log.info(f"  Pre-market: auto-detect, use previous day data")
    log.info(f"  Top by trading amount: {TOP_N_BY_VOLUME}")
    log.info(f"  Surge filter: >= {CHANGE_RATE_THRESHOLD}%")
    log.info(f"  Top ETFs: {TOP_N_ETF}")
    log.info(f"  Min bar request: {MIN_REQUEST_COUNT}")
    # log.info(f"  Max tick pages: ...")  # [v18.1] 불필요
    log.info(f"  Buy/Sell method: Quote Comparison (fields 10/11, 1-min bars)")
    log.info(f"  Repeat: {'every ' + str(args.repeat) + 'min' if args.repeat > 0 else 'single run'}")

    # LLM 키 확인
    _api_file = os.path.join(SCRIPT_DIR, "api11.txt")
    _has_llm_key = False
    if os.path.exists(_api_file):
        try:
            _k = open(_api_file, "r", encoding="utf-8-sig").read().strip().split("\n")[0].strip()
            _has_llm_key = bool(_k)
        except Exception:
            pass
    if not _has_llm_key:
        _has_llm_key = bool(os.environ.get("OPENAI_API_KEY", "").strip())
    log.info(f"  LLM theme: {'YES (GPT-4o-mini)' if _has_llm_key else 'NO (keyword fallback)'}")
    log.info("=" * 60)

    # COM 초기화
    pythoncom.CoInitialize()

    # Step 0: CYBOS Plus 연결 확인
    conn = CybosConnection()
    if not conn.check_connect():
        log.error("[FATAL] CYBOS Plus not connected. Exiting.")
        return

    # 테마 맵 (초기 비어있음, fill_missing_themes에서 채움)
    theme_map: Dict[str, List[str]] = {}

    # 틱 데이터 캐시 (장중 증분수집용)
    tick_cache_file = os.path.join(
        CACHE_DIR, f"tick_cache_daishin_{datetime.now().strftime('%Y%m%d')}.pkl"
    )
    tick_cache: Dict[str, dict] = {}

    if os.path.exists(tick_cache_file):
        try:
            with open(tick_cache_file, 'rb') as f:
                tick_cache = pickle.load(f)
            log.info(f"[CACHE] Loaded tick cache: {len(tick_cache)} stocks")
        except Exception:
            tick_cache = {}

    # 결과 캐시 (장마감 후 재실행 시 CYBOS API 호출 생략)
    def _results_cache_path(date_str: str) -> str:
        return os.path.join(CACHE_DIR, f"results_cache_{date_str}.pkl")

    def _is_market_hours() -> bool:
        """현재 시각이 장중(09:00~15:40)인지 판별"""
        now = datetime.now()
        if now.hour < 9:
            return False
        if now.hour > 15 or (now.hour == 15 and now.minute >= 40):
            return False
        return True

    def run_once(target_date: str = "", label: str = ""):
        nonlocal tick_cache, theme_map

        run_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if label:
            run_time += f" ({label})"

        # 캐시 대상 날짜 결정
        effective_date = target_date if target_date else datetime.now().strftime("%Y%m%d")
        cache_path = _results_cache_path(effective_date)

        results = None
        elapsed = 0.0

        # ── 경로 A: 장외시간 + 캐시 존재 → CYBOS API 호출 생략 ──
        if not _is_market_hours() and os.path.exists(cache_path):
            try:
                with open(cache_path, 'rb') as f:
                    cached = pickle.load(f)
                results = cached["results"]
                # [v18.14] theme_map TTL 검증
                # 정책: 매 거래일마다 갱신. 캐시의 refresh_date가 오늘과 다르면 무효화하고
                #       fill_missing_themes를 강제 호출해 네이버에서 다시 동기화
                _today_str = datetime.now().strftime("%Y%m%d")
                _cache_refresh_date = cached.get("theme_map_refresh_date", "")
                if _cache_refresh_date == _today_str:
                    theme_map.update(cached["theme_map"])
                    log.info(f"[CACHE] Off-market: loaded results cache "
                             f"({len(results)} stocks, date={effective_date}, "
                             f"theme_refresh={_cache_refresh_date})")
                else:
                    log.warning(f"[CACHE] theme_map TTL stale "
                                f"(cache_refresh={_cache_refresh_date or 'N/A'}, "
                                f"today={_today_str}) → 강제 재동기화")
                    # theme_map은 비워둔 채로 두고, 아래에서 fill_missing_themes를 호출하도록 강제
                    # 이를 위해 results를 None으로 만들지 않고, 별도 플래그로 처리
                    theme_map.update(cached["theme_map"])  # 일단 로드 (LLM 분류분 보존용)
                    log.info(f"[CACHE] Off-market: loaded results cache "
                             f"({len(results)} stocks, date={effective_date}) "
                             f"+ 테마 강제 재동기화 예정")
                    # fill_missing_themes를 명시적으로 호출하여 네이버 sync 수행
                    try:
                        fill_missing_themes(results, theme_map)
                        # 캐시 업데이트
                        try:
                            with open(cache_path, 'wb') as f:
                                pickle.dump({
                                    "results": results,
                                    "theme_map": dict(theme_map),
                                    "saved_at": datetime.now().isoformat(),
                                    "is_complete": cached.get("is_complete"),
                                    "max_last_bar_time": cached.get("max_last_bar_time", ""),
                                    "theme_map_refresh_date": _today_str,
                                }, f)
                            log.info(f"[CACHE] theme_map 재동기화 완료 → 캐시 갱신")
                        except Exception as e:
                            log.warning(f"[CACHE] 재동기화 후 저장 실패: {e}")
                    except Exception as e:
                        log.error(f"[CACHE] fill_missing_themes 강제 호출 실패: {e}")

                # [v18.9] 1차 검증: 저장 시점 메타데이터의 is_complete 플래그 (정보용)
                # 실제 폴백은 아래 2차(데이터 기반) 검증에서 처리
                _meta_complete = cached.get("is_complete", None)
                _meta_last = cached.get("max_last_bar_time", "")
                if _meta_complete is False:
                    log.warning(f"[CACHE] 메타 플래그: 불완전 캐시 "
                                f"(last_bar={_meta_last or 'N/A'})")
                elif _meta_complete is None:
                    log.info(f"[CACHE] 메타 없음 (구버전 캐시) → 데이터 기반 검증으로 진행")
                else:
                    log.info(f"[CACHE] 메타 검증 OK (last_bar={_meta_last})")
                log.info(f"[CACHE] Skipping CYBOS API calls (prices unchanged)")

                # [v18.8] tick_cache 날짜 미스매치 자동 복구
                # tick_cache_file은 항상 "오늘 날짜"로 생성되므로,
                # 장전(전일분석) 모드에서는 빈 파일을 보게 됨.
                # → effective_date(어제) tick_cache 파일을 직접 로드.
                tc_has_bars = any(v.get("bars") for v in tick_cache.values())
                if not tc_has_bars:
                    eff_tc_file = os.path.join(
                        CACHE_DIR,
                        f"tick_cache_daishin_{effective_date}.pkl"
                    )
                    if os.path.exists(eff_tc_file):
                        try:
                            with open(eff_tc_file, 'rb') as f:
                                tick_cache = pickle.load(f)
                            log.info(f"[CACHE] Loaded {effective_date} "
                                     f"tick_cache: {len(tick_cache)} stocks")
                        except Exception as e:
                            log.warning(f"[CACHE] {effective_date} tick_cache "
                                        f"load failed: {e}")
                    else:
                        log.warning(f"[CACHE] {effective_date} tick_cache "
                                    f"파일 없음: {eff_tc_file}")

                # [v18.9] 2차 검증: tick_cache 실제 bar 데이터의 마지막 시각 확인
                # 어떤 종목이든 15:20 이상까지 도달했으면 장마감까지 수집된 것으로 간주
                # 불완전한 경우 경로 B(run_analysis)는 장외에 MarketEye 시세가 0이라
                # 못 쓰므로, 대신 tick_cache를 비워서 아래 "직접 fetch" 경로로 유도한다.
                _data_max_last = ""
                for _v in tick_cache.values():
                    _bars = _v.get("bars") or []
                    if _bars:
                        _lt = str(_bars[-1].get("time", ""))
                        if _lt > _data_max_last:
                            _data_max_last = _lt
                _need_full_refetch = False
                if not _data_max_last:
                    log.warning(f"[CACHE] tick_cache에 bar 데이터 없음 → 직접 fetch 경로로 진행")
                    _need_full_refetch = True
                elif _data_max_last < "1520":
                    log.warning(f"[CACHE] 불완전 캐시 감지 (데이터 last_bar={_data_max_last} "
                                f"< 1520) → 결과 종목 tick_cache 초기화 후 직접 fetch")
                    _need_full_refetch = True
                else:
                    log.info(f"[CACHE] 데이터 검증 OK (max_last_bar={_data_max_last})")

                if _need_full_refetch:
                    # 결과 종목에 한해 tick_cache를 비움 → 아래 triangle 재주입에서
                    # n_no_bars == len(results) 조건이 성립해 직접 fetch 루프가 실행됨
                    for _r in results:
                        _code = _r.get("code", "")
                        tick_cache[_code] = {"bars": [], "last_time": ""}

                # [v18.8] _triangle 재주입
                # 결과 캐시는 v18.7 이전 버전에서 저장되었을 수 있으므로
                # tick_cache의 bars로 _triangle을 재계산해 주입한다.
                # tick_cache가 비어있는 종목은 INSUFFICIENT로 표시.
                n_ok = 0
                n_nowave = 0
                n_insuf = 0
                n_no_bars = 0
                for r in results:
                    code = r.get("code", "")
                    cached_bars = tick_cache.get(code, {}).get("bars", [])
                    if cached_bars:
                        tri = _calc_triangle_target(
                            cached_bars, r.get("atr7_pct", 0.0))
                        r["_triangle"] = tri
                        if tri.get("status") == "OK":
                            n_ok += 1
                        elif tri.get("status") == "NO_WAVE":
                            n_nowave += 1
                        else:
                            n_insuf += 1
                    else:
                        r["_triangle"] = {"status": "INSUFFICIENT",
                                          "reason": "no_cached_bars"}
                        n_no_bars += 1
                log.info(f"[CACHE] Triangle recalc: OK={n_ok}, "
                         f"NO_WAVE={n_nowave}, INSUF={n_insuf}, "
                         f"no_bars={n_no_bars}")

                # [v18.8] tick_cache 부족 시 직접 fetch (최후의 수단)
                # 어제 tick_cache 파일도 없는 경우에만 발생.
                if n_no_bars > 0 and n_no_bars == len(results):
                    log.info(f"[CACHE] tick_cache 완전 부재 → 직접 fetch 시작 "
                             f"({n_no_bars}종목, 약 {n_no_bars*0.3:.0f}초 예상)")
                    fetched_ok = 0
                    fetched_fail = 0
                    fetch_start = time.time()
                    for fi, r in enumerate(results):
                        code = r.get("code", "")
                        if code in tick_cache and tick_cache[code].get("bars"):
                            continue
                        try:
                            code_a = ensure_a_prefix(code)
                            bars, _ = fetch_minute_data_daishin(
                                conn, code_a, effective_date)
                            if bars:
                                tick_cache[code] = {
                                    "bars": bars,
                                    "last_time": bars[-1]["time"],
                                }
                                tri = _calc_triangle_target(
                                    bars, r.get("atr7_pct", 0.0))
                                r["_triangle"] = tri
                                fetched_ok += 1
                            else:
                                fetched_fail += 1
                        except Exception as e:
                            fetched_fail += 1
                            if fi < 5:
                                log.warning(f"[FETCH] {code} 실패: {e}")
                        if (fi + 1) % 50 == 0:
                            elap = time.time() - fetch_start
                            rate = fetched_ok / elap if elap > 0 else 0
                            log.info(f"[FETCH] {fi+1}/{len(results)} | "
                                     f"OK={fetched_ok} Fail={fetched_fail} | "
                                     f"{rate:.1f}/s")
                    fetch_elapsed = time.time() - fetch_start
                    log.info(f"[FETCH] Done: OK={fetched_ok}, "
                             f"Fail={fetched_fail} ({fetch_elapsed:.1f}s)")

                    # 새로 fetch한 tick_cache를 effective_date 파일로 저장
                    eff_tc_file = os.path.join(
                        CACHE_DIR,
                        f"tick_cache_daishin_{effective_date}.pkl"
                    )
                    try:
                        with open(eff_tc_file, 'wb') as f:
                            pickle.dump(tick_cache, f)
                        log.info(f"[CACHE] {effective_date} tick_cache saved: "
                                 f"{len(tick_cache)} stocks")
                    except Exception as e:
                        log.warning(f"[CACHE] tick_cache save failed: {e}")

                    n_ok2 = sum(1 for r in results
                                if r.get("_triangle", {}).get("status") == "OK")
                    log.info(f"[CACHE] Triangle final: OK={n_ok2}/{len(results)}")

                # [v18.9] STEP 3 AUX 트리거 조건:
                #   (1) 이번에 분봉 재수집이 발생했을 때
                #   (2) 캐시는 정상이지만 tv_itatdo 필드가 전부 None인 경우
                #       (구버전 캐시 업그레이드 케이스)
                #   (3) tv_itatdo는 있는데 tv_itatdo_u가 없는 경우
                #       (v18.9 U자형 보조값 추가 후 첫 실행)
                _need_step3_aux = _need_full_refetch
                if not _need_step3_aux:
                    if all(r.get('tv_itatdo') is None for r in results):
                        log.info('[STEP 3 AUX] 구버전 캐시 감지 (tv_itatdo 전체 None) → 강제 실행')
                        _need_step3_aux = True
                    elif all(r.get('tv_itatdo_u') is None for r in results):
                        log.info('[STEP 3 AUX] U자형 보조값 부재 (tv_itatdo_u 전체 None) → 강제 실행')
                        _need_step3_aux = True

                if _need_step3_aux:

                    # [v18.9] STEP 3 보조 루프: 장외 전일분석 모드에서
                    # 일봉 데이터를 수집해 est_eod / tv_itatdo / tv_recovery_mins 재계산
                    # 장중 경로와 달리 '전일 실제 EOD' 값(일봉의 close·trading_amount)을
                    # current_price/current_trading_amount로 주입하여 추정 편향 제거.
                    # 전 종목 적용(블록트레이드 유무 무관).
                    log.info(f"[STEP 3 AUX] 일봉 수집 시작 "
                             f"({len(results)}종목, date={effective_date})")
                    _aux_daily_cache_file = os.path.join(
                        CACHE_DIR, f"daily_cache_{effective_date}.pkl"
                    )
                    _aux_daily_cache: Dict[str, List[dict]] = {}
                    if os.path.exists(_aux_daily_cache_file):
                        try:
                            with open(_aux_daily_cache_file, 'rb') as f:
                                _aux_daily_cache = pickle.load(f)
                            log.info(f"[STEP 3 AUX] daily cache loaded: "
                                     f"{len(_aux_daily_cache)} stocks")
                        except Exception:
                            _aux_daily_cache = {}

                    _aux_api_calls = 0
                    _aux_hits = 0
                    _aux_success = 0
                    _aux_no_target_row = 0
                    _aux_start = time.time()
                    _target_int = int(effective_date)

                    for _ai, r in enumerate(results):
                        _code_clean = r.get("code", "")
                        _code_a = ensure_a_prefix(_code_clean)

                        # 일봉 캐시 확인
                        if _code_clean in _aux_daily_cache and _aux_daily_cache[_code_clean]:
                            _dbars = _aux_daily_cache[_code_clean]
                            _aux_hits += 1
                        else:
                            try:
                                _dbars, _calls = fetch_daily_ohlcv(
                                    conn, _code_a, DAILY_BAR_COUNT)
                                _aux_api_calls += _calls
                                _aux_daily_cache[_code_clean] = _dbars
                            except Exception as e:
                                if _ai < 5:
                                    log.warning(f"[STEP 3 AUX] {_code_clean} "
                                                f"일봉 fetch 실패: {e}")
                                _dbars = []

                        if not _dbars:
                            # 일봉 없음 → 기본값
                            r["est_eod"] = 0
                            r["est_eod_vs_d1"] = 0.0
                            r["est_eod_vs_d2"] = 0.0
                            r["close_vs_20d_high"] = 0.0
                            r["tv_itatdo"] = None
                            r["tv_recovery_mins"] = None
                            r["tv_itatdo_u"] = None
                            r["tv_recovery_mins_u"] = None
                            r["limit_close_5d"] = False
                            r["limit_close_days"] = []
                            continue

                        # 전일(target_date)의 일봉 행 찾기
                        _target_row = None
                        for _b in _dbars:
                            if _b.get("date") == _target_int:
                                _target_row = _b
                                break

                        if _target_row is None:
                            # target_date의 일봉이 없음 → 계산 불가
                            _aux_no_target_row += 1
                            r["est_eod"] = 0
                            r["est_eod_vs_d1"] = 0.0
                            r["est_eod_vs_d2"] = 0.0
                            r["close_vs_20d_high"] = 0.0
                            r["tv_itatdo"] = None
                            r["tv_recovery_mins"] = None
                            r["tv_itatdo_u"] = None
                            r["tv_recovery_mins_u"] = None
                            r["limit_close_5d"] = False
                            r["limit_close_days"] = []
                            continue

                        # 전일 실제 close + trading_amount 주입
                        _actual_close = _target_row.get("close", 0) or 0
                        _actual_amt = _target_row.get("trading_amount", 0) or 0

                        # calc_estimated_eod_metrics는 target_date != today이면
                        # elapsed=390으로 고정 → est_eod = _actual_amt 그대로 사용
                        _metrics = calc_estimated_eod_metrics(
                            daily_bars=_dbars,
                            current_price=int(_actual_close),
                            current_trading_amount=int(_actual_amt),
                            target_date_str=effective_date,
                        )
                        r["est_eod"] = _metrics["est_eod"]
                        r["est_eod_vs_d1"] = _metrics["est_eod_vs_d1"]
                        r["est_eod_vs_d2"] = _metrics["est_eod_vs_d2"]
                        r["close_vs_20d_high"] = _metrics["close_vs_20d_high"]

                        # 거래대금 회귀 (이탈도 + 회복예상) — 메인: 단순 시간 비례
                        _tv_rev = calc_turnover_reversion(
                            daily_bars=_dbars,
                            est_eod=_metrics["est_eod"],
                            target_date_str=effective_date,
                        )
                        r["tv_itatdo"] = _tv_rev["itatdo"]
                        r["tv_recovery_mins"] = _tv_rev["recovery_mins"]
                        r["_tv_mu_v"] = _tv_rev["mu_v"]
                        r["_tv_theta"] = _tv_rev["theta"]
                        r["_est_eod"] = _metrics["est_eod"]

                        # [v18.9] 보조: U자형 보정 (AUX는 elapsed=390이라 메인과 동일값)
                        _tv_rev_u = calc_turnover_reversion(
                            daily_bars=_dbars,
                            est_eod=_metrics["est_eod_u"],
                            target_date_str=effective_date,
                        )
                        r["tv_itatdo_u"] = _tv_rev_u["itatdo"]
                        r["tv_recovery_mins_u"] = _tv_rev_u["recovery_mins"]
                        r["_est_eod_u"] = _metrics["est_eod_u"]

                        # 5거래일 내 상/하한가 마감 체크
                        _limit_info = check_limit_close_5d(_dbars, effective_date)
                        r["limit_close_5d"] = _limit_info["has_limit"]
                        r["limit_close_days"] = _limit_info["limit_days"]

                        _aux_success += 1

                        if (_ai + 1) % 100 == 0:
                            _elap = time.time() - _aux_start
                            log.info(f"[STEP 3 AUX] {_ai+1}/{len(results)} | "
                                     f"OK={_aux_success} | {_elap:.0f}s")

                    # 일봉 캐시 저장
                    try:
                        with open(_aux_daily_cache_file, 'wb') as f:
                            pickle.dump(_aux_daily_cache, f)
                    except Exception as e:
                        log.warning(f"[STEP 3 AUX] daily cache 저장 실패: {e}")

                    _aux_elapsed = time.time() - _aux_start
                    log.info(f"[STEP 3 AUX] Done: OK={_aux_success}/{len(results)} | "
                             f"API={_aux_api_calls} hits={_aux_hits} | "
                             f"no_target={_aux_no_target_row} | {_aux_elapsed:.0f}s")

                    if _aux_success == 0:
                        log.warning("[STEP 3 AUX] ⚠️ 전 종목 일봉 조회/매칭 실패")

                    # [v18.9] 직접 fetch 성공 후 results 캐시 재저장 (is_complete=True)
                    # 이게 없으면 내일 아침에 또 같은 폴백 경로를 탐
                    _new_max_last = ""
                    for _v in tick_cache.values():
                        _bars = _v.get("bars") or []
                        if _bars:
                            _lt = str(_bars[-1].get("time", ""))
                            if _lt > _new_max_last:
                                _new_max_last = _lt
                    _new_complete = bool(_new_max_last and _new_max_last >= "1520")
                    try:
                        with open(cache_path, 'wb') as f:
                            pickle.dump({
                                "results": results,
                                "theme_map": dict(theme_map),
                                "saved_at": datetime.now().isoformat(),
                                "is_complete": _new_complete,
                                "max_last_bar_time": _new_max_last,
                                "theme_map_refresh_date": _today_str,  # [v18.14]
                            }, f)
                        log.info(f"[CACHE] Results cache 재저장: "
                                 f"complete={_new_complete}, "
                                 f"last_bar={_new_max_last or 'N/A'}")
                    except Exception as e:
                        log.warning(f"[CACHE] Results cache 재저장 실패: {e}")
            except Exception as e:
                log.warning(f"[CACHE] Results cache load failed: {e}, "
                            f"falling back to API")
                results = None

        # ── 경로 B: API 호출 (장중 or 캐시 없음) ──
        if results is None:
            start = time.time()

            results, tick_cache = run_analysis(conn, theme_map, tick_cache,
                                               target_date=target_date)

            elapsed = time.time() - start

            if results:
                # 테마 분류
                fill_missing_themes(results, theme_map)

                # 틱 캐시 저장
                try:
                    with open(tick_cache_file, 'wb') as f:
                        pickle.dump(tick_cache, f)
                    log.info(f"[CACHE] Saved tick cache: {len(tick_cache)} stocks")
                except Exception as e:
                    log.warning(f"[CACHE] Save failed: {e}")

                # 결과 캐시 저장 (장외시간 재실행용)
                # [v18.9] 캐시 완결성 메타데이터 기록
                # 기준: tick_cache 전 종목의 마지막 bar 시각 중 최대값이 "1520" 이상이면 완결
                _max_last = ""
                for _v in tick_cache.values():
                    _bars = _v.get("bars") or []
                    if _bars:
                        _lt = str(_bars[-1].get("time", ""))
                        if _lt > _max_last:
                            _max_last = _lt
                _is_complete = bool(_max_last and _max_last >= "1520")
                try:
                    with open(cache_path, 'wb') as f:
                        pickle.dump({
                            "results": results,
                            "theme_map": dict(theme_map),
                            "saved_at": datetime.now().isoformat(),
                            "is_complete": _is_complete,
                            "max_last_bar_time": _max_last,
                            "theme_map_refresh_date": datetime.now().strftime("%Y%m%d"),  # [v18.14]
                        }, f)
                    log.info(f"[CACHE] Saved results cache: {cache_path} "
                             f"(complete={_is_complete}, last_bar={_max_last or 'N/A'})")
                except Exception as e:
                    log.warning(f"[CACHE] Results cache save failed: {e}")

        # ── 공통: HTML 리포트 생성 (캐시든 API든) ──
        if results:
            # [v18.8] Stage 1: 매수타점 스냅샷 jsonl 누적
            # 매 분석마다 _triangle status=OK 종목들의 스냅샷을 저장.
            # 이후 verify_triangle_targets.py로 사후 검증 가능.
            # [v18.10] 전일 분석 모드(장외/pre-market에서 과거 날짜 재분석)에서는
            # snapshot append를 스킵. 과거 날짜는 이미 그날 장중에 누적 저장됐으므로,
            # 오늘의 pre-market run이 그 파일에 또 append하면 중복 누적 발생.
            _today_str = datetime.now().strftime("%Y%m%d")
            _is_past_date_analysis = (effective_date and effective_date != _today_str)
            if _is_past_date_analysis:
                log.info(f"[SNAPSHOT] Skipped (past-date analysis mode, "
                         f"date={effective_date} != today={_today_str})")
            else:
                try:
                    snap_date = effective_date if effective_date else datetime.now().strftime("%Y%m%d")
                    snapshot_file = os.path.join(
                        CACHE_DIR, f"triangle_snapshots_{snap_date}.jsonl"
                    )
                    summary_file = os.path.join(
                        CACHE_DIR, f"triangle_summary_{snap_date}.jsonl"
                    )
                    snap_time = datetime.now().strftime("%H%M")
                    snap_count = 0
                    n_ok_save = 0
                    n_nowave_save = 0
                    n_insuf_save = 0
                    with open(snapshot_file, 'a', encoding='utf-8') as snapf:
                        for r in results:
                            tri = r.get("_triangle", {})
                            status = tri.get("status", "INSUFFICIENT")
                            if status == "OK":
                                n_ok_save += 1
                            elif status == "NO_WAVE":
                                n_nowave_save += 1
                            else:
                                n_insuf_save += 1
                            if status != "OK":
                                continue
                            rec = {
                                "snap_time": snap_time,
                                "code": r.get("code", ""),
                                "name": r.get("name", ""),
                                "current_price": tri.get("current_price"),
                                "peak": tri.get("peak"),
                                "center": tri.get("center"),
                                "x": tri.get("x"),
                                "target_2x": tri.get("target_2x"),
                                "target_1_5x": tri.get("target_1_5x"),
                                "atr7_pct": r.get("atr7_pct", 0.0),
                                "time_remain_2x": tri.get("time_remain_2x"),
                                "time_remain_1_5x": tri.get("time_remain_1_5x"),
                                "method": tri.get("method"),
                                "change_rate": r.get("change_rate", 0),
                                "market_cap": r.get("market_cap", 0),
                                "buy_ratio": r.get("buy_ratio", 0),
                            }
                            snapf.write(json.dumps(rec, ensure_ascii=False) + "\n")
                            snap_count += 1
                    # 사이클별 status 카운트 요약 (NO_WAVE 비율 추적용)
                    summary_rec = {
                        "snap_time": snap_time,
                        "n_total": len(results),
                        "n_ok": n_ok_save,
                        "n_nowave": n_nowave_save,
                        "n_insuf": n_insuf_save,
                    }
                    with open(summary_file, 'a', encoding='utf-8') as sumf:
                        sumf.write(json.dumps(summary_rec, ensure_ascii=False) + "\n")
                    log.info(f"[SNAPSHOT] Triangle snapshot saved: "
                             f"{snap_count} stocks → {snapshot_file}")
                except Exception as e:
                    log.warning(f"[SNAPSHOT] Save failed: {e}")

            # [v18.20.1] 캐시 경로에서도 키움 호출. run_analysis 경로면 자동 스킵됨.
            # target_date가 외부 지정이면 그걸 우선, 아니면 effective_date(있으면) 사용
            _kiwoom_dt = target_date if target_date else (effective_date if effective_date else None)
            try:
                _enrich_results_with_kiwoom_sync(results, _kiwoom_dt)
            except Exception as e:
                log.warning(f"[KIWOOM_PT] enrichment 실패(무시): {e}")
                # 안전장치: 모든 result에 program_trade 키 보장
                for _r in results:
                    if "program_trade" not in _r:
                        _r["program_trade"] = None

            html = generate_html_report(results, theme_map, run_time, elapsed)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M")
            filename = f"block_trade_{timestamp}.html"
            filepath = os.path.join(OUTPUT_DIR, filename)

            # ── 회차 네비게이션: 이전 파일 탐색 ──
            existing_files = _scan_report_files(OUTPUT_DIR)
            prev_file = None
            if existing_files:
                # 현재 파일보다 이름이 앞서는 파일 중 마지막 = 직전 회차
                earlier = [f for f in existing_files if f < filename]
                if earlier:
                    prev_file = earlier[-1]

            # 회차 번호: 기존 파일 중 현재보다 앞선 것 + 현재 = index
            nav_index = len([f for f in existing_files if f < filename]) + 1
            nav_total = len(existing_files) + 1  # 현재 파일 포함

            # 현재 HTML에 네비게이션 주입 (prev만, next는 아직 없음)
            html = _inject_nav(html, prev_file=prev_file, next_file=None,
                               nav_index=nav_index, nav_total=nav_total)

            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(html)

            # 직전 회차 파일의 '다음' 링크를 현재 파일로 업데이트
            if prev_file:
                _update_prev_file_next_link(OUTPUT_DIR, prev_file, filename)

            # latest 덮어쓰기 (네비게이션 포함)
            latest_path = os.path.join(OUTPUT_DIR, "block_trade_latest.html")
            with open(latest_path, 'w', encoding='utf-8') as f:
                f.write(html)

            block_count = sum(1 for r in results if r["total_block"] > 0)
            log.info(f"\n{'=' * 60}")
            log.info(f"  REPORT GENERATED: {filepath}")
            log.info(f"  Stocks with block trades: {block_count}/{len(results)}")
            log.info(f"  Elapsed: {elapsed:.1f}s")
            if elapsed > 0:
                log.info(f"  Total API calls: ~{conn.call_count}")
            else:
                log.info(f"  Source: results cache (no API calls)")
            log.info(f"{'=' * 60}")
        else:
            log.warning("[RESULT] No results to report!")

        # [v18.8] 매수타점 자동 검증 + 누적 통계
        # 매 run_once 종료 시 cache 폴더에서 검증 대기 중인 날짜를 자동 처리.
        # 이미 검증된 날짜는 스킵 → 매번 호출해도 부담 없음.
        try:
            n_new = _auto_verify_pending(CACHE_DIR)
            if n_new > 0:
                log.info(f"[STATS] {n_new}일 신규 검증 → 누적 통계 출력")
                _print_triangle_stats(CACHE_DIR)
        except Exception as e:
            log.warning(f"[VERIFY] 자동 검증 실패: {e}")

    # 실행
    if args.repeat > 0:
        log.info(f"[MODE] Auto-repeat every {args.repeat} minutes. Press Ctrl+C to stop.")
        run_count = 0

        # [v18.2] 장전 실행 시: 전일 데이터 → 09:05까지 대기 → 장중 반복
        target_date, is_prev = get_target_date()
        if is_prev:
            run_count += 1
            log.info(f"\n{'#' * 60}")
            log.info(f"  Run #{run_count} (Pre-market: previous day {target_date})")
            log.info(f"{'#' * 60}")
            run_once(target_date=target_date,
                     label=f"전일 {target_date[:4]}-{target_date[4:6]}-{target_date[6:]}")

            # 09:05까지 대기
            now = datetime.now()
            market_start = now.replace(hour=9, minute=5, second=0, microsecond=0)
            wait_sec = (market_start - now).total_seconds()
            if wait_sec > 0:
                log.info(f"[SCHED] Waiting until 09:05 for market open... "
                         f"({wait_sec/60:.1f} min)")
                try:
                    time.sleep(wait_sec)
                except KeyboardInterrupt:
                    log.info("\n[STOP] Interrupted during pre-market wait. Exiting.")
                    pythoncom.CoUninitialize()
                    return

                # 09:05 도달 후 틱 캐시 초기화 (전일 데이터 제거)
                tick_cache = {}
                log.info("[SCHED] Market open! Tick cache cleared for new day.")

        while True:
            try:
                # 장마감 체크 (15:40 이후 자동 종료)
                now = datetime.now()
                if now.hour > 15 or (now.hour == 15 and now.minute >= 40):
                    log.info(f"[STOP] Market closed (15:40+). Exiting.")
                    break

                run_count += 1
                log.info(f"\n{'#' * 60}")
                log.info(f"  Run #{run_count} at {now.strftime('%H:%M:%S')}")
                log.info(f"{'#' * 60}")
                run_once()  # 장중: 당일 데이터

                wait_sec = args.repeat * 60
                next_time = datetime.now().strftime('%H:%M:%S')
                log.info(f"[WAIT] Next run in {args.repeat} minutes "
                         f"(at ~{next_time}+{args.repeat}min, "
                         f"auto-stop at 15:40, Ctrl+C to stop)")
                time.sleep(wait_sec)
            except KeyboardInterrupt:
                log.info("\n[STOP] Interrupted by user. Exiting.")
                break
    else:
        # 단일 실행
        target_date, is_prev = get_target_date()
        if is_prev:
            log.info(f"[MODE] Pre-market single run: analyzing {target_date}")
            run_once(target_date=target_date,
                     label=f"전일 {target_date[:4]}-{target_date[4:6]}-{target_date[6:]}")
        else:
            run_once()

    pythoncom.CoUninitialize()
    log.info("[DONE] Analysis complete.")


if __name__ == "__main__":
    main()