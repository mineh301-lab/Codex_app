---
name: coding-error-prevention
description: 코딩 작업 시 과거 반복 오류 패턴 방지. API rate limit, 디버깅 로그, 리페인팅, 인코딩, 캐시 전략, 성능, 코드 수정, 사전 테스트, 배치파일 CRLF 줄바꿈, 배치파일 if-else 중첩 금지, 관리자 권한 자동 상승, matplotlib/scipy 금지, 시뮬레이션 사전계산 등 체크리스트 제공. 코딩, 스크립트, 배치파일, PineScript, 백테스트, API 호출, 데이터 수집 키워드에 자동 활성화.
---
 
# 코딩 오류 방지 스킬
 
## 개요
 
이 스킬은 사용자와의 과거 대화에서 반복적으로 발생한 코딩 오류 패턴을 정리한 것입니다. **모든 코딩 작업 시작 전에 반드시 이 스킬을 읽고 해당되는 체크리스트를 확인한 후 코드 작성을 시작**해야 합니다.
 
**핵심 원칙**: 과거에 한 번이라도 발생한 오류는 반드시 재발합니다. 사전 체크만이 유일한 방지책입니다.
 
---
 
## 작업 전 공통 체크리스트
 
모든 코딩 작업에 공통으로 적용됩니다:
 
```
□ userMemories의 recent_updates 및 Other instructions 확인
□ 과거 대화에서 동일 유형 작업의 오류 패턴 검색 (conversation_search)
□ 해당 작업에 적용되는 고정 파라미터 확인 (RSI 14, BB 20/2, MACD 12/26/9 등)
□ 디버깅 로그: 모든 함수에 입력→처리→출력 log.info 포함 (log.debug 금지!)
□ 디버깅 로그: 이상 징후 자동 경고(warning) 포함 (빈 응답, 통과율 극히 낮음 등)
□ 디버깅 로그: --debug 안 켜도 기본 로그만으로 원인 파악 가능한지 자문
□ logging.basicConfig()이 모든 logging 호출보다 먼저 오는지 확인
□ 코드 전달 전 py_compile + mock 데이터 실행 테스트
```
 
---
 
## 오류 패턴 #1: API Rate Limit 위반 (심각도: Critical)
 
### 문제
 
외부 API 호출 시 rate limit을 사전에 확인하지 않고 공격적으로 요청하여 429/418 에러, IP 차단, 서비스 거부가 발생합니다.
 
### 과거 사고 이력
 
| 사고 | 원인 | 결과 |
|------|------|------|
| Binance 451 에러 | cafe24 서버에서 과다 호출 | IP ban |
| 바이낸스 선물 스크리닝 | 초당 9건 요청 | 429 에러 → 0.25초로 수정 |
| 미국 주식 스크리닝 (Yahoo Finance) | 0.15초 딜레이 | 429 확실 → 2초 권장 |
| Binance 분당 weight 초과 | 분당 1,200 weight 무시 | 반복 차단 |
 
### 주요 API별 Rate Limit (반드시 웹검색으로 최신값 재확인)
 
| API | Rate Limit | 권장 딜레이 |
|-----|-----------|------------|
| Binance REST | 분당 1,200 weight (IP 기준) | 요청 간 0.25초+ |
| Yahoo Finance (비공식) | ~360 req/hr, IP ban ~950 req | 2초+ |
| 키움 REST API | 초당 5회 (실투자), 시간당 1,000건 | 0.25초+ |
| 한국투자증권 | 초당 20회 | 0.1초+ |
| CoinGecko (무료) | 분당 10-30회 | 3초+ |
 
### 필수 체크리스트
 
```
□ 코드 작성 전 해당 API의 rate limit을 웹검색으로 확인했는가?
□ 공식 제한의 80% 이하로 보수적 설정했는가?
□ 429/418 에러 발생 시 adaptive backoff 로직이 있는가? (30-90초)
□ 랜덤 딜레이를 추가하여 패턴 감지를 회피하는가?
□ 연속 N회(3-5회) 실패 시 자동 중단하는 안전장치가 있는가?
□ 현재 사용량을 로그로 출력하는가? (호출 수/분, 남은 weight 등)
```
 
### 표준 구현 패턴
 
```python
import time
import random
import logging
 
logger = logging.getLogger(__name__)
 
class RateLimitedRequester:
    """Rate limit 준수 요청기 - 모든 외부 API 호출에 사용"""
    
    def __init__(self, base_delay=0.5, max_retries=3, backoff_base=30):
        self.base_delay = base_delay
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.call_count = 0
        self.error_count = 0
        self.start_time = time.time()
    
    def request(self, func, *args, **kwargs):
        for attempt in range(self.max_retries):
            try:
                # 랜덤 지터 포함 딜레이
                delay = self.base_delay + random.uniform(0, self.base_delay * 0.3)
                time.sleep(delay)
                
                result = func(*args, **kwargs)
                self.call_count += 1
                
                # 진행 로그 (100회마다)
                if self.call_count % 100 == 0:
                    elapsed = time.time() - self.start_time
                    rpm = self.call_count / (elapsed / 60) if elapsed > 0 else 0
                    logger.info(f"[API] 호출: {self.call_count}건 | "
                              f"속도: {rpm:.1f}req/min | "
                              f"에러: {self.error_count}건")
                
                return result
                
            except Exception as e:
                self.error_count += 1
                error_str = str(e)
                
                if '429' in error_str or '418' in error_str or '451' in error_str:
                    wait = self.backoff_base * (attempt + 1) + random.uniform(0, 10)
                    logger.warning(f"[RATE LIMIT] {error_str} → {wait:.0f}초 대기 "
                                  f"(시도 {attempt+1}/{self.max_retries})")
                    time.sleep(wait)
                else:
                    logger.error(f"[API ERROR] {error_str}")
                    raise
        
        raise Exception(f"최대 재시도 횟수({self.max_retries}) 초과")
```
 
---
 
## 오류 패턴 #2: 디버깅 로그 부족 (심각도: Critical)
 
### ⚠️ 대원칙: 로그 과잉은 용인, 로그 부족은 용납 불가
 
```
디버깅 로그 폭탄이 쏟아지는 것 → 용인 (콘솔 스크롤하면 됨)
로그 부족으로 버그 원인 파악 지연 → 용납 불가 (시간 낭비 + 재실행 필요)
 
따라서: 의심스러우면 무조건 찍는다. 빼는 건 나중에 할 수 있지만,
        안 찍은 로그는 재실행하지 않으면 영원히 볼 수 없다.
```
 
### 문제
 
로그를 생략하고 코드를 작성하면, 문제 발생 시 파일을 교체하며 디버깅해야 하므로 시간이 훨씬 더 소요됩니다. 특히 **장중에만 데이터가 발생하는 금융 API**의 경우, 장 마감 후에는 재현 자체가 불가능하므로 로그 누락의 피해가 치명적입니다.
 
### 과거 사고 이력
 
| 사고 | 원인 | 결과 |
|------|------|------|
| 국내주식 시차 상관계수 | 캐시 사용 여부 로그 없음 | 반복 수신 의심, 원인 파악 불가 |
| IVWAP 멀티프로세싱 | 실패 원인 로그 없음 | 디버깅 불가 |
| 키움+한투 병렬 수집 | 진행 상황 불명확 | 완료 여부 판단 불가 |
| 대량체결 분석기 v3.0 (2026-02) | logging.basicConfig 순서 오류 | 모든 log.info 숨겨짐 → "Done"만 표시 |
| **대량체결 분석기 v17 (2026-02-10)** | **종목별 틱 수/페이지 수/threshold 통과 건수 로그 없음** | **31종목 전부 총건수 1~2건인데 원인 파악 불가. 페이지네이션 문제인지, 데이터 구조 문제인지, 캐시 병합 문제인지 구분 불가. 장 마감 후 발견하여 재현도 불가** |
 
### 핵심 교훈: v17 대량체결 분석기 사례
 
v17의 Step 2 실행 로그는 이것이 전부였습니다:
 
```
[STEP 2] Stock #1: code=090710, name=휴림로봇, rate=5.39%   ← 첫 5종목만
[STEP 2] 20/36 | OK:20 Fail:0 ETN:0 | 3.4/sec | ETA:0.1min  ← 20건마다 요약
[STEP 2] Done: 36 OK, 0 fail, 0 ETN skipped (9.9s)          ← 최종 합계
```
 
이 로그만 보면 **"36종목 전부 성공, 에러 0건"**으로 보입니다. 하지만 실제로는:
- 종목당 API를 1페이지만 호출하고 멈춤 (cont_yn 상태 불명)
- 수집된 틱이 종목당 수십 건 수준 (전체 장중 틱의 극히 일부)
- 1억원 threshold를 통과한 블록이 종목당 0~2건 (데이터 부족 or 정상?)
- 캐시에서 기존 데이터가 얼마나 병합됐는지 불명
 
**찍혔어야 하는 로그:**
 
```
[TICK] 018880 한온시스템 | pages=1/50 cont_yn=N | raw=47ticks cached=0+47=47 | ≥1억: buy=2 sell=0 total=2 | last_tm=152843
[TICK] 047040 대우건설   | pages=3/50 cont_yn=N | raw=892ticks cached=0+892=892 | ≥1억: buy=1 sell=1 total=2 | last_tm=153000
[TICK] 056080 유진로봇   | pages=1/50 cont_yn=N | raw=31ticks cached=0+31=31 | ≥1억: buy=1 sell=0 total=1 | last_tm=152612
...
[STEP 2] SUMMARY: 36종목 | 총틱=4,231 avg=118/종목 | 총블록=48건 avg=1.3/종목 | pages: avg=1.2 max=3
[STEP 2] ⚠️ WARNING: 블록 건수 max=2 — 전 종목 2건 이하. 데이터 수집량 점검 필요!
```
 
이 로그가 있었으면 **한 번 돌리고 즉시** "페이지가 1밖에 안 나온다", "틱은 47개인데 블록은 2개뿐이다"를 파악할 수 있었습니다.
 
### ⚠️ logging.basicConfig() 순서 함정 (중요)
 
Python의 `logging.basicConfig()`는 **이미 루트 로거에 핸들러가 있으면 아무것도 하지 않습니다**.
모듈 최상단의 try/except에서 `logging.warning()`을 호출하면, Python이 내부적으로 **기본 핸들러(WARNING 레벨, stderr)**를 자동 생성합니다. 이후 `logging.basicConfig(level=INFO, stream=stdout)` 호출은 **무시**됩니다.
 
```python
# ❌ 금지: 이 순서로 작성하면 log.info()가 전부 숨겨짐
try:
    import some_module
except ImportError:
    logging.warning("module not found")  # ← 여기서 기본 핸들러 생성됨!
 
# 이 시점에서 이미 핸들러가 존재 → basicConfig 무시됨
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
log = logging.getLogger(__name__)
log.info("이 메시지는 절대 보이지 않음")  # WARNING 레벨 미만이라 출력 안됨
 
# ✅ 올바른: basicConfig를 모든 logging 호출보다 먼저 실행
import logging
import sys
 
logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
log = logging.getLogger(__name__)
 
# 이후에 try/except 등에서 log.warning() 사용
try:
    import some_module
except ImportError:
    log.warning("module not found")  # 이미 올바른 핸들러가 설정된 상태
```
 
**절대 규칙**: `logging.basicConfig()`은 **파일 최상단, import 직후, 다른 어떤 logging 호출보다 먼저** 실행해야 합니다. ensure_packages() 등 패키지 체크 함수보다도 먼저 와야 합니다.
 
### 디버깅 로그 3대 원칙
 
```
원칙 1: 모든 함수는 입력→처리→출력을 log.info로 찍는다
         → log.debug가 아니다. 기본 실행에서 보여야 한다.
         → --debug 안 켜도 원인 파악이 가능해야 한다.
 
원칙 2: "성공"도 상세히 찍는다
         → "OK" 한 줄이 아니라, 무엇이 얼마나 성공했는지
         → "36 OK"가 아니라 "36종목 OK, 총 4231틱, 블록 48건"
 
원칙 3: 이상 징후를 자동 경고한다
         → 데이터가 예상보다 적으면 WARNING
         → 빈 응답이면 WARNING
         → 모든 값이 동일하면 WARNING (의심 패턴)
```
 
### 함수 유형별 필수 로그 항목 (전부 log.info — DEBUG 아님!)
 
#### 유형 A: API 호출 함수
 
```python
def fetch_something(code, name, ...):
    # === 입력 로그 ===
    log.info(f"[FUNC] {code} {name} | 요청 파라미터: page={page}, limit={limit}")
 
    # === 각 API 호출마다 ===
    log.info(f"[API] {code} | page={page} | HTTP {resp.status_code} | "
             f"items={len(items)} | cont_yn={cont_yn} | next_key={next_key[:20]}")
 
    # === 루프 종료 사유 ===
    log.info(f"[API] {code} | 종료사유: {'cont_yn=N' if not cont else 'max_pages 도달' if page>=max else '빈 응답'} | "
             f"총 {page+1}페이지, {len(all_items)}건")
 
    # === 출력 로그 ===
    log.info(f"[FUNC] {code} {name} | 결과: {len(result)}건 | 소요: {elapsed:.1f}s")
 
    # === 이상 징후 경고 ===
    if len(result) == 0:
        log.warning(f"[FUNC] ⚠️ {code} {name} | 결과 0건 — 빈 응답 확인 필요")
    if len(result) < expected_min:
        log.warning(f"[FUNC] ⚠️ {code} {name} | 결과 {len(result)}건 < 예상 최소 {expected_min}건")
```
 
#### 유형 B: 데이터 분석/필터링 함수
 
```python
def analyze_something(data, threshold=100_000_000):
    total = len(data)
 
    # === 입력 로그 ===
    log.info(f"[ANALYZE] 입력: {total}건 | threshold: {threshold:,}")
 
    # === 처리 중 핵심 수치 ===
    passed = len([x for x in data if x['amount'] >= threshold])
    log.info(f"[ANALYZE] 필터 통과: {passed}/{total}건 ({passed/total*100:.1f}%) | "
             f"매수={buy_count} 매도={sell_count}")
 
    # === 이상 징후 경고 ===
    if passed == 0:
        log.warning(f"[ANALYZE] ⚠️ threshold {threshold:,} 통과 건수 0 — 기준값 점검 필요")
    if total > 0 and passed / total < 0.01:
        log.warning(f"[ANALYZE] ⚠️ 통과율 {passed/total*100:.2f}% 극히 낮음 — 데이터 or 기준값 점검")
 
    # === 출력 로그 ===
    log.info(f"[ANALYZE] 결과: buy_amt={buy_amt:,} sell_amt={sell_amt:,} net={net:,} | "
             f"ratio={ratio:.1f}% direction={direction}")
```
 
#### 유형 C: 캐시 병합 함수
 
```python
def merge_with_cache(code, new_data, cache):
    cached = cache.get(code, {})
    prev_count = len(cached.get("items", []))
    new_count = len(new_data)
 
    # === 병합 로그 ===
    merged = prev_items + new_items
    log.info(f"[CACHE] {code} | 기존={prev_count} + 신규={new_count} = 병합={len(merged)} | "
             f"last_time: {cached.get('last_time','없음')} → {new_last_time}")
 
    # === 이상 징후 경고 ===
    if prev_count > 0 and new_count == 0:
        log.warning(f"[CACHE] ⚠️ {code} | 신규 데이터 0건 — API 응답 확인 필요")
    if new_count > 0 and len(merged) == prev_count:
        log.warning(f"[CACHE] ⚠️ {code} | 병합 후 건수 변화 없음 — 중복 데이터?")
```
 
#### 유형 D: 메인 루프 (Step/Phase 단위)
 
```python
# === 각 종목 처리 후 1줄 요약 (전 종목 찍기 — 생략 금지!) ===
for i, stock in enumerate(stocks):
    # ... 처리 ...
    log.info(f"[STEP 2] #{i+1:>3d}/{len(stocks)} {code} {name:12s} | "
             f"pages={pages} ticks={tick_count} blocks={block_count} | "
             f"buy={buy_amt:,.0f} sell={sell_amt:,.0f} net={net:,.0f}")
 
# === Step 완료 시 통계 요약 ===
log.info(f"[STEP 2] {'='*60}")
log.info(f"[STEP 2] SUMMARY: {total}종목 | "
         f"총틱={sum_ticks:,} avg={sum_ticks//total}/종목 | "
         f"총블록={sum_blocks}건 avg={sum_blocks/total:.1f}/종목 | "
         f"pages: avg={avg_pages:.1f} max={max_pages}")
 
# === 이상 징후 자동 경고 ===
if max_blocks <= 2:
    log.warning(f"[STEP 2] ⚠️ 전 종목 블록 건수 max={max_blocks} — 데이터 수집량 심각하게 부족!")
if avg_ticks < 50:
    log.warning(f"[STEP 2] ⚠️ 종목당 평균 틱 {avg_ticks:.0f}건 — 페이지네이션 또는 API 응답 점검 필요")
if zero_block_pct > 0.5:
    log.warning(f"[STEP 2] ⚠️ 블록 0건 종목 {zero_block_count}/{total}개 ({zero_block_pct:.0%}) — threshold 점검 필요")
```
 
### 이상 징후 자동 경고 필수 목록
 
아래 상황이 감지되면 **반드시 log.warning()으로 경고**해야 합니다:
 
```
□ API 응답이 빈 배열/None → "⚠️ 빈 응답"
□ 연속 조회(cont_yn)가 첫 페이지에서 바로 N → "⚠️ 1페이지만 수신"
□ 필터 통과 건수가 전체의 1% 미만 → "⚠️ 통과율 극히 낮음"
□ 전체 종목에서 최대 건수가 2 이하 → "⚠️ 데이터 부족 의심"
□ 캐시 병합 후 건수 변화 없음 → "⚠️ 중복 또는 빈 신규 데이터"
□ 예상 소요시간 대비 실제 소요시간이 50% 이상 차이 → "⚠️ 성능 이상"
□ 모든 종목의 값이 동일 (예: 전부 ratio=50%) → "⚠️ 의심 패턴"
```
 
### 필수 체크리스트 (코드 작성 완료 후 반드시 검증)
 
```
□ 모든 함수에 입력→처리→출력 로그가 log.info로 있는가? (log.debug 금지!)
□ API 호출 함수: 페이지 수, cont_yn, 응답 건수, 종료 사유가 찍히는가?
□ 분석 함수: 전체 건수, 필터 통과 건수, 통과율이 찍히는가?
□ 캐시 함수: 기존 건수 + 신규 건수 = 병합 건수가 찍히는가?
□ 메인 루프: 종목별 1줄 요약이 전 종목에 대해 찍히는가? (첫 5개만 아님!)
□ Step 완료 시: 평균/최소/최대 통계 요약이 있는가?
□ 이상 징후 자동 경고(warning)가 7가지 이상 포함되어 있는가?
□ --debug 안 켜도 기본 실행 로그만으로 원인 파악이 가능한가?
□ logging.basicConfig()이 모든 logging 호출보다 먼저 위치하는가?
```
 
### --verbose 옵션 패턴
 
기본(INFO)에서 이미 충분한 로그가 나오되, 더 상세한 원시 데이터 확인이 필요할 때 사용합니다.
 
```python
import argparse
import logging
 
parser = argparse.ArgumentParser()
parser.add_argument('--verbose', '-v', action='count', default=0,
                    help='로그 상세도 (기본: INFO, -v: DEBUG raw data)')
parser.add_argument('--debug', action='store_true',
                    help='최대 상세 로그 (API 원시 응답 포함)')
args = parser.parse_args()
 
if args.debug:
    log_level = logging.DEBUG
elif args.verbose:
    log_level = logging.DEBUG
else:
    log_level = logging.INFO  # ← 기본이 INFO. 이 레벨에서 원인 파악 가능해야 함!
 
logging.basicConfig(
    level=log_level,
    format='[%(levelname)s] %(asctime)s | %(message)s',
    datefmt='%H:%M:%S'
)
```
 
```
기본 (INFO):  함수별 입출력 요약 + 이상 징후 경고 + Step 통계 → 원인 파악 가능
--verbose:    + API 원시 응답 키/값 샘플 + 개별 틱 데이터 샘플
--debug:      + 전체 API request/response body + 캐시 파일 내용
```
 
---
 
## 오류 패턴 #3: 리페인팅 (PineScript/백테스트) (심각도: Critical — 64만원 손실 전례)
 
### 문제
 
미완성봉(현재 진행 중인 봉) 데이터를 참조하면, 시그널이 나타났다가 봉 마감 시 사라지는 리페인팅이 발생합니다. 실거래에서 허위 시그널로 진입하여 손실을 입습니다.
 
### 과거 사고 이력
 
| 사고 | 원인 | 결과 |
|------|------|------|
| Volume Fitness PineScript | `volume` (현재봉) 사용 | 리페인팅 발생 → 64만원 손실 |
| 1분봉 백테스트 시그널 | 시그널봉 마감 전 진입 가정 | 비현실적 수익률 |
 
### 절대 규칙 (예외 없음)
 
```
PineScript:
  ✅ volume[1], close[1], high[1], low[1], open[1]  (확정봉)
  ❌ volume, close, high, low, open  (현재 미완성봉)
 
Python 백테스트:
  ✅ vol_ratio = df['volume'].shift(1) / df['volume'].rolling(20).mean().shift(1)
  ❌ vol_ratio = df['volume'] / df['volume'].rolling(20).mean()
 
진입가:
  ✅ 시그널 발생 봉[i]의 다음 봉[i+1] 시가(open)로 진입
  ❌ 시그널 발생 봉[i]의 종가(close)로 진입
```
 
### 체크리스트
 
```
□ 모든 가격/거래량 참조에 [1] 또는 .shift(1)을 사용하는가?
□ 이동평균/지표 계산도 확정봉 기준인가?
□ 진입가가 시그널 다음봉 시가(open)인가?
□ barstate.isconfirmed 또는 동등 로직을 사용하는가?
```
 
---
 
## 오류 패턴 #4: 배치파일 인코딩 (심각도: High)
 
### 문제
 
Windows CMD는 기본 코드 페이지가 CP949(한글)이므로, UTF-8로 저장된 .bat 파일의 한글이 깨지면서 명령 해석 오류가 발생합니다.
 
### 증상
 
```
'91'은(는) 내부 또는 외부 명령, 실행할 수 있는 프로그램, 또는
배치 파일이 아닙니다.
'?맞실,'은(는) 내부 또는 외부 명령...
```
 
### 절대 규칙
 
```
□ .bat 파일에 한글 절대 사용 금지 → 모든 텍스트를 영어로 작성
□ echo 메시지, 주석(REM), 변수명, 폴더명 전부 영어
□ chcp 65001은 불안정하므로 의존하지 않음
□ 모든 에러 처리 블록에 pause 명령 포함 (창 즉시 종료 방지)
□ Python 경로 찾기: py, python, python3 순서로 체크
□ [#15 참조] 반드시 CRLF 줄바꿈 + ANSI 인코딩으로 생성 (create_file 금지!)
□ [#16 참조] 관리자 권한이 필요한 경우 자동 권한 상승 코드 포함
```
 
### 표준 배치파일 템플릿
 
```batch
@echo off
setlocal enabledelayedexpansion
 
REM === Check Python ===
set PYTHON_CMD=
where py >nul 2>&1 && set PYTHON_CMD=py && goto :found_python
where python >nul 2>&1 && set PYTHON_CMD=python && goto :found_python
where python3 >nul 2>&1 && set PYTHON_CMD=python3 && goto :found_python
 
echo [ERROR] Python not found. Please install Python.
pause
exit /b 1
 
:found_python
echo [INFO] Using: %PYTHON_CMD%
 
REM === Check required packages ===
%PYTHON_CMD% -c "import pandas" >nul 2>&1
if errorlevel 1 (
    echo [INFO] Installing pandas...
    %PYTHON_CMD% -m pip install pandas --quiet
)
 
REM === Run main script ===
%PYTHON_CMD% "%~dp0main.py" %*
if errorlevel 1 (
    echo [ERROR] Script failed with exit code %errorlevel%
    pause
    exit /b %errorlevel%
)
 
echo [DONE] Completed successfully.
pause
```
 
---
 
## 오류 패턴 #5: 캐시 전략 미흡 (심각도: High)
 
### 문제
 
캐시 만료 시 전체 데이터를 처음부터 다시 수집하면 시간이 크게 낭비됩니다.
 
### 절대 규칙
 
```
□ 캐시 만료 시 → 전체 재수집이 아닌 증분 수집+병합 방식
□ 기존 데이터는 유지하고 신규분만 이어붙이기
□ 캐시 상태를 로그로 명확히 표시 (HIT/MISS/EXPIRED/MERGE)
□ 캐시 유효시간을 용도에 맞게 설정 (실시간: 1분, 일봉: 4시간 등)
```
 
### 표준 구현 패턴
 
```python
import os
import json
import time
import logging
 
logger = logging.getLogger(__name__)
 
class IncrementalCache:
    """증분 수집+병합 캐시"""
    
    def __init__(self, cache_dir, ttl_seconds=14400):  # 기본 4시간
        self.cache_dir = cache_dir
        self.ttl = ttl_seconds
        os.makedirs(cache_dir, exist_ok=True)
    
    def get(self, key):
        path = os.path.join(self.cache_dir, f"{key}.json")
        if not os.path.exists(path):
            logger.info(f"[CACHE MISS] {key} - 파일 없음")
            return None
        
        mtime = os.path.getmtime(path)
        age = time.time() - mtime
        
        with open(path, 'r') as f:
            data = json.load(f)
        
        if age > self.ttl:
            logger.info(f"[CACHE EXPIRED] {key} - {age/3600:.1f}시간 경과 → 증분 수집 필요")
            return {"data": data, "expired": True, "last_timestamp": data.get("last_timestamp")}
        
        logger.info(f"[CACHE HIT] {key} - {age/60:.0f}분 전 ({len(data.get('records', []))}건)")
        return {"data": data, "expired": False}
    
    def merge(self, key, new_records, timestamp_key="timestamp"):
        """기존 데이터에 신규 데이터를 병합"""
        existing = self.get(key)
        
        if existing and existing["data"].get("records"):
            old_records = existing["data"]["records"]
            # 중복 제거 병합
            existing_timestamps = {r[timestamp_key] for r in old_records}
            added = [r for r in new_records if r[timestamp_key] not in existing_timestamps]
            merged = old_records + added
            logger.info(f"[CACHE MERGE] {key} - 기존 {len(old_records)}건 + 신규 {len(added)}건 = {len(merged)}건")
        else:
            merged = new_records
            logger.info(f"[CACHE NEW] {key} - {len(merged)}건 저장")
        
        merged.sort(key=lambda x: x[timestamp_key])
        
        path = os.path.join(self.cache_dir, f"{key}.json")
        with open(path, 'w') as f:
            json.dump({
                "records": merged,
                "last_timestamp": merged[-1][timestamp_key] if merged else None
            }, f, ensure_ascii=False)
        
        return merged
```
 
---
 
## 오류 패턴 #6: 코드 수정 시 원본 훼손 (심각도: Critical)
 
### 문제
 
기존 코드를 수정할 때, 복사와 수정을 동시에 진행하면 원본의 설정값이나 UI 언어가 의도치 않게 변경됩니다. 사용자가 전달한 모든 코드는 **반드시 전체 복사를 먼저 완료한 후 부분 수정**해야 합니다.
 
### 과거 사고 이력
 
- 필터 기준값이 임의로 변경됨
- 한글 UI가 영어로 바뀜
- "개선"이라며 요청하지 않은 변경 추가
- 복사하면서 동시에 수정하다가 원본 로직 훼손
 
### 핵심 원칙: 전체복사 먼저 → 부분수정 (절대 동시 진행 금지)
 
```
[올바른 순서]
1단계: 사용자가 전달한 코드를 100% 그대로 전체 복사하여 새 파일 생성
       → 이 시점에서 원본과 1바이트도 다르면 안 됨
2단계: 복사 완료 후, 요청된 변경사항만 부분적으로 수정
3단계: 수정 후 diff 비교로 의도하지 않은 변경 확인
 
[금지 행위]
❌ 코드를 복사하면서 동시에 수정하는 것
❌ "이 부분은 더 좋은 방식이 있으니" 하며 임의 개선
❌ 원본의 변수명, UI 언어, 기준값을 임의로 변경
❌ 요청하지 않은 함수 추가/삭제
```
 
### 절대 규칙
 
```
□ 수정 전: 원본의 UI 언어(한글/영어), 필터 기준값, 키 이름을 먼저 확인
□ 수정 순서: 전체 복사 먼저 → 그 후 부분 수정 (동시 진행 절대 금지!)
□ 요청된 변경만 수행 (임의 "개선" 절대 금지)
□ 수정 후: 원본과 diff 비교하여 의도하지 않은 변경 확인
□ 고정 파라미터 변경 여부 확인: RSI=14, BB=(20,2), MACD=(12,26,9)
```
 
---
 
## 오류 패턴 #7: 성능 문제 (시간복잡도 폭발) (심각도: High)
 
### 문제
 
반복문 내에서 무거운 연산(DataFrame 생성, 파일 I/O, API 호출)을 매번 수행하면 시간복잡도가 폭발합니다.
 
### 판단 기준
 
```
반복횟수 × 단위연산비용 계산 필수:
  - 1,000회 미만: for 루프 OK
  - 1,000회 이상: pandas corrwith, vectorize 등 벡터 연산 우선
  - 10,000회 이상: 벡터 연산 필수 + 병렬화 검토
  - 100,000회 이상: 청크 처리 + 멀티프로세싱 필수
```
 
### 금지 패턴
 
```python
# ❌ 금지: 루프 내 DataFrame 복사/생성
for symbol in symbols:  # 2,500개
    df_temp = df.copy()  # 매번 복사
    df_temp = df_temp.merge(other_df, on='date')  # 매번 merge
    result = df_temp.corr()  # 매번 상관계수 계산
 
# ✅ 올바른 방식: 벡터 연산
pivot = df.pivot(index='date', columns='symbol', values='close')
corr_matrix = pivot.corr()  # 한 번에 계산
```
 
### 체크리스트
 
```
□ 코드 작성 전 반복횟수 × 단위연산비용 계산했는가?
□ 1,000회+ 반복이면 벡터 연산으로 대체했는가?
□ 함수가 반복 호출될 때 무거운 연산이 매번 실행되지 않는가?
□ DataFrame 생성, 파일 I/O가 루프 밖에 있는가?
□ 캐싱/재사용 구조를 적용했는가?
```
 
---
 
## 오류 패턴 #8: 전달 전 테스트 미흡 (심각도: High)
 
### 문제
 
문법 검사(py_compile)만 통과하고 실행 테스트를 생략하면, dtype 오류, NaN 처리 오류, timezone 처리 오류 등이 사용자 환경에서 발생합니다.
 
### 필수 테스트 항목
 
```
□ py_compile 문법 검사 (기본)
□ mock 데이터로 실행 테스트 (numpy/pandas 연산 포함)
□ dtype 관련 테스트 (int↔float 변환, NaN 할당, 문자열↔숫자)
□ 엣지 케이스: 빈 데이터, 단일 행, 음수값, 결측치
□ 엑셀 파일이라면: 구글 스프레드시트 호환성 확인 (*1 형변환)
```
 
### mock 테스트 패턴
 
```python
import pandas as pd
import numpy as np
 
# mock 데이터 생성
mock_df = pd.DataFrame({
    'timestamp': pd.date_range('2025-01-01', periods=100, freq='1h'),
    'open': np.random.uniform(90000, 110000, 100),
    'high': np.random.uniform(100000, 120000, 100),
    'low': np.random.uniform(80000, 100000, 100),
    'close': np.random.uniform(90000, 110000, 100),
    'volume': np.random.uniform(1, 100, 100),
})
 
# 엣지 케이스 주입
mock_df.loc[5, 'volume'] = 0       # 거래량 0
mock_df.loc[10, 'close'] = np.nan  # 결측치
mock_df.loc[15, 'volume'] = np.nan # 결측치
 
# 함수 실행 테스트
try:
    result = your_function(mock_df)
    print(f"[TEST PASS] 결과 shape: {result.shape}, dtype: {result.dtypes}")
except Exception as e:
    print(f"[TEST FAIL] {type(e).__name__}: {e}")
```
 
---
 
## 오류 패턴 #9: matplotlib/scipy 사용 (심각도: High)
 
### 문제
 
사용자 환경(Windows, Python 3.9 32-bit)에서 matplotlib, scipy 등 C 확장 모듈 설치 시 C++ 빌드 툴(Meson, MSVC) 관련 오류가 발생합니다. scipy의 경우 `--only-binary :all:` 옵션으로 wheel 설치가 가능하지만, numpy 버전 다운그레이드를 유발하여 **ILLEGAL_INSTRUCTION(0xC000001D) 크래시**로 Python이 즉사합니다.
 
### 과거 사고 이력 (scipy)
 
| 사고 | 원인 | 결과 |
|------|------|------|
| 백테스트 v2 (2026-02-15) | `pip install scipy` → Meson 빌드 실패 (C 컴파일러 없음) | scipy 설치 실패 |
| 동일 건 wheel 설치 시도 | `--only-binary :all:` → scipy 1.9.1 설치 성공, 단 numpy 1.26.4→1.24.4 다운그레이드 | numpy 1.24.4 win32 빌드가 CPU 미지원 명령어 사용 → `import numpy`만으로 ILLEGAL_INSTRUCTION 즉사 |
| 동일 건 연쇄 피해 | numpy 즉사 → pandas, arch 등 numpy 의존 패키지 전부 import 불가 | 모든 Python 스크립트 실행 불가, numpy 원복 필요 |
 
### 절대 규칙
 
```
□ matplotlib 사용 절대 금지
□ scipy 사용 절대 금지 — 대체 구현 사용:
  - t-test p-value: math.erfc 기반 정규근사 (거래수 30+이면 정밀도 충분)
  - 이항분포 검정: 정규근사 + 연속성 보정
  - 기타 통계: numpy만으로 구현 가능한지 먼저 검토
□ 차트가 필요하면 Chart.js (HTML 기반) 사용
□ 또는 plotly (pip install plotly → HTML 출력)
□ 간단한 표는 콘솔 출력 (tabulate, prettytable)
```
 
### scipy 대체 구현 표준 패턴
 
```python
import math
 
def normal_cdf(x):
    """표준정규분포 CDF (math.erfc 기반, ~15자리 정확도)"""
    return 0.5 * math.erfc(-x / math.sqrt(2))
 
def t_cdf(t_val, df):
    """t-분포 CDF. df>=30이면 정규근사, 미만이면 Cornish-Fisher 보정."""
    if df >= 30:
        return normal_cdf(t_val)
    g1 = (t_val**3 + t_val) / (4 * df)
    g2 = (5*t_val**5 + 16*t_val**3 + 3*t_val) / (96 * df**2)
    return normal_cdf(t_val + g1 + g2)
 
def binom_sf(k, n, p):
    """이항분포 생존함수 P(X > k). 정규근사 + 연속성 보정."""
    mu = n * p
    sigma = math.sqrt(n * p * (1 - p))
    if sigma == 0:
        return 0.0
    z = (k - mu + 0.5) / sigma
    return 1 - normal_cdf(z)
```
 
### ⚠️ --no-deps 설치 시 런타임 import 실패 함정
 
`pip install pykrx --no-deps`로 matplotlib을 빼고 설치해도, pykrx가 **내부적으로 `import matplotlib`을 실행**하여 ImportError가 발생합니다. 이 경우 matplotlib mock module을 주입해야 합니다.
 
### 과거 사고 이력
 
| 사고 | 원인 | 결과 |
|------|------|------|
| 대량체결 분석기 v3.0 (2026-02) | pykrx --no-deps 설치 후 import 시 matplotlib ImportError | pykrx 사용 불가 → ka10032 only 모드 fallback |
 
### matplotlib mock 표준 패턴
 
제3자 패키지가 내부적으로 matplotlib을 import하지만, 차트 기능을 사용하지 않는 경우에 적용합니다.
 
```python
import sys
import types
 
def inject_matplotlib_mock():
    """matplotlib mock module 주입 (차트 미사용 시)"""
    if "matplotlib" in sys.modules:
        return  # 이미 설치되어 있으면 불필요
 
    class _MockModule(types.ModuleType):
        """속성 접근 시 no-op 함수 반환하는 mock module"""
        def __init__(self, name):
            super().__init__(name)
            self.__path__ = []
            self.__version__ = "0.0.0"
        def __getattr__(self, item):
            if item.startswith("__") and item.endswith("__"):
                raise AttributeError(item)  # dunder 재귀 방지
            return lambda *a, **kw: None
        def __call__(self, *args, **kwargs):
            return None
 
    mock_mpl = _MockModule("matplotlib")
    for sub in ["pyplot", "dates", "figure", "axes", "ticker",
                "font_manager", "colors", "cm", "style"]:
        m = _MockModule(f"matplotlib.{sub}")
        sys.modules[f"matplotlib.{sub}"] = m
        setattr(mock_mpl, sub, m)  # import X.Y as Z 패턴 지원
    sys.modules["matplotlib"] = mock_mpl
 
# 사용: pykrx 등 matplotlib 의존 패키지 import 전에 호출
inject_matplotlib_mock()
from pykrx import stock as krx_stock  # 이제 ImportError 안 남
```
 
### 주의사항
 
```
□ mock 주입은 반드시 해당 패키지 import 전에 실행
□ __getattr__에서 dunder 속성은 AttributeError를 raise해야 함 (안 하면 RecursionError)
□ setattr로 하위 모듈을 부모 모듈의 속성으로도 등록해야 함 (from X import Y 패턴 지원)
□ 실제 matplotlib이 설치된 환경에서는 mock 주입 불필요 (조건 체크 필수)
```
 
---
 
## 오류 패턴 #9-2: 패키지 미설치 (ModuleNotFoundError) (심각도: High)
 
### 문제
 
Python 스크립트가 필요한 패키지가 설치되지 않은 환경에서 실행되어 `ModuleNotFoundError`로 즉시 중단됩니다. 사용자가 직접 pip install을 해야 하는 상황이 반복됩니다.
 
### 과거 사고 이력
 
| 사고 | 패키지 | 에러 |
|------|--------|------|
| 이미지 분할 스크립트 | Pillow | `No module named 'PIL'` |
| 나랑AI 프로젝트 | dotenv | `Cannot find package 'dotenv'` |
| 백테스트 실행 | ccxt, openpyxl | `ModuleNotFoundError` |
| 빗썸AI 코드 | openpyxl 자주 누락 | 엑셀 저장 실패 |
 
### 절대 규칙
 
```
□ 모든 Python 스크립트 상단에 패키지 자동 체크/설치 함수 포함
□ 배치파일에도 pip install 체크 로직 포함
□ 설치 실패 시 명확한 에러 메시지와 수동 설치 방법 안내
□ 설치 후 import 실패 시 프로세스 자동 재시작 (Pillow 사례)
```
 
### 표준 구현 패턴 (Python 스크립트 최상단)
 
```python
import subprocess
import sys
 
def ensure_packages(*packages):
    """필요한 패키지가 없으면 자동 설치"""
    missing = []
    for pkg in packages:
        # import 이름과 pip 이름이 다른 경우 처리
        import_name = {
            'Pillow': 'PIL',
            'python-dotenv': 'dotenv',
            'beautifulsoup4': 'bs4',
            'scikit-learn': 'sklearn',
        }.get(pkg, pkg)
        
        try:
            __import__(import_name)
        except ImportError:
            missing.append(pkg)
    
    if missing:
        print(f"[SETUP] 미설치 패키지 발견: {', '.join(missing)}")
        print(f"[SETUP] 자동 설치 중...")
        for pkg in missing:
            try:
                subprocess.check_call(
                    [sys.executable, '-m', 'pip', 'install', pkg, '--quiet'],
                    stdout=subprocess.DEVNULL
                )
                print(f"  ✓ {pkg} 설치 완료")
            except subprocess.CalledProcessError:
                print(f"  ✗ {pkg} 설치 실패 → 수동 설치: pip install {pkg}")
                sys.exit(1)
        
        print("[SETUP] 설치 완료. 패키지 로딩 중...")
 
# 사용 예시: 스크립트 최상단에서 호출
ensure_packages('pandas', 'numpy', 'requests', 'openpyxl', 'ccxt')
 
# 이후 정상 import
import pandas as pd
import numpy as np
```
 
### 배치파일 표준 패턴 (기존 템플릿 보완)
 
```batch
REM === Check and install required packages ===
echo [INFO] Checking required packages...
 
for %%P in (pandas numpy requests openpyxl ccxt) do (
    %PYTHON_CMD% -c "import %%P" >nul 2>&1
    if errorlevel 1 (
        echo [INSTALL] %%P not found. Installing...
        %PYTHON_CMD% -m pip install %%P --quiet
    ) else (
        echo [OK] %%P
    )
)
```
 
---
 
## 오류 패턴 #10: 고정 파라미터 위반 (심각도: Medium)
 
### 문제
 
사용자가 명시적으로 고정한 기술적 지표 파라미터를 임의로 변경합니다.
 
### 고정 파라미터 목록 (절대 변경 금지)
 
| 지표 | 파라미터 | 값 |
|------|---------|-----|
| RSI | 기간 | **14** (고정) |
| 볼린저밴드 | 기간, 표준편차 | **(20, 2)** (고정) |
| MACD | fast, slow, signal | **(12, 26, 9)** (고정) |
 
변경이 필요해 보여도 **절대 임의 변경하지 않고**, 사용자에게 "파라미터를 변경하시겠습니까?"라고 명시적으로 확인합니다.
 
---
 
## 오류 패턴 #11: 13개 통계지표 누락 (심각도: Medium)
 
### 문제
 
백테스트 결과 출력 시 필수 통계지표가 빠집니다.
 
### 필수 13개 통계지표 (전부 출력 필수)
 
```
1.  캔들당 평균수익률
2.  누적수익률
3.  CAGR (연간복리수익률)
4.  MDD (최대낙폭)
5.  평균보유기간
6.  총거래횟수
7.  승리거래 평균수익률
8.  패배거래 평균손실률
9.  수익률 표준편차
10. 손익비 (Profit Factor)
11. 승률
12. 횡보비율
13. 비트코인 베타계수
```
 
---
 
## 오류 패턴 #12: PineScript 줄바꿈 및 한글 변수명 오류 (심각도: High)
 
### 문제
 
PineScript(TradingView)에서 함수 호출이나 삼항 연산자를 여러 줄에 걸쳐 작성하면 문법 오류가 발생합니다. 또한 변수명에 한글을 사용하면 컴파일 오류가 발생합니다.
 
### 과거 사고 이력
 
| 사고 | 원인 | 결과 |
|------|------|------|
| AMM Price Impact v2 | input.float() 줄바꿈 | 문법 오류 → 한 줄로 재작성 |
| Larry Williams Position Sizing | 삼항 연산자 여러 줄 | 문법 오류 → switch 문으로 교체 |
| IVWAP Trend Strength | 삼항 연산자 줄바꿈 | "오류" 보고 → 한 줄로 수정 |
| Volume Fitness v2 | `auto_mcap_원` 한글 변수명 | 컴파일 오류 → 영문으로 변경 |
| Inverse VWAP | `timeframe = ""` 불필요 파라미터 | 오류 → 제거 |
 
### 절대 규칙
 
```
□ input.xxx() 함수는 반드시 한 줄에 작성 (줄바꿈 금지)
□ 삼항 연산자(? :)를 여러 줄에 걸쳐 쓰지 않음 → switch 문 사용
□ 변수명에 한글 절대 사용 금지 (주석의 한글은 OK)
□ 긴 파라미터는 tooltip에 넣되, 함수 호출 자체는 한 줄 유지
□ options=[] 배열도 한 줄에 작성
```
 
### 잘못된 예 vs 올바른 예
 
```pinescript
// ❌ 금지: 여러 줄 삼항 연산자
var table_pos = table_position == "top_left" ? position.top_left :
                table_position == "top_center" ? position.top_center :
                position.bottom_right
 
// ✅ 올바른: switch 문 사용
get_table_position() =>
    switch table_position
        "top_left" => position.top_left
        "top_center" => position.top_center
        => position.bottom_right
 
// ❌ 금지: input 줄바꿈
mcap_mode = input.string("Auto", "Market Cap Mode", 
     options=["Auto", "Manual"],
     tooltip="Auto mode...")
 
// ✅ 올바른: 한 줄
mcap_mode = input.string("Auto", "Market Cap Mode", options=["Auto", "Manual"], tooltip="Auto mode...")
 
// ❌ 금지: 한글 변수명
auto_mcap_원 = shares * close
 
// ✅ 올바른: 영문 변수명
auto_mcap_krw = shares * close
```
 
---
 
## 오류 패턴 #13: 코드 절단 (심각도: Medium)
 
### 문제
 
긴 코드를 작성할 때 중간에 끊겨서 불완전한 코드가 전달됩니다.
 
### 대응 방법
 
```
□ 긴 코드(200줄+)는 파일로 생성하여 전달
□ 함수별로 분리하여 모듈화
□ "# ... (이하 생략)" 같은 표현 절대 사용 금지
□ 코드 전달 후 "완전한 코드입니다" 명시
```
 
---
 
## 오류 패턴 #14: 구글 스프레드시트 호환성 (심각도: Critical — 손절경고 미작동 사례)
 
### 문제
 
xlsx 파일의 숫자 비교 수식이 구글 스프레드시트에서 실패합니다. 텍스트로 입력된 숫자와 실제 숫자를 비교할 때 `*1` 형변환 없이는 비교가 작동하지 않습니다.
 
### 과거 사고 이력
 
| 사고 | 원인 | 결과 |
|------|------|------|
| 손절경고 미작동 (2025-01-29) | 숫자 비교 수식에 *1 미적용 | 실제 금전적 손실 발생 |
 
### 절대 규칙
 
```
□ 모든 숫자 비교 수식에 *1 형변환 적용 (예: F40*1<=C$20*1)
□ 조건부 서식도 동일하게 *1 형변환
□ 파일 전달 전 셀프QA:
  - 텍스트로 숫자 입력 → 수식 작동 확인
  - 빈 셀, 음수 등 엣지 케이스 테스트
□ gsheet-compatible-xlsx 스킬도 함께 참조
```
 
---
 
## 오류 패턴 #15: 배치파일 줄바꿈/인코딩 파일 생성 오류 (심각도: Critical — 모든 pause 무력화)
 
### 문제
 
Claude의 `create_file` 도구는 Linux 환경에서 실행되므로 **줄바꿈이 LF(0x0A)**로 생성됩니다. Windows CMD는 **CRLF(0x0D 0x0A)**만 정상 파싱하므로, LF 전용 .bat 파일은 **구문 해석 자체가 실패**하여 어떤 pause도 실행되지 않고 즉시 종료됩니다.
 
또한 `create_file`은 UTF-8로 저장하므로, 한글이 없더라도 BOM이나 멀티바이트 시퀀스가 섞이면 CMD가 오동작할 수 있습니다.
 
### ⚠️ 왜 이것이 가장 치명적인가
 
다른 배치파일 오류(한글, pause 누락 등)는 부분적 실패지만, **LF 줄바꿈은 파일 전체를 무력화**합니다:
 
```
LF 줄바꿈 파일:
  @echo off\nsetlocal...\npause
  ↓ CMD가 해석
  "@echo off\nsetlocal...\npause"  ← 전체가 1줄로 인식 → 구문 에러 → 즉시 종료
 
CRLF 줄바꿈 파일:
  @echo off\r\nsetlocal...\r\npause
  ↓ CMD가 해석
  Line 1: @echo off
  Line 2: setlocal...     ← 정상 파싱
  Line N: pause            ← 정상 실행
```
 
**pause가 있어도, call :subroutine 구조로 감싸도, 어떤 안전장치를 넣어도 LF면 전부 무용지물입니다.**
 
### 과거 사고 이력
 
| 사고 | 원인 | 결과 |
|------|------|------|
| auto_clicker.bat (2025-10-25) | create_file로 생성 → LF | 관리자 권한 실행해도 즉시 닫힘 |
| bithumb_backtest_robust.bat (2025-10-30) | create_file로 생성 → LF + 한글 | 한글 깨짐 + 즉시 닫힘 |
| run_block_trade_v18.bat (2026-02-10) | create_file로 생성 → LF 137줄, CRLF 0줄 | DEBUG echo조차 안 보이고 즉시 닫힘 |
| 동일 파일 재생성 시도 3회 (2026-02-10) | 매번 create_file 사용 → 매번 LF | 3번 연속 동일 실패 |
| run_block_trade_v18.bat (2026-02-11) | echo 안의 `(32-bit)` → `)` 가 if 블록 닫기로 인식 | 32-bit Python 찾았는데 에러 메시지 출력 후 종료 |
 
### ⚠️ 추가 함정: echo 안의 괄호가 if 블록을 깨뜨림
 
CRLF 문제를 해결해도 **echo 텍스트 안의 `)`가 if 블록의 닫는 괄호로 인식**되어 제어 흐름이 깨지는 문제가 있습니다. 이것은 CMD의 구문 파서가 문자열 리터럴 안의 특수문자를 구분하지 못하기 때문입니다.
 
```
[재현 예시]
 
if "!PYTHON_CMD!"=="" (
    echo Please download Windows installer (32-bit)
    REM → CMD가 (32-bit) 의 ) 를 if 블록 닫기로 인식!
    REM → 이 줄에서 if 블록이 끝남
    echo This line runs UNCONDITIONALLY  ← 항상 실행됨!
    echo And so does this one             ← 항상 실행됨!
)
REM → 이 ) 는 매칭할 ( 가 없으므로 구문 에러
 
[실제 사고 - 2026-02-11]
 
if "!PYTHON_CMD!"=="" (
    echo   3. Click "Windows installer (32-bit)"    ← 여기서 if 끝남
    echo   4. During install, CHECK "Add to PATH"   ← 무조건 실행
    echo   5. Re-run this batch file                ← 무조건 실행
)
 
결과: 32-bit Python을 정상적으로 찾았는데도 에러 안내가 출력되고 종료됨
```
 
### 괄호 이스케이프 절대 규칙
 
```
if/for 블록 안의 echo에서 반드시 이스케이프해야 하는 특수문자:
  ( → ^(
  ) → ^)
  & → ^&
  | → ^|
  < → ^<
  > → ^>
  % → %% (환경변수 아닌 리터럴일 때)
 
예시:
  ❌ echo Download Windows installer (32-bit)
  ✅ echo Download Windows installer ^(32-bit^)
 
  ❌ echo Use pip install pandas & numpy
  ✅ echo Use pip install pandas ^& numpy
 
블록 밖의 echo는 이스케이프 불필요:
  echo This (works) fine outside if blocks   ← OK
```
 
### ⚠️ 추가 함정: if-else 중첩이 CMD 파서를 즉사시킴
 
CMD의 괄호 파서는 **if-else 블록 안에 다시 if-else가 중첩되면** 괄호 매칭을 잘못하여 배치파일이 **열리자마자 즉시 종료**됩니다. pause도 실행되지 않으므로 에러 메시지조차 볼 수 없습니다.
 
```
[재현 예시 - 2026-02-15 v2 백테스트 배치파일]
 
❌ 즉사하는 패턴 (중첩 if-else):
 
python -c "import arch" >nul 2>&1
if errorlevel 1 (
    echo [WARN] arch not found. Installing...
    pip install arch --quiet
    if errorlevel 1 (
        echo [WARN] arch install failed.
    ) else (
        echo [OK] arch installed
    )
) else (
    echo [OK] arch available
)
 
→ CMD 파서가 안쪽 ) else ( 를 바깥 if의 닫기로 인식
→ 구문 해석 실패 → 배치파일 즉시 종료 (pause 무효)
 
✅ 올바른 패턴 (서브루틴 분리):
 
call :check_pkg_optional arch
goto :continue
 
:check_pkg_optional
python -c "import %~1" >nul 2>&1
if not errorlevel 1 goto :eof
echo [WARN] %~1 not found. Installing...
pip install %~1 --quiet >nul 2>&1
goto :eof
 
또는 단순 분기:
 
python -c "import arch" >nul 2>&1
if not errorlevel 1 echo [OK] arch available
if errorlevel 1 echo [WARN] arch not found
```
 
### if-else 중첩 금지 절대 규칙
 
```
□ if ( ... ) else ( ... ) 블록 안에 또 다른 if-else 절대 금지
□ 복잡한 분기가 필요하면 call :label 서브루틴으로 분리
□ 또는 if not errorlevel 1 goto :eof 패턴으로 단순 분기
□ 안쪽 if가 필요하다면 블록 밖에서 별도 변수에 결과 저장 후 판단
```
 
### 근본 원인: create_file 도구의 한계
 
```
Claude 실행 환경: Ubuntu Linux
  → create_file은 Python의 open('w') 모드로 파일 생성
  → Linux의 기본 줄바꿈: LF (0x0A)
  → 인코딩: UTF-8
 
Windows CMD 요구사항:
  → 줄바꿈: CRLF (0x0D 0x0A)
  → 인코딩: ANSI (CP949) 또는 ASCII-only
```
 
### 절대 규칙: .bat 파일 생성 시 create_file 사용 금지
 
```
□ .bat 파일은 반드시 bash_tool + Python 바이너리 모드로 생성
□ create_file 도구로 .bat 파일을 절대 만들지 않음
□ 생성 후 반드시 CRLF 검증 (LF 0줄이어야 함)
```
 
### 표준 생성 방법 (bash_tool 사용)
 
```bash
python3 << 'MKBAT'
content = r"""@echo off
echo [DEBUG] Batch file loaded OK.
echo.
call :main_logic %*
echo.
echo Press any key to close...
pause >nul
exit /b 0
 
:main_logic
setlocal enabledelayedexpansion
 
REM === Your logic here ===
echo [INFO] Running...
 
endlocal
exit /b 0
"""
 
# 핵심: 'wb' 바이너리 모드 + \r\n 명시적 변환
with open('/home/claude/output.bat', 'wb') as f:
    for line in content.strip().split('\n'):
        f.write((line.rstrip('\r') + '\r\n').encode('ascii'))
 
MKBAT
 
# 검증
python3 -c "
with open('/home/claude/output.bat', 'rb') as f:
    data = f.read()
crlf = data.count(b'\r\n')
lf_only = data.count(b'\n') - crlf
print(f'CRLF: {crlf}, LF-only: {lf_only}')
assert lf_only == 0, f'FATAL: {lf_only} LF-only lines found!'
print('PASSED: All lines are CRLF')
"
```
 
### 생성 후 필수 검증 (이것 없이 전달 금지)
 
```python
# 이 검증을 통과해야만 사용자에게 전달 가능
python3 -c "
with open('파일경로.bat', 'rb') as f:
    data = f.read()
crlf = data.count(b'\r\n')
lf_only = data.count(b'\n') - crlf
has_korean = any('\uac00' <= chr(b) <= '\ud7a3' for b in data if b < 256)
 
# echo 안의 이스케이프 안 된 괄호 검사
lines = data.decode('ascii').split('\r\n')
paren_issues = []
in_block = 0  # if/for 블록 깊이 추적
for i, line in enumerate(lines, 1):
    s = line.strip()
    if s.startswith('if ') or s.startswith('for '):
        if '(' in s: in_block += 1
    if in_block > 0 and s.startswith('echo') and not s.startswith('echo.'):
        text = s[4:]
        for j, ch in enumerate(text):
            if ch in (')', '(') and (j == 0 or text[j-1] != '^'):
                paren_issues.append(f'Line {i}: {s[:70]}')
                break
    if s == ')': in_block = max(0, in_block - 1)
 
checks = {
    'CRLF only (LF=0)': lf_only == 0,
    'No Korean chars': not has_korean,
    'Starts with @echo': data.startswith(b'@echo'),
    'Contains pause': b'pause' in data,
    'No unescaped parens in echo': len(paren_issues) == 0,
}
 
for desc, ok in checks.items():
    print(f\"{'✅' if ok else '❌'} {desc}\")
 
if paren_issues:
    print(f'\n⚠️ Unescaped parentheses in echo (use ^( ^) instead):')
    for p in paren_issues:
        print(f'  {p}')
 
assert all(checks.values()), 'FAILED: Fix issues before delivery!'
print('\nALL CHECKS PASSED')
"
```
 
### 필수 체크리스트
 
```
□ .bat 파일을 create_file로 만들지 않았는가? (bash_tool + Python 바이너리 모드 사용)
□ 생성 후 CRLF 검증을 실행했는가? (LF-only = 0)
□ 한글이 포함되어 있지 않은가?
□ ASCII-only 문자만 사용했는가? (특수문자, 이모지 없음)
□ 생성된 파일이 @echo off로 시작하는가?
□ pause가 포함되어 있는가?
□ if/for 블록 안의 echo에서 괄호를 ^( ^) 로 이스케이프했는가?
□ if/for 블록 안의 echo에서 &, |, <, > 도 ^로 이스케이프했는가?
```
 
---
 
## 오류 패턴 #16: 배치파일 관리자 권한 자동 상승 누락 (심각도: High)
 
### 문제
 
Python 패키지 설치(pip install), COM 객체 접근(CYBOS Plus), 시스템 디렉토리 접근 등 **관리자 권한이 필요한 작업**에서 배치파일에 자동 권한 상승(UAC elevation) 코드를 넣지 않아, 사용자가 매번 수동으로 "우클릭 → 관리자 권한으로 실행"을 해야 합니다. 이마저도 안내가 불충분하여 일반 더블클릭으로 실행 → 권한 오류 → 원인 파악 불가가 반복됩니다.
 
### 과거 사고 이력
 
| 사고 | 필요 권한 | 결과 |
|------|----------|------|
| auto_clicker.bat (2025-10-25) | keyboard 라이브러리 설치 (pip install) | 설치 실패 → 에러 메시지만 | 
| auto_clicker.bat (2025-10-25) | keyboard 모듈 런타임 (키보드 후킹) | 권한 부족으로 작동 안 함 |
| block_trade_analyzer v17/v18 (2026-02-10) | CYBOS Plus COM 접근 (32-bit Python) | COM 초기화 실패 |
| 다수 배치파일 | pip install --quiet | 일부 환경에서 권한 부족 설치 실패 |
 
### 관리자 권한이 필요한 상황 판별 기준
 
```
반드시 관리자 권한 필요:
  ✅ keyboard, pyautogui 등 시스템 훅 라이브러리
  ✅ CYBOS Plus, HTS COM 객체 접근
  ✅ Windows 서비스 제어
  ✅ 시스템 디렉토리 (C:\Program Files 등) 쓰기
 
관리자 권한 권장:
  ⚠️ pip install (일부 환경에서 필요)
  ⚠️ 레지스트리 접근
  ⚠️ 방화벽/네트워크 설정
 
관리자 권한 불필요:
  ❌ 일반 Python 스크립트 실행
  ❌ 사용자 폴더 내 파일 읽기/쓰기
  ❌ 웹 API 호출
```
 
### 절대 규칙
 
```
□ 위 판별 기준에 해당하면 배치파일에 자동 권한 상승 코드 필수 포함
□ "우클릭 → 관리자 권한" 안내만 하고 끝내지 않음 → 자동화할 것
□ 권한 상승 실패(사용자가 UAC 거부) 시 명확한 안내 메시지 + pause
□ 이미 관리자인 경우 중복 상승하지 않음 (net session 체크)
```
 
### 표준 관리자 권한 자동 상승 패턴
 
```batch
@echo off
 
REM === Auto-elevate to Administrator ===
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo [INFO] Requesting administrator privileges...
    powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Process -FilePath \"%~f0\" -Verb RunAs"
    exit /b 0
)
 
REM === Restore working directory (RunAs resets to System32) ===
cd /d "%~dp0"
 
echo [OK] Running as Administrator.
echo [OK] Working dir: %cd%
echo.
 
REM === Main logic ===
setlocal enabledelayedexpansion
 
REM ... (메인 로직) ...
 
pause
```
 
### ⚠️ 흔한 실수 3가지
 
**실수 1: -WorkingDirectory 옵션 사용**
 
```
❌ powershell -Command "Start-Process ... -WorkingDirectory '%~dp0'"
 
문제:
  배치파일 경로에 한글이 포함되면 (예: C:\Users\...\바탕 화면\...)
  CMD(CP949) → PowerShell(UTF-16) 인코딩 변환 과정에서 경로가 깨짐
  → Start-Process가 잘못된 경로를 받아 실패
 
해결:
  ✅ -WorkingDirectory 제거
  ✅ 상승 후 cd /d "%~dp0" 로 작업 디렉토리 복원
  (cd /d는 CMD 내부에서 처리하므로 인코딩 문제 없음)
```
 
**실수 2: 작업 디렉토리 미복원**
 
```
관리자 권한 상승 후 흔한 증상:
  "Script not found: C:\Windows\System32\main.py"
 
원인:
  Start-Process -Verb RunAs가 작업 디렉토리를 System32로 초기화
 
해결:
  상승 코드 바로 뒤에 반드시:
  cd /d "%~dp0"
```
 
**실수 3: ExecutionPolicy 미설정**
 
```
❌ powershell -Command "Start-Process ..."
 
문제:
  일부 환경에서 PowerShell 실행 정책이 Restricted로 설정됨
  → Start-Process 명령 자체가 차단됨
 
해결:
  ✅ powershell -NoProfile -ExecutionPolicy Bypass -Command "..."
  (-NoProfile은 프로파일 스크립트 로딩 생략 → 속도 향상 + 간섭 방지)
```
 
### 통합 표준 배치파일 템플릿 (#4 + #15 + #16 모두 적용)
 
이것이 **모든 배치파일의 기본 골격**입니다. 새 배치파일을 만들 때 반드시 이 템플릿에서 시작합니다.
 
```batch
@echo off
 
REM === [#16] Auto-elevate to Administrator ===
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo [INFO] Requesting administrator privileges...
    powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Process -FilePath \"%~f0\" -Verb RunAs"
    exit /b 0
)
cd /d "%~dp0"
 
echo [OK] Running as Administrator.
echo [OK] Working dir: %cd%
echo.
 
REM === Safety wrapper: always pause at end ===
call :main_logic %*
echo.
echo Press any key to close...
pause >nul
exit /b 0
 
:main_logic
setlocal enabledelayedexpansion
 
REM === [#4] Check Python (py/python/python3) ===
set PYTHON_CMD=
where py >nul 2>&1 && set PYTHON_CMD=py && goto :found_python
where python >nul 2>&1 && set PYTHON_CMD=python && goto :found_python
where python3 >nul 2>&1 && set PYTHON_CMD=python3 && goto :found_python
 
echo [ERROR] Python not found. Please install Python.
exit /b 1
 
:found_python
echo [OK] Python: %PYTHON_CMD%
 
REM === [#9-2] Check required packages ===
for %%P in (pandas numpy requests) do (
    %PYTHON_CMD% -c "import %%P" >nul 2>&1
    if errorlevel 1 (
        echo [INSTALL] %%P not found. Installing...
        %PYTHON_CMD% -m pip install %%P --quiet
    ) else (
        echo [OK] %%P
    )
)
 
REM === Run main script ===
set SCRIPT=%~dp0main.py
if not exist "%SCRIPT%" (
    echo [ERROR] Script not found: %SCRIPT%
    exit /b 1
)
 
echo.
echo [RUN] Starting at %TIME:~0,8%...
%PYTHON_CMD% "%SCRIPT%" %*
if errorlevel 1 (
    echo [ERROR] Script failed.
    exit /b 1
)
 
echo [DONE] Completed successfully.
endlocal
exit /b 0
```
 
**이 템플릿의 생성은 반드시 [#15]의 bash_tool + Python 바이너리 모드로 수행합니다.**
 
### 관리자 권한이 불필요한 경우의 간략 템플릿
 
```batch
@echo off
echo [DEBUG] Batch file loaded OK.
call :main_logic %*
echo.
echo Press any key to close...
pause >nul
exit /b 0
 
:main_logic
setlocal enabledelayedexpansion
cd /d "%~dp0"
 
REM === (메인 로직) ===
 
endlocal
exit /b 0
```
 
### 필수 체크리스트
 
```
□ 이 스크립트가 관리자 권한을 필요로 하는가? (위 판별 기준 참조)
□ 필요하다면 net session 체크 + 자동 상승 코드가 포함되어 있는가?
□ powershell에 -NoProfile -ExecutionPolicy Bypass 옵션이 포함되어 있는가?
□ -WorkingDirectory 옵션을 사용하지 않았는가? (한글 경로에서 깨짐 → cd /d로 대체)
□ 자동 상승 후 cd /d "%~dp0"로 작업 디렉토리를 복원하는가?
□ UAC 거부 시 사용자에게 안내 메시지가 표시되는가?
□ 관리자 권한 불필요한데 불필요하게 상승하고 있지 않은가?
```
 
---
 
## 오류 패턴 #17: 시뮬레이션 루프 내 사전계산 가능 연산 반복 (심각도: Critical — 7,360배 성능 차이)
 
### 문제
 
백테스트·시뮬레이션에서 "현재 시점까지의 데이터로 X를 계산"하는 로직을 매 봉/매 체크마다 **처음부터 다시 계산**하면 O(n²)이 되어 실행 시간이 폭발합니다. #7(시간복잡도 폭발)의 구체적 하위 패턴이지만, 백테스트에서 반복적으로 발생하므로 별도 기재합니다.
 
### 사고 이력
 
| 날짜 | 오류 | 결과 |
|------|------|------|
| **2026-04-03** | `find_swings_up_to(bars[:bi+1])` 를 5봉마다 호출 → 매번 전체 봉 재스캔 | 종목당 6초 (146종목 = 15분). 사전계산 방식으로 전환 후 **0.0004초/종목 (7,360배 향상)** |
 
### 근본 원인
 
"리페인팅 방지를 위해 현재 시점까지만 사용" → "그러면 매번 처음부터 다시 계산해야지"라는 잘못된 추론. **사전계산 + 확정 시점 필터링**으로 리페인팅 방지와 O(n) 성능을 동시에 달성 가능.
 
### 올바른 패턴: 사전계산 + 확정 필터링
 
```python
# ❌ 금지: 매 체크마다 전체 재계산 O(n²)
for bi in range(11, len(day_df), 5):
    bars_so_far = day_df.iloc[:bi + 1]         # 매번 슬라이싱
    swings = find_swings_up_to(bars_so_far)    # 매번 전체 스캔
    wave = calc_wave(swings)
 
# ✅ 올바른: 사전계산 1회 + 확정 필터 O(n)
# 하루 시작 시 전체 스윙을 한 번만 계산
all_swings = precompute_swings(day_df, lb=5)  # 각 스윙에 confirmed_at = idx + lb 기록
 
for bi in range(11, len(day_df), 5):
    # 현재 봉까지 확정된 스윙만 필터 (리페인팅 방지)
    confirmed = [s for s in all_swings if s['confirmed_at'] <= bi]
    wave = calc_wave(confirmed)
```
 
### 적용 범위 (이 패턴이 해당하는 모든 연산)
 
```
□ 스윙 포인트 탐지 (lookback 기반)
□ 이동평균 계산 (rolling → 사전계산)
□ ATR 계산
□ 볼린저밴드 계산
□ VWAP 계산 (누적 합산 → 사전계산)
□ 거래대금 누적 (cumsum → 사전계산)
 
핵심 원칙:
  "현재 시점까지만 사용" ≠ "매번 처음부터 계산"
  "사전계산 + 확정 시점 필터링"이 유일하게 올바른 구현
```
 
### 체크리스트
 
```
□ 시뮬레이션 루프 안에서 DataFrame 슬라이싱(df.iloc[:bi])을 하고 있는가?
□ 그 슬라이스에 대해 전체 스캔 함수를 호출하고 있는가?
□ → 해당 함수를 루프 밖에서 1회 사전계산하고, 루프 안에서는 필터링만 할 수 있는가?
□ numpy 배열 직접 접근 (.values) + early break 최적화를 적용했는가?
□ 사전계산 결과가 기존 O(n²) 결과와 동일한지 mock 데이터로 검증했는가?
```
 
---
 
 
## 오류 패턴 #18: 관리자 권한 cmd.exe에서 Python stdout 미표시 (심각도: Critical)
 
### 문제
 
Windows에서 `Start-Process -Verb RunAs`로 관리자 권한 상승 후 실행된 cmd.exe에서, Python 스크립트의 `print()` 및 `logging(stream=sys.stdout)` 출력이 콘솔에 전혀 표시되지 않는 현상. 스크립트는 정상 종료(exit code 0)하지만 화면에는 아무것도 안 보임.
 
### 사고 이력
 
| 날짜 | 오류 | 결과 |
|------|------|------|
| **2026-04-06** | ETF 백테스트 스크립트: `logging.basicConfig(stream=sys.stdout)` → 출력 0줄 | 3회 연속 "출력 없음" 재현. `print(flush=True)`, `-u` 플래그, `chcp 65001` 제거 모두 효과 없음 |
 
### 근본 원인
 
관리자 권한 상승(UAC) 시 새로운 cmd.exe 프로세스가 생성되는데, 이 프로세스의 stdout 핸들이 Python 자식 프로세스에 올바르게 상속되지 않는 경우가 있음. bat 파일의 `echo` 명령어는 cmd.exe 내장 명령이라 정상 출력되지만, 외부 프로세스(Python)의 stdout은 표시 안 됨.
 
**효과 없는 시도들:**
- `print(msg, flush=True)` → 효과 없음
- `python -u` (unbuffered) 플래그 → 효과 없음
- `logging` → `print()` 전환 → 효과 없음
- `chcp 65001` 제거 → 효과 없음
 
### 올바른 해결 패턴: 파일 출력 + bat에서 type 표시
 
Python 스크립트가 **로그 파일에 직접 쓰고**, bat 파일이 **실행 후 `type` 명령으로 로그 파일을 콘솔에 표시**하는 방식. stdout이 안 되더라도 파일 I/O는 항상 작동함.
 
```python
# Python: 파일 + stderr + stdout 3중 출력
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_LOG_PATH = os.path.join(SCRIPT_DIR, "script_log.txt")
_LOG_FILE = open(_LOG_PATH, "w", encoding="utf-8")
 
def P(msg=""):
    line = f"{datetime.now().strftime('%H:%M:%S')} {msg}"
    # 1) 파일 (guaranteed)
    _LOG_FILE.write(line + "\n")
    _LOG_FILE.flush()
    # 2) stderr (usually unbuffered)
    try:
        sys.stderr.write(line + "\n")
        sys.stderr.flush()
    except Exception:
        pass
    # 3) stdout (might fail on admin cmd)
    try:
        print(line, flush=True)
    except Exception:
        pass
```
 
```batch
REM BAT: Python 실행 후 로그 파일 표시
"%PYTHON%" -u "%SCRIPT%" 2>&1
echo.
if exist "%LOGFILE%" ( type "%LOGFILE%" )
```
 
### 필수 체크리스트
 
```
□ 관리자 권한(UAC)으로 실행되는 bat에서 Python을 호출하는가?
□ bat 파일에서 Python stdout/stderr를 파일로 캡처 후 type으로 표시하는가?
```
 
---
 
## 오류 패턴 #19: 32-bit Python에서 64-bit 패키지 로드 크래시 (심각도: Critical)
 
### 문제
 
32-bit Python 환경에서 numpy, pandas 등의 패키지가 64-bit로 컴파일된 .pyd/.dll을 로드하면, Python이 **exit code -1073741795 (STATUS_ILLEGAL_INSTRUCTION)**로 즉시 크래시합니다. 에러 메시지 없이 즉사하므로 원인 파악이 극히 어렵습니다.
 
### 사고 이력
 
| 날짜 | 오류 | 결과 |
|------|------|------|
| **2026-04-06** | ETF 백테스트: 64-bit pip으로 설치된 numpy를 32-bit Python39에서 로드 | exit code -1073741795, stdout/stderr 모두 빈 파일. 5회 연속 "출력 없음" 재현 |
 
### 근본 원인
 
사용자 PC에 64-bit Python과 32-bit Python이 공존할 때, `pip install`이 PATH 우선순위에 따라 64-bit pip으로 실행되어 64-bit 바이너리가 설치됨. 32-bit Python에서 이 바이너리를 import하면 CPU 명령어 불일치로 크래시.
 
### 올바른 해결 패턴: bat에서 safe_import + auto-repair
 
```batch
:safe_import
"%PYTHON%" -c "import %~1" >nul 2>nul
set _ERR=!errorlevel!
if !_ERR! equ 0 ( echo [OK] %~2 & exit /b 0 )
REM --- 음수 exit = 크래시 (64/32-bit 불일치) ---
if !_ERR! lss 0 (
    echo [CRASH] %~2 64/32-bit mismatch. Force reinstalling...
    "%PYTHON%" -m pip install --force-reinstall %~2 --quiet
) else (
    echo [MISSING] %~2. Installing...
    "%PYTHON%" -m pip install %~2 --quiet
)
REM --- 재설치 후 재검증 ---
"%PYTHON%" -c "import %~1" >nul 2>nul
if !errorlevel! neq 0 ( echo [FAIL] %~2 still broken. & exit /b 1 )
echo [FIXED] %~2 reinstalled OK.
exit /b 0
```
 
### 필수 체크리스트
 
```
□ 32-bit Python을 사용하는 스크립트인가? (CYBOS Plus 등)
□ → bat에서 패키지 import 테스트 시 exit code를 확인하는가?
□ → 음수 exit code(크래시) 감지 시 --force-reinstall 자동 실행하는가?
□ → 재설치 후 재검증(re-import)을 수행하는가?
□ → pip install 시 반드시 32-bit Python의 pip을 명시적으로 호출하는가? ("%PYTHON%" -m pip)
```
 
---
 
### 필수 체크리스트 (패턴 #18 원본)
 
```
□ 관리자 권한(UAC)으로 실행되는 bat에서 Python을 호출하는가?
□ → 그렇다면 Python 출력을 로그 파일에도 기록하는 P() 패턴을 사용하는가?
□ → bat 파일에 실행 후 type "%LOGFILE%" 로 로그 표시가 포함되어 있는가?
□ → stderr에도 출력하는 폴백이 있는가?
□ bat 파일에 chcp 65001이 없는가? (Python 3.9 이하에서 stdout 깨짐 유발)
□ 2>&1 리다이렉션이 Python 호출에 포함되어 있는가?
```
 
---
 
## 작업 유형별 종합 체크리스트
 
### Python 스크립트 작성
 
```
□ [공통] userMemories 제약조건 확인
□ [공통] 과거 동일 유형 오류 검색
□ [#1] 외부 API 사용 시 rate limit 웹검색 확인
□ [#2] 모든 함수에 입력→처리→출력 로그가 log.info로 포함 (log.debug 금지!)
□ [#2] API 함수: 페이지 수, cont_yn, 응답 건수, 종료 사유 찍히는지 확인
□ [#2] 분석 함수: 전체 건수, 필터 통과 건수, 통과율 찍히는지 확인
□ [#2] 캐시 함수: 기존+신규=병합 건수 찍히는지 확인
□ [#2] 메인 루프: 전 종목 1줄 요약 (첫 5개만 X, 전부!)
□ [#2] Step 완료 시: 평균/최소/최대 통계 요약 포함
□ [#2] 이상 징후 자동 경고(warning) 7가지 이상 포함
□ [#2] --debug 안 켜도 기본 로그만으로 원인 파악 가능한지 자문
□ [#2] logging.basicConfig()이 파일 최상단에 위치 (다른 logging 호출보다 먼저)
□ [#7] 반복횟수 × 단위연산비용 계산
□ [#17] 시뮬레이션 루프 안 사전계산 가능 연산이 매번 반복되지 않는가?
□ [#8] mock 데이터 실행 테스트
□ [#9] matplotlib/scipy 사용 안 함 확인 (scipy → math.erfc 기반 순수 Python 대체)
□ [#9-2] 패키지 자동 체크/설치 (ensure_packages) 포함
□ [#10] 고정 파라미터 위반 없음 확인
□ [#11] 백테스트면 13개 통계지표 전부 포함
□ [#13] 코드 절단 없이 완전한 코드
□ [#18] 관리자 cmd에서 실행되는 경우: P() 3중 출력 패턴 사용 (파일+stderr+stdout)
```
 
### PineScript 작성
 
```
□ [공통] userMemories 제약조건 확인
□ [#3] 모든 참조에 [1] 확정봉 사용 (리페인팅 방지)
□ [#3] 진입가 = 시그널 다음봉 시가
□ [#10] RSI=14, BB=(20,2), MACD=(12,26,9) 고정
□ [#12] input.xxx() 함수 한 줄 작성 (줄바꿈 금지)
□ [#12] 삼항 연산자 여러 줄 금지 → switch 문 사용
□ [#12] 변수명 한글 사용 금지 (영문만)
```
 
### 배치파일(.bat) 작성
 
```
□ [#4] 한글 절대 미사용 (영어만)
□ [#4] 모든 에러 블록에 pause 포함
□ [#4] Python 경로 py → python → python3 순서 체크
□ [#4] 표준 템플릿 기반으로 작성
□ [#9-2] 필수 패키지 자동 체크/설치 로직 포함
□ [#15] create_file 도구로 생성하지 않음 (bash_tool + Python 바이너리 모드 사용)
□ [#15] 생성 후 CRLF 검증 통과 (LF-only = 0)
□ [#15] ASCII-only 문자 확인 (한글, 이모지, 특수문자 없음)
□ [#15] if/for 블록 안 echo의 괄호를 ^( ^) 로 이스케이프했는가?
□ [#15] if-else 블록 안에 또 다른 if-else가 중첩되어 있지 않은가? (서브루틴 분리 필수)
□ [#16] 관리자 권한 필요 여부 판별 (pip install, COM, 시스템 훅 등)
□ [#16] 필요 시 net session + Start-Process -Verb RunAs 자동 상승 코드 포함
□ [#16] -WorkingDirectory 사용하지 않음 (한글 경로 깨짐) → cd /d "%~dp0" 사용
□ [#16] powershell에 -NoProfile -ExecutionPolicy Bypass 포함
```
 
### 기존 코드 수정
 
```
□ [#6] 원본 UI 언어, 필터 기준값, 키 이름 먼저 확인
□ [#6] 전체 복사 완료 먼저 → 그 후 부분 수정 (절대 동시 진행 금지!)
□ [#6] 요청된 변경만 수행 (임의 개선 금지)
□ [#6] 수정 후 diff 비교
□ [#10] 고정 파라미터 변경 없음 확인
```
 
### 엑셀 파일 생성/수정
 
```
□ [#14] 모든 숫자 비교에 *1 형변환
□ [#14] 조건부 서식도 *1 형변환
□ [#14] 셀프QA (텍스트 숫자, 빈 셀, 음수)
□ gsheet-compatible-xlsx 스킬 함께 참조
```
 
### 데이터 수집/캐시 코드
 
```
□ [#1] API rate limit 확인 및 준수
□ [#2] API 호출 함수: 페이지 수, cont_yn, 응답 건수, 종료 사유 log.info
□ [#2] 캐시 HIT/MISS/EXPIRED/MERGE 로그 (기존+신규=병합 건수)
□ [#2] 메인 루프: 전 종목 1줄 요약 (첫 N개만 아님, 전부!)
□ [#2] Step 완료 시: 평균/최소/최대 통계 요약
□ [#2] 이상 징후 자동 경고: 빈 응답, 1페이지만 수신, 전 종목 건수 2 이하 등
□ [#5] 증분 수집+병합 방식 (전체 재수집 금지)
□ [#7] 대량 데이터 시 벡터 연산/병렬화
□ [#17] 시뮬레이션/분석 루프 내 사전계산 가능 연산 반복 여부 확인
```
 
---
 
## 최종 전달 전 체크리스트
 
코드를 사용자에게 전달하기 직전에 반드시 확인:
 
```
□ py_compile 문법 검사 통과
□ mock 데이터 실행 테스트 통과
□ dtype 관련 (int↔float, NaN) 테스트 통과
□ 고정 파라미터 위반 없음
□ 디버깅 로그: 모든 함수에 입력→처리→출력이 log.info로 있는가? (log.debug 아님!)
□ 디버깅 로그: --debug 안 켜고 기본 실행만으로 "뭐가 문제인지" 즉시 파악 가능한가?
□ 디버깅 로그: 이상 징후 자동 경고(warning)가 포함되어 있는가?
□ 디버깅 로그: 메인 루프에서 전 항목 1줄 요약이 찍히는가? (첫 N개만 아님!)
□ logging.basicConfig()이 모든 logging 호출보다 먼저 위치
□ 코드 절단 없이 완전한 코드
□ 마크다운 코드 블록 쌍 맞음 (여는 블록 = 닫는 블록)
□ 과거 오류 패턴 재발 없음
□ .bat 파일 포함 시: create_file이 아닌 bash_tool로 생성했는가? [#15]
□ .bat 파일 포함 시: CRLF 검증 + 괄호 이스케이프 검증 통과했는가? [#15]
□ .bat 파일 포함 시: if-else 블록 중첩이 없는가? (서브루틴 분리 필수) [#15]
□ .bat 파일 포함 시: 관리자 권한 필요 여부를 판별하고 해당 시 자동 상승 코드를 넣었는가? [#16]
□ .bat 파일 포함 시: -WorkingDirectory 사용하지 않고 cd /d 로 대체했는가? [#16]
□ .bat 파일 포함 시: 관리자 cmd + Python이면 P() 3중 출력 + type 로그 표시를 넣었는가? [#18]
```
 
하나라도 통과하지 못하면 **절대 전달하지 말고 수정 후 재검증**합니다.