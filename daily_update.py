import os, json, gspread, time
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
from fredapi import Fred

def daily_professional_update():
    # 1. 초기 설정 및 API 연결
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_json = json.loads(os.environ.get('GSPREAD_JSON'))
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
    client = gspread.authorize(creds)
    fred = Fred(api_key=os.environ.get('FRED_API_KEY'))
    
    sheets_info = {'ASSETS': os.environ.get('SHEET_ID_ASSETS'), 
                   'LIQUID': os.environ.get('SHEET_ID_LIQUID'), 
                   'MACRO': os.environ.get('SHEET_ID_MACRO')}

    # 데이터 누락 방지를 위해 조회 기간을 2년으로 확대 (Backfill 로직)
    start_date = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')
    today_str = datetime.now().strftime('%Y-%m-%d')

    # 2. 고정밀 티커 매핑 (가장 업데이트가 빠른 소스로 교체)
    yf_map = {
        'BTC-USD': ['ASSETS', 'Crypto', '비트코인'],
        'ETH-USD': ['ASSETS', 'Crypto', '이더리움'], # 2024년 멈춤 해결
        'GC=F': ['ASSETS', 'Commodity', '골드(금)'],
        'SI=F': ['ASSETS', 'Commodity', '실버(은)'],
        'HG=F': ['ASSETS', 'Commodity', '구리_현물'], # 2024년 멈춤 해결
        'CL=F': ['ASSETS', 'Energy', 'WTI원유'],
        '^NDX': ['ASSETS', 'Index', '나스닥100'],
        '^GSPC': ['ASSETS', 'Index', 'S&P500'],
        '^DJI': ['ASSETS', 'Index', '다우존스30'],
        'DX-Y.NYB': ['MACRO', 'Currency', '달러인덱스'] # FRED 대신 yfinance에서 실시간 수집
    }

    fred_map = {
        # 유동성/정책
        'DFEDTARU': ['LIQUID', 'Policy', '기준금리(상단)', 1],
        'WALCL': ['LIQUID', 'Liquidity', '연준총자산', 1000000],
        'WTREGEN': ['LIQUID', 'Liquidity', 'TGA잔고', 1],
        'RRPONTSYD': ['LIQUID', 'Liquidity', '역레포잔고', 1],
        'T10Y2Y': ['LIQUID', 'Rates', '장단기금리차', 1],
        'DGS10': ['LIQUID', 'Rates', '미_10년물_금리', 1],
        'DGS2': ['LIQUID', 'Rates', '미_2년물_금리', 1],
        'BAMLH0A0HYM2': ['LIQUID', 'Rates', '정크본드스프레드', 1], # 신용 지표 업데이트
        'VIXCLS': ['LIQUID', 'Volatility', 'VIX공포지수', 1],
        'BUSLOANS': ['LIQUID', 'Economy', '은행총대출', 1], # 티커 교체 (TOTLL -> BUSLOANS)
        # 거시경제
        'CPIAUCSL': ['MACRO', 'Inflation', 'CPI', 1],
        'PPIACO': ['MACRO', 'Inflation', 'PPI', 1],
        'PCEPI': ['MACRO', 'Inflation', 'PCE물가', 1],
        'UNRATE': ['MACRO', 'Labor', '실업률', 1],
        'GDPC1': ['MACRO', 'Economy', '실질GDP', 1],
        'RSXFS': ['MACRO', 'Economy', '소매판매', 1],
        'DGORDER': ['MACRO', 'Economy', '내구재주문', 1],
        'DEXKOUS': ['MACRO', 'Currency', '원달러환율', 1]
    }

    for group_name, sheet_id in sheets_info.items():
        if not sheet_id: continue
        try:
            sheet = client.open_by_key(sheet_id).sheet1
            new_rows = []

            # A. yfinance 데이터 (자산 가격)
            group_yf = {k: v for k, v in yf_map.items() if v[0] == group_name}
            for ticker, info in group_yf.items():
                print(f"Fetching {info[2]}...")
                df = yf.download(ticker, start=start_date, progress=False)
                if not df.empty:
                    # 최신 pandas 버전의 MultiIndex 대응
                    close_series = df['Close'][ticker] if isinstance(df['Close'], pd.DataFrame) else df['Close']
                    for date, val in close_series.tail(500).items(): # 최근 500일치 집중 보강
                        if pd.notna(val):
                            new_rows.append([date.strftime('%Y-%m-%d'), info[1], info[2], round(float(val), 2)])

            # B. FRED 데이터 (매크로 지표)
            group_fred = {k: v for k, v in fred_map.items() if v[0] == group_name}
            for ticker, info in group_fred.items():
                print(f"Fetching {info[2]}...")
                try:
                    s = fred.get_series(ticker, observation_start=start_date)
                    for date, val in s.items():
                        if pd.notna(val) and val != ".":
                            new_rows.append([date.strftime('%Y-%m-%d'), info[1], info[2], round(float(val)/info[3], 3)])
                except: continue

            # 3. 데이터 정제: 중복 제거 및 무결성 확보
            if new_rows:
                final_df = pd.DataFrame(new_rows, columns=["Date", "Category", "Name", "Value"])
                # 날짜와 이름이 같은 중복 데이터 중 최신값만 남김
                final_df = final_df.drop_duplicates(subset=["Date", "Name"], keep='last')
                final_df = final_df.sort_values(by=["Date", "Name"])
                
                sheet.clear()
                sheet.append_row(["Date", "Category", "Name", "Value"])
                sheet.append_rows(final_df.values.tolist())
                print(f"✅ {group_name} Update Success.")
            
            time.sleep(1) # API 레이트 리밋 방지
        except Exception as e:
            print(f"🚨 {group_name} Error: {e}")

if __name__ == "__main__":
    daily_professional_update()
