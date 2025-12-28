import os, json, gspread, time
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from fredapi import Fred
import yfinance as yf

def init_sheet():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_json = json.loads(os.environ.get('GSPREAD_JSON'))
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
    client = gspread.authorize(creds)
    sheet_id = os.environ.get('SPREADSHEET_ID')
    sheet = client.open_by_key(sheet_id).sheet1

    fred = Fred(api_key=os.environ.get('FRED_API_KEY'))
    all_data = []
    start_date = '2020-01-01'

    # 1. FRED 데이터 (리포트에 있던 모든 지표 포함)
    fred_dict = {
        # --- 유동성 및 은행 (Liquidity & Banking) ---
        'WALCL': ['Liquidity', '연준총자산', 1000000],
        'M2SL': ['Money', 'M2통화량', 1000],
        'WTREGEN': ['Liquidity', 'TGA잔고', 1],
        'RRPONTSYD': ['Liquidity', '역레포잔고', 1],
        'DPSACBW027SBKG': ['Banking', '은행총예금', 1],
        'TOTLL': ['Banking', '은행총대출', 1],
        
        # --- 금리 및 정책 (Rates & Policy) ---
        'DFEDTARU': ['Policy', '기준금리(상단)', 1],
        'DFEDTARL': ['Policy', '기준금리(하단)', 1],
        'EFFR': ['Rates', '실효연방금리(EFFR)', 1],
        'SOFR': ['Rates', '담보금리(SOFR)', 1],
        'IORB': ['Rates', '준비금이자(IORB)', 1],
        'T10Y2Y': ['Rates', '장단기금리차', 1],
        'BAMLH0A0HYM2': ['Risk', '정크본드스프레드', 1],
        
        # --- 거시경제 지표 (Economy & Inflation) ---
        'UNRATE': ['Economy', '실업률', 1],
        'CPIAUCSL': ['Inflation', 'CPI', 1],
        'CPILFESL': ['Inflation', 'Core_CPI', 1],
        'PPIACO': ['Inflation', 'PPI', 1],
        'PCU3311': ['Inflation', 'Core_PPI', 1],
        'GDPC1': ['Economy', '실질GDP', 1],
        'GFDEBTN': ['Debt', '총국가부채', 1000],
        'NFCI': ['Sentiment', '금융조건지수', 1],
        'PAYEMS': ['Employment', '비농업고용자수', 1]
    }

    for ticker, info in fred_dict.items():
        print(f"FRED 수집 중: {info[1]}...")
        try:
            series = fred.get_series(ticker, observation_start=start_date)
            for date, val in series.items():
                if pd.notna(val):
                    all_data.append([date.strftime('%Y-%m-%d'), info[0], info[1], round(val/info[2], 3)])
            time.sleep(0.3)
        except: print(f"FRED 에러: {ticker}")

    # 2. Yahoo Finance 데이터 (시장 지표 + 코인 + 원자재)
    yf_dict = {
        # --- 주가 지수 ---
        '^NDX': ['Index', '나스닥100'],
        '^GSPC': ['Index', 'S&P500'],
        '^DJI': ['Index', '다우존스'],
        '^RUT': ['Index', '러셀2000'],
        'DX-Y.NYB': ['Currency', '달러인덱스'],
        
        # --- 가상자산 (Coinbase 기준 포함) ---
        'BTC-USD': ['Crypto', '비트코인'],
        'ETH-USD': ['Crypto', '이더리움'],
        
        # --- 원자재 ---
        'GC=F': ['Commodity', '금_선물'],
        'SI=F': ['Commodity', '은_선물'],
        'HG=F': ['Commodity', '구리_선물'],
        
        # --- 채권 수익률 ---
        '^TNX': ['Rates', '미_10년물_금리'],
        '^IRX': ['Rates', '미_3개월_금리']
    }

    for ticker, info in yf_dict.items():
        print(f"YFinance 수집 중: {info[1]}...")
        try:
            df = yf.download(ticker, start=start_date, progress=False)
            for date, row in df.iterrows():
                val = float(row['Close'])
                if pd.notna(val):
                    all_data.append([date.strftime('%Y-%m-%d'), info[0], info[1], round(val, 2)])
        except: print(f"YFinance 에러: {ticker}")

    # 데이터 정렬 및 업로드
    all_data.sort(key=lambda x: x[0])
    sheet.clear()
    sheet.append_row(["Date", "Category", "Name", "Value"])
    
    # 데이터 양이 많으므로 분할 업로드
    batch_size = 1000
    for i in range(0, len(all_data), batch_size):
        sheet.append_rows(all_data[i:i+batch_size])
        print(f"{i}행 업로드 중...")
        time.sleep(1)
    
    print(f"✅ 총 {len(all_data)}행으로 데이터베이스 초기 구축 완료!")

if __name__ == "__main__":
    init_sheet()
