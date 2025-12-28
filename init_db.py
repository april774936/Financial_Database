import os, json, gspread, time
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from fredapi import Fred

def init_split_sheets():
    # 1. 인증 설정
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_json = json.loads(os.environ.get('GSPREAD_JSON'))
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
    client = gspread.authorize(creds)
    fred = Fred(api_key=os.environ.get('FRED_API_KEY'))
    
    # Secrets에 등록한 3개의 시트 ID를 가져옵니다
    sheets_info = {
        'ASSETS': os.environ.get('SHEET_ID_ASSETS'),
        'LIQUID': os.environ.get('SHEET_ID_LIQUID'),
        'MACRO': os.environ.get('SHEET_ID_MACRO')
    }

    # [중요] 전체 지표 분류 정의
    fred_dict = {
        # --- ASSETS 시트 (자산/지수/원자재) ---
        'NASDAQ100': ['ASSETS', 'Index', '나스닥100', 1],
        'SP500': ['ASSETS', 'Index', 'S&P500', 1],
        'DJIA': ['ASSETS', 'Index', '다우존스', 1],
        'DCOILWTICO': ['ASSETS', 'Energy', 'WTI원유', 1],
        'CBBTCUSD': ['ASSETS', 'Crypto', '비트코인', 1],
        'CBETHUSD': ['ASSETS', 'Crypto', '이더리움', 1],
        'GOLDAMGBD228NLBM': ['ASSETS', 'Commodity', '금_현물', 1],
        'ID71081': ['ASSETS', 'Commodity', '은_현물', 1],
        'PCOPPUSDM': ['ASSETS', 'Commodity', '구리_현물', 1],

        # --- LIQUID 시트 (유동성/금리) ---
        'WALCL': ['LIQUID', 'Liquidity', '연준총자산', 1000000],
        'M2SL': ['LIQUID', 'Money', 'M2통화량', 1000],
        'WTREGEN': ['LIQUID', 'Liquidity', 'TGA잔고', 1],
        'RRPONTSYD': ['LIQUID', 'Liquidity', '역레포잔고', 1],
        'DFEDTARU': ['LIQUID', 'Policy', '기준금리(상단)', 1],
        'T10Y2Y': ['LIQUID', 'Rates', '장단기금리차', 1],
        'DGS10': ['LIQUID', 'Rates', '미_10년물_금리', 1],
        'DGS2': ['LIQUID', 'Rates', '미_2년물_금리', 1],
        'BAMLH0A0HYM2': ['LIQUID', 'Risk', '정크본드스프레드', 1],
        'VIXCLS': ['LIQUID', 'Volatility', 'VIX공포지수', 1],

        # --- MACRO 시트 (경제/고용/소비) ---
        'CPIAUCSL': ['MACRO', 'Inflation', 'CPI', 1],
        'PPIACO': ['MACRO', 'Inflation', 'PPI', 1],
        'UNRATE': ['MACRO', 'Economy', '실업률', 1],
        'GDPC1': ['MACRO', 'Economy', '실질GDP', 1],
        'CSUSHPINSA': ['MACRO', 'Housing', '미국주택가격지수', 1],
        'UMCSENT': ['MACRO', 'Sentiment', '소비자심리지수', 1],
        'DEXKOUS': ['MACRO', 'Currency', '원달러환율', 1],
        'DTWEXBGS': ['MACRO', 'Currency', '달러인덱스', 1],
        'PCE': ['MACRO', 'Economy', '개인소비지출', 1],
        'RSXFS': ['MACRO', 'Economy', '소매판매', 1],
        'DGORDER': ['MACRO', 'Economy', '내구재주문', 1],
        'TDSP': ['MACRO', 'Banking', '가계부채상환비율', 1],
        'TOTLL': ['MACRO', 'Banking', '은행총대출', 1]
    }

    # 데이터 수집 (지표별로 쪼개기 위해 딕셔너리에 담기)
    split_data = {'ASSETS': [], 'LIQUID': [], 'MACRO': []}

    print("FRED에서 역사적 데이터 수집을 시작합니다 (1970~)...")
    for ticker, info in fred_dict.items():
        print(f"진행 중: {info[2]} ({ticker})")
        try:
            s = fred.get_series(ticker, observation_start='1970-01-01')
            group = info[0]
            for date, val in s.items():
                if pd.notna(val) and val != ".":
                    split_data[group].append([date.strftime('%Y-%m-%d'), info[1], info[2], round(float(val)/info[3], 3)])
            time.sleep(0.3)
        except Exception
