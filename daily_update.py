import os, json, gspread, time
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
from fredapi import Fred

def daily_combined_update():
    # 1. 인증 및 환경 설정
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_json = json.loads(os.environ.get('GSPREAD_JSON'))
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
    client = gspread.authorize(creds)
    fred = Fred(api_key=os.environ.get('FRED_API_KEY'))
    
    sheets_info = {
        'ASSETS': os.environ.get('SHEET_ID_ASSETS'),
        'LIQUID': os.environ.get('SHEET_ID_LIQUID'),
        'MACRO': os.environ.get('SHEET_ID_MACRO')
    }
    
    # 최근 1년치 데이터 범위 (FRED용)
    start_date_fred = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    # 최근 10일치 데이터 범위 (yfinance 최신성 확보용)
    start_date_yf = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d')

    # 2. 지표 배분 (에러 방지 및 최신성 극대화)
    # A. yfinance 섹션: 실시간 가격이 중요한 자산들
    yf_targets = {
        'QQQ': ['ASSETS', 'Index', '나스닥100'],
        'SPY': ['ASSETS', 'Index', 'S&P500'],
        'DIA': ['ASSETS', 'Index', '다우존스30'],
        'BTC-USD': ['ASSETS', 'Crypto', '비트코인'],
        'ETH-USD': ['ASSETS', 'Crypto', '이더리움'],
        'GC=F': ['ASSETS', 'Commodity', '금_현물'],
        'HG=F': ['ASSETS', 'Commodity', '구리_현물'],
        'CL=F': ['ASSETS', 'Energy', 'WTI원유']
    }

    # B. FRED 섹션: 경제 정책 및 매크로 지표
    fred_dict = {
        'WALCL': ['LIQUID', 'Liquidity', '연준총자산', 1000000],
        'WTREGEN': ['LIQUID', 'Liquidity', 'TGA잔고', 1],
        'RRPONTSYD': ['LIQUID', 'Liquidity', '역레포잔고', 1],
        'DFEDTARU': ['LIQUID', 'Policy', '기준금리(상단)', 1],
        'T10Y2Y': ['LIQUID', 'Rates', '장단기금리차', 1],
        'DGS10': ['LIQUID', 'Rates', '미_10년물_금리', 1],
        'DGS2': ['LIQUID', 'Rates', '미_2년물_금리', 1],
        'VIXCLS': ['LIQUID', 'Volatility', 'VIX공포지수', 1],
        'BAMLH0A0HYM2': ['LIQUID', 'Rates', '정크본드스프레드', 1],
        'TOTLL': ['LIQUID', 'Economy', '은행총대출', 1],
        'CPIAUCSL': ['MACRO', 'Inflation', 'CPI', 1],
        'PPIACO': ['MACRO', 'Inflation', 'PPI', 1],
        'UNRATE': ['MACRO', 'Labor', '실업률', 1],
        'DEXKOUS': ['MACRO', 'Currency', '원달러환율', 1],
        'DTWEXBGS': ['MACRO', 'Currency', '달러인덱스', 1],
        'RSXFS': ['MACRO', 'Economy', '소매판매', 1],
        'DGORDER': ['MACRO', 'Economy', '내구재주문', 1],
        'TDSP': ['MACRO', 'Economy', '가계부채상환비율', 1],
        'GDPC1': ['MACRO', 'Economy', '실질GDP', 1]
    }

    for group_name, sheet_id in sheets_info.items():
        if not sheet_id: continue
