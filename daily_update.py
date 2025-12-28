import os, json, gspread, time
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
from fredapi import Fred

def daily_ultimate_update():
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

    # 데이터 공백 방지를 위해 조회 기간을 2년(730일)으로 설정
    start_date = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')
    today_str = datetime.now().strftime('%Y-%m-%d')

    # A. yfinance 타겟 (가격 및 밸류에이션)
    yf_map = {
        '^NDX': ['ASSETS', 'Index', '나스닥100'],
        '^GSPC': ['ASSETS', 'Index', 'S&P500'],
        'BTC-USD': ['ASSETS', 'Crypto', '비트코인'],
        'ETH-USD': ['ASSETS', 'Crypto', '이더리움'],
        'GC=F': ['ASSETS', 'Commodity', '골드(금)'],
        'HG=F': ['ASSETS', 'Commodity', '구리_현물'],
        'DX-Y.NYB': ['MACRO', 'Currency', '달러인덱스']
    }
    
    # 밸류에이션 (PE, EPS)
    valuation_tickers = {'SPY': 'S&P500', 'QQQ': '나스닥100'}

    # B. FRED 타겟 (M2 및 리스크 지표 추가)
    fred_map = {
        # --- 유동성 및 통화량 ---
        'WM2NS': ['LIQUID', 'Liquidity', 'M2통화량', 1], # 시중 통화량 (가장 중요)
        'WALCL': ['LIQUID', 'Liquidity', '연준총자산', 1000000],
        'RRPONTSYD': ['LIQUID', 'Liquidity', '역레포잔고', 1],
        'WTREGEN': ['LIQUID', 'Liquidity', 'TGA잔고', 1],
        
        # --- 리스크 및 신용 스프레드 ---
        'BAMLH0A0HYM2': ['LIQUID', 'Rates', '하이일드스프레드', 1], # 기업 부도 위험/심리
        'STLPPM': ['LIQUID', 'Volatility', '금융스트레스지수', 1], # 시장 긴장 상태
        'VIXCLS': ['LIQUID', 'Volatility', 'VIX공포지수', 1],
        
        # --- 금리 및 통화 정책 ---
        'DFEDTARU': ['LIQUID', 'Policy', '기준금리(상단)', 1],
        'T10Y2Y': ['LIQUID', 'Rates', '장단기금리차', 1],
        'DGS10': ['LIQUID', 'Rates',
