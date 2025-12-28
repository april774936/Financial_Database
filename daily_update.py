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
    
    # 데이터 범위 설정
    start_date_fred = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    start_date_yf = (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d') # 최근 데이터 집중 수집

    # 2. 지표 배분
    # A. yfinance 섹션 (실시간 가격 및 최신성 필수 자산)
    yf_targets = {
        'QQQ': ['ASSETS', 'Index', '나스닥100'],
        'SPY': ['ASSETS', 'Index', 'S&P500'],
        'DIA': ['ASSETS', 'Index', '다우존스30'],
        'BTC-USD': ['ASSETS', 'Crypto', '비트코인'],
        'ETH-USD': ['ASSETS', 'Crypto', '이더리움'],
        'GC=F': ['ASSETS', 'Commodity', '골드(금)'],
        'SI=F': ['ASSETS', 'Commodity', '실버(은)'],
        'HG=F': ['ASSETS', 'Commodity', '구리_현물'],
        'CL=F': ['ASSETS', 'Energy', 'WTI원유']
    }

    # B. FRED 섹션 (정책 및 매크로 지표)
    fred_dict = {
        'WALCL': ['LIQUID', 'Liquidity', '연준총자산', 1000000],
        'WTREGEN': ['LIQUID', 'Liquidity', 'TGA잔고', 1],
        'RRPONTSYD': ['LIQUID', 'Liquidity', '역레포잔고', 1],
        'DFEDTARU': ['LIQUID', 'Policy', '기준금리(상단)', 1],
        'T10Y2Y': ['LIQUID', 'Rates', '장단기금리차', 1],
        'DGS10': ['LIQUID', 'Rates', '미_10년물_금리', 1],
        'DGS2': ['LIQUID', 'Rates', '미_2년물_금리', 1],
        'BAMLH0A0HYM2': ['LIQUID', 'Rates', '정크본드스프레드', 1],
        'VIXCLS': ['LIQUID', 'Volatility', 'VIX공포지수', 1],
        'CPIAUCSL': ['MACRO', 'Inflation', 'CPI', 1],
        'PPIACO': ['MACRO', 'Inflation', 'PPI', 1],
        'PCEPI': ['MACRO', 'Inflation', '개인소비지출(PCE)'], 
        'GDPC1': ['MACRO', 'Economy', '실질GDP', 1],
        'UNRATE': ['MACRO', 'Labor', '실업률', 1],
        'TOTLL': ['MACRO', 'Economy', '은행총대출', 1],
        'RSXFS': ['MACRO', 'Economy', '소매판매', 1],
        'DGORDER': ['MACRO', 'Economy', '내구재주문', 1],
        'DEXKOUS': ['MACRO', 'Currency', '원달러환율', 1],
        'DTWEXBGS': ['MACRO', 'Currency', '달러인덱스', 1]
    }

    for group_name, sheet_id in sheets_info.items():
        if not sheet_id: continue
        try:
            sheet = client.open_by_key(sheet_id).sheet1
            sheet.clear()
            sheet.append_row(["Date", "Category", "Name", "Value"])
            new_rows = []

            # --- 파트 1: yfinance 수집 ---
            if group_name == 'ASSETS':
                for ticker, info in yf_targets.items():
                    print(f"yfinance 수집: {info[2]}")
                    try:
                        data = yf.download(ticker, start=start_date_yf, progress=False)
                        if not data.empty:
                            df_close = data['Close']
                            if isinstance(df_close, pd.DataFrame): # 멀티인덱스 대응
                                df_close = df_close[ticker]
                            
                            for date, val in df_close.items():
                                if pd.notna(val):
                                    new_rows.append([date.strftime('%Y-%m-%d'), info[1], info[2], round(float(val), 2)])
                    except: continue

            # --- 파트 2: FRED 수집 ---
            group_tickers = {k: v for k, v in fred_dict.items() if v[0] == group_name}
            for ticker, info in group_tickers.items():
                print(f"FRED 수집: {info[2]}")
                try:
                    s = fred.get_series(ticker, observation_start=start_date_fred)
                    divisor = info[3] if len(info) > 3 else 1
                    for date, val in s.items():
                        if pd.notna(val) and val != ".":
                            new_rows.append([date.strftime('%Y-%m-%d'), info[1], info[2], round(float(val)/divisor, 3)])
                    time.sleep(0.2)
                except: continue

            # 중복 제거 및 정렬 업로드
            if new_rows:
                df_final = pd.DataFrame(new_rows, columns=["Date", "Category", "Name", "Value"])
                df_final = df_final.drop_duplicates(subset=["Date", "Name"], keep='last')
                df_final = df_final.sort_values(by=["Date", "Name"])
                sheet.append_rows(df_final.values.tolist())
                print(f"✅ {group_name} 업데이트 완료")
        except Exception as e:
            print(f"🚨 {group_name} 실패: {e}")

if __name__ == "__main__":
    daily_combined_update()
