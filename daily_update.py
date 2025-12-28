import os, json, gspread, time
from datetime import datetime, timedelta
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from fredapi import Fred

def daily_light_update():
    # 1. 인증 및 API 설정
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

    # 최근 1년치만 수집
    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

    # 지표 리스트 (가장 안정적인 티커로 재구성)
    fred_dict = {
        # --- ASSETS (자산) ---
        'WILL5000IND': ['ASSETS', 'Index', '미국전체주식지수', 1],
        'DCOILWTICO': ['ASSETS', 'Energy', 'WTI원유', 1],
        'CBBTCUSD': ['ASSETS', 'Crypto', '비트코인', 1],
        'GOLDAMGBD228NLBM': ['ASSETS', 'Commodity', '금_현물', 1],
        
        # --- LIQUID (유동성/금리) ---
        'WALCL': ['LIQUID', 'Liquidity', '연준총자산', 1000000],
        'M2SL': ['LIQUID', 'Money', 'M2통화량', 1000],
        'WTREGEN': ['LIQUID', 'Liquidity', 'TGA잔고', 1],
        'RRPONTSYD': ['LIQUID', 'Liquidity', '역레포잔고', 1],
        'DFEDTARU': ['LIQUID', 'Policy', '기준금리(상단)', 1],
        'T10Y2Y': ['LIQUID', 'Rates', '장단기금리차', 1],
        'DGS10': ['LIQUID', 'Rates', '미_10년물_금리', 1],
        'DGS2': ['LIQUID', 'Rates', '미_2년물_금리', 1],
        'VIXCLS': ['LIQUID', 'Volatility', 'VIX공포지수', 1],
        
        # --- MACRO (거시경제) ---
        'CPIAUCSL': ['MACRO', 'Inflation', 'CPI', 1],
        'PPIACO': ['MACRO', 'Inflation', 'PPI', 1],
        'UNRATE': ['MACRO', 'Economy', '실업률', 1],
        'GDPC1': ['MACRO', 'Economy', '실질GDP', 1],
        'DEXKOUS': ['MACRO', 'Currency', '원달러환율', 1],
        'DTWEXBGS': ['MACRO', 'Currency', '달러인덱스', 1]
    }

    for group_name, sheet_id in sheets_info.items():
        if not sheet_id:
            print(f"경고: {group_name} 시트 ID가 설정되지 않았습니다.")
            continue
            
        try:
            print(f"--- {group_name} 업데이트 시작 ---")
            sheet = client.open_by_key(sheet_id).sheet1
            sheet.clear()
            sheet.append_row(["Date", "Category", "Name", "Value"])
            
            new_rows = []
            group_tickers = {k: v for k, v in fred_dict.items() if v[0] == group_name}
            
            for ticker, info in group_tickers.items():
                print(f"[{group_name}] 수집 시도: {ticker} ({info[2]})")
                try:
                    s = fred.get_series(ticker, observation_start=start_date)
                    if s.empty:
                        continue
                    for date, val in s.items():
                        if pd.notna(val) and val != ".":
                            new_rows.append([date.strftime('%Y-%m-%d'), info[1], info[2], round(float(val)/info[3], 3)])
                    time.sleep(0.5)
                except Exception as e:
                    print(f"❌ {ticker} 수집 실패: {e}")
                    continue
            
            if new_rows:
                new_rows.sort(key=lambda x: x[0])
                sheet.append_rows(new_rows)
                print(f"✅ {group_name} 시트 업데이트 성공!")
        except Exception as e:
            print(f"🚨 {group_name} 그룹 작업 중 오류 발생: {e}")

if __name__ == "__main__":
    daily_light_update()
