import os, json, gspread, time
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
from fredapi import Fred

def daily_pro_valuation_update():
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

    start_date = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')
    today_str = datetime.now().strftime('%Y-%m-%d')

    # A. yfinance 타겟 (자산 가격 및 ETF 펀더멘털)
    yf_map = {
        '^NDX': ['ASSETS', 'Index', '나스닥100'],
        '^GSPC': ['ASSETS', 'Index', 'S&P500'],
        '^DJI': ['ASSETS', 'Index', '다우존스30'],
        'BTC-USD': ['ASSETS', 'Crypto', '비트코인'],
        'ETH-USD': ['ASSETS', 'Crypto', '이더리움'],
        'GC=F': ['ASSETS', 'Commodity', '골드(금)'],
        'SI=F': ['ASSETS', 'Commodity', '실버(은)'],
        'HG=F': ['ASSETS', 'Commodity', '구리_현물'],
        'CL=F': ['ASSETS', 'Energy', 'WTI원유'],
        'DX-Y.NYB': ['MACRO', 'Currency', '달러인덱스']
    }

    # 밸류에이션 전용 타겟 (LIQUID 시트로 분류)
    valuation_tickers = {
        'SPY': 'S&P500',
        'QQQ': '나스닥100'
    }

    # B. FRED 타겟
    fred_map = {
        'DFEDTARU': ['LIQUID', 'Policy', '기준금리(상단)', 1],
        'WALCL': ['LIQUID', 'Liquidity', '연준총자산', 1000000],
        'WTREGEN': ['LIQUID', 'Liquidity', 'TGA잔고', 1],
        'RRPONTSYD': ['LIQUID', 'Liquidity', '역레포잔고', 1],
        'T10Y2Y': ['LIQUID', 'Rates', '장단기금리차', 1],
        'DGS10': ['LIQUID', 'Rates', '미_10년물_금리', 1],
        'VIXCLS': ['LIQUID', 'Volatility', 'VIX공포지수', 1],
        'BUSLOANS': ['LIQUID', 'Economy', '은행총대출', 1],
        'CPIAUCSL': ['MACRO', 'Inflation', 'CPI', 1],
        'PPIACO': ['MACRO', 'Inflation', 'PPI', 1],
        'UNRATE': ['MACRO', 'Labor', '실업률', 1],
        'DEXKOUS': ['MACRO', 'Currency', '원달러환율', 1]
    }

    for group_name, sheet_id in sheets_info.items():
        if not sheet_id: continue
        try:
            sheet = client.open_by_key(sheet_id).sheet1
            new_rows = []

            # --- yfinance 수집 (가격) ---
            group_yf = {k: v for k, v in yf_map.items() if v[0] == group_name}
            for ticker, info in group_yf.items():
                df = yf.download(ticker, start=start_date, progress=False)
                if not df.empty:
                    close_series = df['Close'][ticker] if isinstance(df['Close'], pd.DataFrame) else df['Close']
                    for date, val in close_series.tail(365).items():
                        new_rows.append([date.strftime('%Y-%m-%d'), info[1], info[2], round(float(val), 2)])

            # --- yfinance 수집 (EPS & PE - LIQUID 시트로 추가) ---
            if group_name == 'LIQUID':
                for t_code, t_name in valuation_tickers.items():
                    print(f"Valuation 수집 중: {t_name}")
                    t_info = yf.Ticker(t_code).info
                    pe_val = t_info.get('trailingPE')
                    eps_val = t_info.get('trailingEps')
                    
                    if pe_val:
                        new_rows.append([today_str, 'Valuation', f'{t_name}_PE', round(float(pe_val), 2)])
                    if eps_val:
                        new_rows.append([today_str, 'Valuation', f'{t_name}_EPS', round(float(eps_val), 2)])

            # --- FRED 수집 ---
            group_fred = {k: v for k, v in fred_map.items() if v[0] == group_name}
            for ticker, info in group_fred.items():
                try:
                    s = fred.get_series(ticker, observation_start=start_date)
                    for date, val in s.items():
                        if pd.notna(val) and val != ".":
                            new_rows.append([date.strftime('%Y-%m-%d'), info[1], info[2], round(float(val)/info[3], 3)])
                except: continue

            # 중복 제거 및 시트 업데이트
            if new_rows:
                final_df = pd.DataFrame(new_rows, columns=["Date", "Category", "Name", "Value"])
                final_df = final_df.drop_duplicates(subset=["Date", "Name"], keep='last')
                final_df = final_df.sort_values(by=["Date", "Name"])
                
                sheet.clear()
                sheet.append_row(["Date", "Category", "Name", "Value"])
                sheet.append_rows(final_df.values.tolist())
                print(f"✅ {group_name} 업데이트 완료")
            
            time.sleep(1)
        except Exception as e:
            print(f"🚨 {group_name} 에러: {e}")

if __name__ == "__main__":
    daily_pro_valuation_update()
