import os, json, gspread, time
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
from fredapi import Fred

def daily_ultimate_update():
    # 1. 인증 및 환경 설정
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    try:
        creds_json = json.loads(os.environ.get('GSPREAD_JSON'))
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
        client = gspread.authorize(creds)
        fred = Fred(api_key=os.environ.get('FRED_API_KEY'))
    except Exception as e:
        print(f"인증 실패: {e}")
        return

    sheets_info = {
        'ASSETS': os.environ.get('SHEET_ID_ASSETS'), 
        'LIQUID': os.environ.get('SHEET_ID_LIQUID'), 
        'MACRO': os.environ.get('SHEET_ID_MACRO')
    }

    # 데이터 누락 방지를 위해 2년치 조회
    start_date = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')
    today_str = datetime.now().strftime('%Y-%m-%d')

    # A. yfinance 타겟 (가격 및 PE/EPS)
    yf_map = {
        '^NDX': ['ASSETS', 'Index', '나스닥100'],
        '^GSPC': ['ASSETS', 'Index', 'S&P500'],
        'BTC-USD': ['ASSETS', 'Crypto', '비트코인'],
        'ETH-USD': ['ASSETS', 'Crypto', '이더리움'],
        'GC=F': ['ASSETS', 'Commodity', '골드(금)'],
        'HG=F': ['ASSETS', 'Commodity', '구리_현물'],
        'DX-Y.NYB': ['MACRO', 'Currency', '달러인덱스']
    }
    
    valuation_tickers = {'SPY': 'S&P500', 'QQQ': '나스닥100'}

    # B. FRED 타겟 (M2, 리스크 지표 등) - 구문 오류 완벽 수정
    fred_map = {
        'WM2NS': ['LIQUID', 'Liquidity', 'M2통화량', 1],
        'WALCL': ['LIQUID', 'Liquidity', '연준총자산', 1000000],
        'RRPONTSYD': ['LIQUID', 'Liquidity', '역레포잔고', 1],
        'WTREGEN': ['LIQUID', 'Liquidity', 'TGA잔고', 1],
        'BAMLH0A0HYM2': ['LIQUID', 'Rates', '하이일드스프레드', 1],
        'STLPPM': ['LIQUID', 'Volatility', '금융스트레스지수', 1],
        'VIXCLS': ['LIQUID', 'Volatility', 'VIX공포지수', 1],
        'DFEDTARU': ['LIQUID', 'Policy', '기준금리(상단)', 1],
        'T10Y2Y': ['LIQUID', 'Rates', '장단기금리차', 1],
        'DGS10': ['LIQUID', 'Rates', '미_10년물_금리', 1],
        'CPIAUCSL': ['MACRO', 'Inflation', 'CPI', 1],
        'PCEPI': ['MACRO', 'Inflation', 'PCE물가', 1],
        'UNRATE': ['MACRO', 'Labor', '실업률', 1],
        'BUSLOANS': ['MACRO', 'Economy', '은행총대출', 1],
        'DEXKOUS': ['MACRO', 'Currency', '원달러환율', 1]
    }

    for group_name, sheet_id in sheets_info.items():
        if not sheet_id: continue
        try:
            doc = client.open_by_key(sheet_id)
            sheet = doc.sheet1
            new_rows = []

            # --- yfinance 수집 ---
            group_yf = {k: v for k, v in yf_map.items() if v[0] == group_name}
            for ticker, info in group_yf.items():
                df = yf.download(ticker, start=start_date, progress=False)
                if not df.empty:
                    close_series = df['Close'][ticker] if isinstance(df['Close'], pd.DataFrame) else df['Close']
                    for date, val in close_series.tail(400).items():
                        new_rows.append([date.strftime('%Y-%m-%d'), info[1], info[2], round(float(val), 2)])

            # --- 밸류에이션 수집 (LIQUID 전용) ---
            if group_name == 'LIQUID':
                for t_code, t_name in valuation_tickers.items():
                    t_obj = yf.Ticker(t_code)
                    pe = t_obj.info.get('trailingPE')
                    eps = t_obj.info.get('trailingEps')
                    if pe: new_rows.append([today_str, 'Valuation', f'{t_name}_PE', round(float(pe), 2)])
                    if eps: new_rows.append([today_str, 'Valuation', f'{t_name}_EPS', round(float(eps), 2)])

            # --- FRED 수집 ---
            group_fred = {k: v for k, v in fred_map.items() if v[0] == group_name}
            for ticker, info in group_fred.items():
                try:
                    s = fred.get_series(ticker, observation_start=start_date)
                    for date, val in s.items():
                        if pd.notna(val) and val != ".":
                            new_rows.append([date.strftime('%Y-%m-%d'), info[1], info[2], round(float(val)/info[3], 3)])
                except: continue

            # --- 데이터 업로드 ---
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
    daily_ultimate_update()
