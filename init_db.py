import os, json, gspread, time
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from fredapi import Fred
import yfinance as yf

def init_sheet():
    # 1. 구글 시트 인증
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_json = json.loads(os.environ.get('GSPREAD_JSON'))
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(os.environ.get('SPREADSHEET_ID')).sheet1

    fred = Fred(api_key=os.environ.get('FRED_API_KEY'))
    all_data = []
    start_date = '1970-01-01'

    # 1. FRED 지표
    fred_dict = {
        'WALCL': ['Liquidity', '연준총자산', 1000000], 
        'M2SL': ['Money', 'M2통화량', 1000],
        'WTREGEN': ['Liquidity', 'TGA잔고', 1], 
        'RRPONTSYD': ['Liquidity', '역레포잔고', 1],
        'DPSACBW027SBKG': ['Banking', '은행총예금', 1], 
        'TOTLL': ['Banking', '은행총대출', 1],
        'DFEDTARU': ['Policy', '기준금리(상단)', 1], 
        'EFFR': ['Rates', 'EFFR', 1],
        'SOFR': ['Rates', 'SOFR', 1], 
        'IORB': ['Rates', 'IORB', 1],
        'T10Y2Y': ['Rates', '장단기금리차', 1], 
        'BAMLH0A0HYM2': ['Risk', '정크본드스프레드', 1],
        'UNRATE': ['Economy', '실업률', 1], 
        'CPIAUCSL': ['Inflation', 'CPI', 1],
        'CPILFESL': ['Inflation', 'Core_CPI', 1], 
        'PPIACO': ['Inflation', 'PPI', 1],
        'GDPC1': ['Economy', '실질GDP', 1], 
        'GFDEBTN': ['Debt', '총국가부채', 1000],
        'CSUSHPINSA': ['Housing', '미국주택가격지수', 1],
        'UMCSENT': ['Sentiment', '소비자심리지수', 1]
    }

    for ticker, info in fred_dict.items():
        print(f"FRED 수집 중: {info[1]}...")
        try:
            s = fred.get_series(ticker, observation_start=start_date)
            for date, val in s.items():
                if pd.notna(val):
                    all_data.append([date.strftime('%Y-%m-%d'), info[0], info[1], round(val/info[2], 3)])
            time.sleep(0.4)
        except Exception as e: print(f"FRED 에러 ({info[1]}): {e}")

    # 2. Yahoo Finance 지표
    yf_dict = {
        '^NDX': ['Index', '나스닥100'], 
        '^GSPC': ['Index', 'S&P500'],
        '^DJI': ['Index', '다우존스'], 
        '^RUT': ['Index', '러셀2000'],
        'BTC-USD': ['Crypto', '비트코인'], 
        'ETH-USD': ['Crypto', '이더리움'],
        'GC=F': ['Commodity', '금_선물'], 
        'HG=F': ['Commodity', '구리_선물'],
        'CL=F': ['Energy', 'WTI원유선물'],
        '^VIX': ['Volatility', 'VIX_공포지수'],
        'KRW=X': ['Currency', '원달러환율'],
        '^TNX': ['Rates', '미_10년물_금리'], 
        '^IRX': ['Rates', '미_3개월_금리'],
        'DX-Y.NYB': ['Currency', '달러인덱스']
    }

    for ticker, info in yf_dict.items():
        print(f"YFinance 수집 중: {info[1]}...")
        try:
            # 주가 데이터 수집
            df = yf.download(ticker, start=start_date, progress=False)
            if not df.empty:
                # 데이터가 1차원 Series로 올 수 있도록 처리
                close_data = df['Close']
                for date, val in close_data.items():
                    if pd.notna(val):
                        # 멀티인덱스나 Series 대응
                        v = float(val.iloc[0]) if hasattr(val, 'iloc') else float(val)
                        all_data.append([date.strftime('%Y-%m-%d'), info[0], info[1], round(v, 2)])
        except Exception as e: print(f"YFinance 에러 ({info[1]}): {e}")

    # 3. 데이터 정렬
    all_data.sort(key=lambda x: (x[0], x[2]))

    # 4. 시트 초기화 및 업로드
    print(f"시트 초기화... 총 {len(all_data)}행 업로드 시작")
    sheet.clear()
    sheet.append_row(["Date", "Category", "Name", "Value"])
    
    batch_size = 2000
    for i in range(0, len(all_data), batch_size):
        sheet.append_rows(all_data[i:i+batch_size])
        print(f"{i} / {len(all_data)} 행 완료...")
        time.sleep(1)
    
    print(f"✅ 역사적 데이터베이스 구축 완료!")

if __name__ == "__main__":
    init_sheet()
