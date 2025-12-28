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

    # 최근 1년치만 수집 (용량 최적화 및 NotebookLM 연동 안정성 확보)
    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

    # 전체 지표 리스트
    fred_dict = {
        'NASDAQ100': ['ASSETS', 'Index', '나스닥100', 1],
        'SP500': ['ASSETS', 'Index', 'S&P500', 1],
        'DJIA': ['ASSETS', 'Index', '다우존스', 1],
        'DCOILWTICO': ['ASSETS', 'Energy', 'WTI원유', 1],
        'CBBTCUSD': ['ASSETS', 'Crypto', '비트코인', 1],
        'CBETHUSD': ['ASSETS', 'Crypto', '이더리움', 1],
        'GOLDAMGBD228NLBM': ['ASSETS', 'Commodity', '금_현물', 1],
        'ID71081': ['ASSETS', 'Commodity', '은_현물', 1],
        'PCOPPUSDM': ['ASSETS', 'Commodity', '구리_현물', 1],
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

    for group_name, sheet_id in sheets_info.items():
        if not sheet_id: continue
        try:
            sheet = client.open_by_key(sheet_id).sheet1
            sheet.clear() # 기존 데이터 모두 삭제
            sheet.append_row(["Date", "Category", "Name", "Value"])
            
            new_rows = []
            group_tickers = {k: v for k, v in fred_dict.items() if v[0] == group_name}
            
            for ticker, info in group_tickers.items():
                s = fred.get_series(ticker, observation_start=start_date)
                for date, val in s.items():
                    if pd.notna(val) and val != ".":
                        new_rows.append([date.strftime('%Y-%m-%d'), info[1], info[2], round(float(val)/info[3], 3)])
            
            if new_rows:
                # 날짜순 정렬 후 업로드
                new_rows.sort(key=lambda x: x[0])
                sheet.append_rows(new_rows)
            print(f"✅ {group_name} 시트 업데이트 성공 (최근 1년 데이터)")
        except Exception as e:
            print(f"❌ {group_name} 업데이트 실패: {e}")

if __name__ == "__main__":
    daily_light_update()
