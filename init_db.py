import os, json, gspread, time
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from fredapi import Fred

def init_split_sheets():
    # 1. 인증 및 API 설정
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_json = json.loads(os.environ.get('GSPREAD_JSON'))
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
    client = gspread.authorize(creds)
    fred = Fred(api_key=os.environ.get('FRED_API_KEY'))
    
    # [수정포인트] 시트 이름과 환경변수에 등록한 ID 매칭
    sheets_info = {
        'ASSETS': os.environ.get('SHEET_ID_ASSETS'),     # 시트 1 ID
        'LIQUID': os.environ.get('SHEET_ID_LIQUID'),     # 시트 2 ID
        'MACRO': os.environ.get('SHEET_ID_MACRO')       # 시트 3 ID
    }

    # 지표 분류 정의
    fred_dict = {
        # ASSETS 그룹 (시장 지수 및 자산)
        'NASDAQ100': ['ASSETS', 'Index', '나스닥100', 1],
        'SP500': ['ASSETS', 'Index', 'S&P500', 1],
        'DJIA': ['ASSETS', 'Index', '다우존스', 1],
        'DCOILWTICO': ['ASSETS', 'Energy', 'WTI원유', 1],
        'CBBTCUSD': ['ASSETS', 'Crypto', '비트코인', 1],
        'CBETHUSD': ['ASSETS', 'Crypto', '이더리움', 1],
        'GOLDAMGBD228NLBM': ['ASSETS', 'Commodity', '금_현물', 1],
        'ID71081': ['ASSETS', 'Commodity', '은_현물', 1],
        'PCOPPUSDM': ['ASSETS', 'Commodity', '구리_현물', 1],

        # LIQUID 그룹 (유동성 및 금리)
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

        # MACRO 그룹 (경제지표 및 환율)
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

    # 데이터 수집용 딕셔너리 초기화
    split_data = {'ASSETS': [], 'LIQUID': [], 'MACRO': []}

    for ticker, info in fred_dict.items():
        print(f"수집 중: {info[2]}...")
        try:
            s = fred.get_series(ticker, observation_start='1970-01-01')
            group = info[0]
            for date, val in s.items():
                if pd.notna(val) and val != ".":
                    split_data[group].append([date.strftime('%Y-%m-%d'), info[1], info[2], round(float(val)/info[3], 3)])
            time.sleep(0.3)
        except Exception as e: print(f"에러 {ticker}: {e}")

    # 시트별 업로드
    for group, sheet_id in sheets_info.items():
        if not sheet_id: continue
        print(f"{group} 시트 업로드 시작...")
        target_sheet = client.open_by_key(sheet_id).sheet1
        target_sheet.clear()
        target_sheet.append_row(["Date", "Category", "Name", "Value"])
        
        group_list = sorted(split_data[group], key=lambda x: (x[0], x[2]))
        batch_size = 3000
        for i in range(0, len(group_list), batch_size):
            target_sheet.append_rows(group_list[i:i+batch_size])
            time.sleep(1)
        print(f"✅ {group} 시트 완료!")

if __name__ == "__main__":
    init_split_sheets()
