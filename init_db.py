import os, json, gspread, time
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from fredapi import Fred

def init_sheet():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_json = json.loads(os.environ.get('GSPREAD_JSON'))
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(os.environ.get('SPREADSHEET_ID')).sheet1

    fred = Fred(api_key=os.environ.get('FRED_API_KEY'))
    all_data = []
    start_date = '1970-01-01'

    # 사용자 요청 1~5번 + 필수 원자재 + 가계지표 완벽 포함 딕셔너리
    # [카테고리, 이름, 단위변환]
    fred_dict = {
        # 1. 유동성 및 정책
        'WALCL': ['Liquidity', '연준총자산', 1000000],
        'M2SL': ['Money', 'M2통화량', 1000],
        'WTREGEN': ['Liquidity', 'TGA잔고', 1],
        'RRPONTSYD': ['Liquidity', '역레포잔고', 1],
        'DFEDTARU': ['Policy', '기준금리(상단)', 1],

        # 2. 시장 지수 및 자산 가격 (+ 필수 원자재 & 코인)
        'NASDAQ100': ['Index', '나스닥100', 1],
        'SP500': ['Index', 'S&P500', 1],
        'DJIA': ['Index', '다우존스', 1],
        'DCOILWTICO': ['Energy', 'WTI원유', 1],
        'CBBTCUSD': ['Crypto', '비트코인', 1],
        'CBETHUSD': ['Crypto', '이더리움', 1],
        'GOLDAMGBD228NLBM': ['Commodity', '금_현물', 1],
        'ID71081': ['Commodity', '은_현물', 1],
        'PCOPPUSDM': ['Commodity', '구리_현물', 1],

        # 3. 금리 및 채권
        'T10Y2Y': ['Rates', '장단기금리차', 1],
        'DGS10': ['Rates', '미_10년물_금리', 1],
        'DGS2': ['Rates', '미_2년물_금리', 1],
        'BAMLH0A0HYM2': ['Risk', '정크본드스프레드', 1],

        # 4. 경제 및 물가 지표
        'CPIAUCSL': ['Inflation', 'CPI', 1],
        'PPIACO': ['Inflation', 'PPI', 1],
        'UNRATE': ['Economy', '실업률', 1],
        'GDPC1': ['Economy', '실질GDP', 1],
        'CSUSHPINSA': ['Housing', '미국주택가격지수', 1],
        'UMCSENT': ['Sentiment', '소비자심리지수', 1],

        # 5. 환율 및 변동성
        'DEXKOUS': ['Currency', '원달러환율', 1],
        'DTWEXBGS': ['Currency', '달러인덱스', 1],
        'VIXCLS': ['Volatility', 'VIX공포지수', 1],

        # 6. 추가 경제지표 (가계지출, 대출, 소매판매, 내구재 등)
        'PCE': ['Economy', '개인소비지출', 1],
        'RSXFS': ['Economy', '소매판매', 1],
        'DGORDER': ['Economy', '내구재주문', 1],
        'TDSP': ['Banking', '가계부채상환비율', 1],
        'TOTLL': ['Banking', '은행총대출', 1],
        'H8B1029NCBCMG': ['Banking', '상업은행대출_부동산', 1] # 추가 판단: 부동산 대출 추이
    }

    for ticker, info in fred_dict.items():
        print(f"FRED 수집 중: {info[1]} ({ticker})...")
        try:
            s = fred.get_series(ticker, observation_start=start_date)
            if not s.empty:
                for date, val in s.items():
                    if pd.notna(val) and val != ".":
                        all_data.append([date.strftime('%Y-%m-%d'), info[0], info[1], round(float(val)/info[2], 3)])
            time.sleep(0.3)
        except Exception as e:
            print(f"에러 ({info[1]}): {e}")

    # 데이터 정렬 (날짜 -> 종목명)
    all_data.sort(key=lambda x: (x[0], x[2]))

    if all_data:
        print(f"총 {len(all_data)}행 업로드 시작...")
        sheet.clear()
        sheet.append_row(["Date", "Category", "Name", "Value"])
        
        batch_size = 3000
        for i in range(0, len(all_data), batch_size):
            sheet.append_rows(all_data[i:i+batch_size])
            print(f"{i} / {len(all_data)} 완료")
            time.sleep(1)
        print("✅ 모든 데이터 로드 완료!")

if __name__ == "__main__":
    init_sheet()
