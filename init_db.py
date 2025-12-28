import os, json, gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from fredapi import Fred

def init_sheet():
    # 1. 인증 및 시트 연결
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_json = json.loads(os.environ.get('GSPREAD_JSON'))
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
    client = gspread.authorize(creds)
    sheet_id = os.environ.get('SPREADSHEET_ID')
    sheet = client.open_by_key(sheet_id).sheet1

    fred = Fred(api_key=os.environ.get('FRED_API_KEY'))
    
    # 가져올 지표들 및 단위 변환 설정
    indicators = {
        'WALCL': ['유동성', '연준총자산', 1000000],
        'M2SL': ['유동성', 'M2통화량', 1000],
        'WTREGEN': ['유동성', 'TGA잔고', 1000],
        'RRPONTSYD': ['유동성', '역레포잔고', 1],
        'BAMLH0A0HYM2': ['리스크', '정크본드스프레드', 1]
    }

    all_data = []
    # 2020년부터 현재까지의 데이터를 모두 가져옵니다
    for ticker, info in indicators.items():
        print(f"Fetching: {info[1]}...")
        series = fred.get_series(ticker, observation_start='2020-01-01')
        for date, val in series.items():
            if pd.notna(val):
                all_data.append([date.strftime('%Y-%m-%d'), info[0], info[1], round(val/info[2], 2)])

    # 데이터 날짜순 정렬
    all_data.sort(key=lambda x: x[0])

    # 시트 초기화 및 헤더 추가
    sheet.clear()
    sheet.append_row(["Date", "Category", "Name", "Value"])
    
    # 데이터가 너무 많으면 끊어서 보냄 (안정성)
    batch_size = 500
    for i in range(0, len(all_data), batch_size):
        sheet.append_rows(all_data[i:i+batch_size])
    
    print(f"✅ 총 {len(all_data)}개의 과거 데이터 로드 완료!")

if __name__ == "__main__":
    init_sheet()
