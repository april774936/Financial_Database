import os, json, gspread, time
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from fredapi import Fred
from datetime import datetime, timedelta

def daily_update():
    # 1. 인증 설정
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

    # 지표 분류 (기존과 동일)
    fred_dict = {
        'NASDAQ100': ['ASSETS', 'Index', '나스닥100', 1],
        'SP500': ['ASSETS', 'Index', 'S&P500', 1],
        'WALCL': ['LIQUID', 'Liquidity', '연준총자산', 1000000],
        'CPIAUCSL': ['MACRO', 'Inflation', 'CPI', 1],
        # ... (이전 코드에 있던 모든 지표 리스트를 여기에 넣으시면 됩니다)
    }

    # 어제 날짜 기준으로 데이터 수집
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

    for group_name, sheet_id in sheets_info.items():
        if not sheet_id: continue
        
        sheet = client.open_by_key(sheet_id).sheet1
        existing_data = sheet.get_all_values()
        # 중복 방지를 위해 기존 날짜+이름 조합 저장
        existing_keys = set([(row[0], row[2]) for row in existing_data[1:]])
        
        new_rows = []
        # 해당 그룹에 속한 지표만 필터링해서 수집
        group_tickers = {k: v for k, v in fred_dict.items() if v[0] == group_name}
        
        for ticker, info in group_tickers.items():
            try:
                s = fred.get_series(ticker, observation_start=start_date)
                for date, val in s.items():
                    d_str = date.strftime('%Y-%m-%d')
                    if pd.notna(val) and val != "." and (d_str, info[2]) not in existing_keys:
                        new_rows.append([d_str, info[1], info[2], round(float(val)/info[3], 3)])
            except: continue
        
        if new_rows:
            # 날짜순 정렬 후 추가
            new_rows.sort(key=lambda x: x[0])
            sheet.append_rows(new_rows)
            print(f"✅ {group_name} 시트에 {len(new_rows)}개 데이터 추가 완료")

if __name__ == "__main__":
    daily_update()
