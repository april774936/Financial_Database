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

    # 최근 1년치만 수집 (용량 최적화)
    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

    # 지표 리스트 (생략 - 이전과 동일한 fred_dict 사용)
    # ... (여기에 34개 지표 리스트 삽입) ...

    for group_name, sheet_id in sheets_info.items():
        if not sheet_id: continue
        sheet = client.open_by_key(sheet_id).sheet1
        sheet.clear() # 기존 데이터 삭제 (중요!)
        sheet.append_row(["Date", "Category", "Name", "Value"])
        
        new_rows = []
        group_tickers = {k: v for k, v in fred_dict.items() if v[0] == group_name}
        for ticker, info in group_tickers.items():
            s = fred.get_series(ticker, observation_start=start_date)
            for date, val in s.items():
                if pd.notna(val):
                    new_rows.append([date.strftime('%Y-%m-%d'), info[1], info[2], round(float(val)/info[3], 3)])
        
        sheet.append_rows(new_rows)
        print(f"✅ {group_name} 시트 업데이트 완료!")

if __name__ == "__main__":
    daily_light_update()
