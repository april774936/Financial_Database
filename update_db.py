import os, json, gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

def update_sheet(data_list):
    # data_list 예시: [['2025-12-28', 'NQ=F', '나스닥 선물', 21000.50], ...]
    
    # 1. 인증 설정
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_json = json.loads(os.environ.get('GSPREAD_JSON'))
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
    client = gspread.authorize(creds)
    
    # 2. 시트 열기
    sheet_id = os.environ.get('SPREADSHEET_ID')
    sheet = client.open_by_key(sheet_id).sheet1 # 첫 번째 탭 사용
    
    # 3. 데이터 추가 (가장 아래 줄에 붙여넣기)
    sheet.append_rows(data_list)
    print("✅ 구글 시트 업데이트 완료!")

# 실행 예시 (실제 코드에서는 수집된 변수를 넣음)
# now = datetime.now().strftime('%Y-%m-%d %H:%M')
# update_sheet([[now, 'WALCL', '연준총자산', 6.56]])
