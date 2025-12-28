import os, json, gspread, time
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
from fredapi import Fred
import yfinance as yf

def init_sheet_matrix():
    # 1. 인증 및 시트 연결
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_json = json.loads(os.environ.get('GSPREAD_JSON'))
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(os.environ.get('SPREADSHEET_ID')).sheet1

    fred = Fred(api_key=os.environ.get('FRED_API_KEY'))
    # 최대한 과거 데이터를 가져오기 위해 시작일을 1970년으로 설정
    start_date = '1970-01-01'
    
    # 데이터 수집용 딕셔너리
    collected_dfs = []

    # 1. FRED 데이터 (티커: 이름)
    fred_dict = {
        'WALCL': '연준총자산', 'M2SL': 'M2통화량', 'WTREGEN': 'TGA잔고', 'RRPONTSYD': '역레포잔고',
        'DPSACBW027SBKG': '은행총예금', 'TOTLL': '은행총대출', 'DFEDTARU': '기준금리(상단)',
        'EFFR': 'EFFR', 'SOFR': 'SOFR', 'IORB': 'IORB', 'T10Y2Y': '장단기금리차',
        'BAMLH0A0HYM2': '정크본드스프레드', 'UNRATE': '실업률', 'CPIAUCSL': 'CPI',
        'CPILFESL': 'Core_CPI', 'PPIACO': 'PPI', 'GDPC1': '실질GDP', 'GFDEBTN': '총국가부채'
    }

    for ticker, name in fred_dict.items():
        print(f"FRED 수집 중: {name}...")
        try:
            s = fred.get_series(ticker, observation_start=start_date)
            df = pd.DataFrame(s, columns=[name])
            collected_dfs.append(df)
            time.sleep(0.2)
        except: pass

    # 2. Yahoo Finance 데이터
    yf_dict = {
        '^NDX': '나스닥100', '^GSPC': 'S&P500', '^DJI': '다우존스', '^RUT': '러셀2000',
        'BTC-USD': '비트코인', 'ETH-USD': '이더리움', 'GC=F': '금_선물', 'HG=F': '구리_선물',
        '^TNX': '미_10년물_금리', 'DX-Y.NYB': '달러인덱스'
    }

    for ticker, name in yf_dict.items():
        print(f"YFinance 수집 중: {name}...")
        try:
            df = yf.download(ticker, start=start_date, progress=False)['Close']
            df = pd.DataFrame(df)
            df.columns = [name]
            collected_dfs.append(df)
        except: pass

    # 3. 데이터 통합 및 피벗 (날짜 기준 병합)
    final_df = pd.concat(collected_dfs, axis=1)
    final_df.index = final_df.index.strftime('%Y-%m-%d')
    
    # 행(종목)과 열(날짜)을 바꿈 (Transpose)
    matrix_df = final_df.transpose()
    matrix_df.index.name = "Category/Date"
    
    # 시트 업로드를 위한 데이터 정리
    # 첫 행은 날짜들, 그 다음 행부터는 [종목명, 값, 값, 값...]
    header = ["Category/Date"] + matrix_df.columns.tolist()
    rows = []
    for name, row_data in matrix_df.iterrows():
        rows.append([name] + row_data.fillna("").tolist())

    # 4. 시트 쓰기
    sheet.clear()
    sheet.append_row(header)
    
    # 구글 시트는 열 제한이 있으므로 너무 많으면 에러가 날 수 있음 (최신 데이터 위주로 자를 수도 있음)
    # 일단 전체 전송 시도
    batch_size = 50 # 종목이 적으므로 행 단위로 안전하게
    for i in range(0, len(rows), batch_size):
        sheet.append_rows(rows[i:i+batch_size])
    
    print("✅ 매트릭스 형식으로 과거 데이터 로드 완료!")

if __name__ == "__main__":
    init_sheet_matrix()
