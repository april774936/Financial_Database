import os, time
import pandas as pd
from fredapi import Fred

# 여기에 본인의 FRED API KEY를 입력하세요
FRED_API_KEY = '본인의_API_키_입력' 

def export_historical_to_txt():
    fred = Fred(api_key=FRED_API_KEY)
    
    # 지표 리스트 정의 (전체 34개)
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

    content_map = {'ASSETS': "", 'LIQUID': "", 'MACRO': ""}

    print("과거 데이터(1970-2024)를 FRED에서 가져오는 중... 잠시만 기다려주세요.")
    
    for ticker, info in fred_dict.items():
        print(f"다운로드 중: {info[2]} ({ticker})")
        try:
            # 2024년 말까지의 고정된 과거 데이터만 가져옴
            s = fred.get_series(ticker, observation_start='1970-01-01', observation_end='2024-12-31')
            group = info[0]
            for date, val in s.items():
                if pd.notna(val) and val != ".":
                    line = f"{date.strftime('%Y-%m-%d')}, {info[2]}, {round(float(val)/info[3], 3)}\n"
                    content_map[group] += line
            time.sleep(0.5) # API 부하 방지
        except Exception as e:
            print(f"오류 발생 ({ticker}): {e}")

    # 3개의 텍스트 파일로 저장
    for group, content in content_map.items():
        filename = f"HISTORICAL_{group}.txt"
        with open(filename, "w", encoding="utf-8") as f:
            f.write(f"{group} Historical Financial Data (1970-2024)\n")
            f.write("Format: Date, Indicator Name, Value\n")
            f.write("-" * 30 + "\n\n")
            f.write(content)
        print(f"✅ {filename} 생성 완료!")

if __name__ == "__main__":
    export_historical_to_txt()
