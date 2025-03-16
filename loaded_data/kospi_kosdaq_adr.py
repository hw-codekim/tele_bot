
import pandas as pd
import requests
from io import BytesIO
import pandas as pd
import numpy as np

import parmap
from pandas.tseries.offsets import BDay
from datetime import datetime
import matplotlib.pyplot as plt
import time
from tqdm import tqdm


# 그냥 발생한 
class Daily_stockprice: 
    def get_kospi_kosdaq(biz_day):
        gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
        gen_otp = {
            'locale': 'ko_KR',
            'mktId': 'ALL',
            'trdDd': biz_day,
            'share': '1',
            'money': '1',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT01501'
            }

        headers = {
                'Referer':'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
                }
        time.sleep(0.5)
        otp_stk = requests.post(gen_otp_url,gen_otp,headers=headers).text.strip()

        down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
        down = requests.post(down_url, {'code':otp_stk}, headers=headers)
        
        daily_updown = pd.read_csv(BytesIO(down.content), encoding='EUC-KR')
        daily_updown['시가총액'] = round(daily_updown['시가총액']/100000000,0)
        daily_updown['거래대금'] = round(daily_updown['거래대금']/100000000,1)
        daily_updown = daily_updown.replace({np.nan:None})
        daily_updown['종목명'] = daily_updown['종목명'].str.strip()
        daily_updown.insert(0,'기준일',biz_day)
        today = datetime.today().strftime('%Y%m%d')
        if today == biz_day:
            daily_updown.to_csv(f'[{today}]종목별등락률.csv',index=False)
        daily_updown = daily_updown[
                        (daily_updown['시장구분'] != 'KONEX') &
                        (~daily_updown['종목명'].str.endswith('우')) &
                        (~daily_updown['종목명'].str.endswith('우B')) &
                        (~daily_updown['소속부'].str.contains('전환', na=False)) &
                        (~daily_updown['소속부'].str.contains('리츠', na=False)) &
                        (~daily_updown['소속부'].str.contains('SPAC', na=False)) &
                        (~daily_updown['소속부'].str.contains('관리', na=False))
                        ]
        daily_updown
        daily_updown = daily_updown[['기준일','등락률']]
        daily_updown.set_index(['기준일'],inplace=True)
        daily_updown['등락률'] = daily_updown['등락률'].astype(float)
        result = []       
        for date, changes in daily_updown.groupby(level=0):  # 날짜별 그룹핑
            up = (changes['등락률'] > 0).sum()
            down = (changes['등락률'] < 0).sum()
            flat = (changes['등락률'] == 0).sum()
            if up != 0 or down != 0 or flat != 0:
                result.append([date, up, down, flat])  # 날짜별로 정리
        result_df = pd.DataFrame(result, columns=['날짜','상승','하락','유지'])
        print(result_df)
        return result_df
    
    def get_adr():
        
        # 공휴일 리스트
        krx_holidays = [
            '2025-01-01',  # 신정
            '2025-02-28', '2025-03-01',  # 삼일절 연휴
            '2025-05-05',  # 어린이날
            '2025-06-06',  # 현충일
            '2025-08-15',  # 광복절
            '2025-09-08', '2025-09-09', '2025-09-10',  # 추석 연휴
            '2025-10-03',  # 개천절
            '2025-12-25',  # 성탄절
        ]

        # 📌 오늘 날짜
        today = datetime.today().strftime('%Y%m%d')
        biz_days = pd.date_range(start='2025-01-01', end=today, freq=BDay()).strftime('%Y%m%d').tolist()
        krx_holidays_str = [pd.to_datetime(date).strftime('%Y%m%d') for date in krx_holidays]
        biz_days = [day for day in biz_days if day not in krx_holidays_str]
        new_data = pd.DataFrame()
        for day in tqdm(biz_days, total=len(biz_days)):
            result = Daily_stockprice.get_kospi_kosdaq(day)
            new_data = pd.concat([new_data,result])
            
        new_data['상승MA20'] = new_data['상승'].rolling(window=20).sum().fillna(0)
        new_data['하락MA20'] = new_data['하락'].rolling(window=20).sum().fillna(0)

        # ADR 계산 (0으로 나누는 오류 방지)
        new_data['ADR'] = new_data.apply(lambda row: round(row['상승MA20'] / row['하락MA20'] * 100, 1) if row['하락MA20'] > 0 else None, axis=1)
        new_data = new_data.dropna(subset=['ADR'])
        
        return new_data
    
    def get_graph(df):
        plt.figure(figsize=(10, 5))
        plt.plot(df['날짜'], df['ADR'], marker='o', linestyle='-', color='b', label='ADR')

        # 가로선 추가
        plt.axhline(y=120, color='r', linestyle='--', label='Threshold 120')
        plt.axhline(y=70, color='g', linestyle='--', label='Threshold 70')

        # 그래프 꾸미기
        plt.xticks(rotation=45)  # X축 날짜 가독성 증가
        plt.xlabel("날짜")
        plt.ylabel("ADR")
        plt.title("ADR 꺾은선 그래프")
        plt.legend()
        plt.grid(True)

        # 그래프 출력
        plt.show()

        
if __name__ == '__main__':
    day = '20240305'
    # dd = Daily_stockprice.get_kospi_kosdaq(day)
    df = Daily_stockprice.get_adr()
    Daily_stockprice.get_graph(df)