
import pandas as pd
import requests
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
from tqdm import tqdm
import parmap
from pandas.tseries.offsets import BDay



#52주 신고가 찾기

class Max: 
    def get_price(biz_day):
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
        daily_updown = daily_updown[
                        (daily_updown['시장구분'] != 'KONEX') &
                        (~daily_updown['종목명'].str.endswith('우')) &
                        (~daily_updown['종목명'].str.endswith('우B')) &
                        (~daily_updown['소속부'].str.contains('전환', na=False)) &
                        (~daily_updown['소속부'].str.contains('리츠', na=False)) &
                        (~daily_updown['소속부'].str.contains('SPAC', na=False)) &
                        (~daily_updown['소속부'].str.contains('관리', na=False))
                        ]
        
        return daily_updown

    def get_52_week_high(ref_day):

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
        biz_days = pd.date_range(start='2024-02-20', end=today, freq=BDay()).strftime('%Y%m%d').tolist()
        krx_holidays_str = [pd.to_datetime(date).strftime('%Y%m%d') for date in krx_holidays]
        biz_days = [day for day in biz_days if day not in krx_holidays_str]
        new_data = pd.DataFrame()
        for day in tqdm(biz_days, total=len(biz_days)):
            result = Max.get_price(day)
            new_data = pd.concat([new_data,result])

        high_prices = new_data.groupby('종목명')['시가총액'].max().reset_index()
        latest_data = new_data[new_data['기준일'] == today]

        # 신고가 판별
        result = latest_data.merge(high_prices, on='종목명', suffixes=('_현재', '_52주최고'))

        result['신고가'] = result['시가총액_현재'] == result['시가총액_52주최고']

        return result[result['신고가']]  # 신고가 종목만 반환
    
    
    def get_gap(ref_day):
        # today = datetime.today().strftime('%Y%m%d')
        result = Max.get_price(ref_day)
        return result
        
if __name__ == '__main__':
    day = '20250220'
    df = Max.get_52_week_high(day)
    df.to_csv(f'./saved_data/{day}_52주 신고가.csv', encoding='utf-8-sig',index=False)
    dd = Max.get_gap(day)
    dd.to_csv(f'./saved_data/{day}_종목별 등락률.csv', encoding='utf-8-sig', index=False)
    # print(df)
    
    
    