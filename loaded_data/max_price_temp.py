import pandas as pd
import requests
from io import BytesIO
import numpy as np
from datetime import datetime
import time
from tqdm import tqdm
from pandas.tseries.offsets import BDay

class Max: 
    @staticmethod
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
            'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
        }
        time.sleep(0.5)
        otp_stk = requests.post(gen_otp_url, gen_otp, headers=headers).text.strip()

        down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
        down = requests.post(down_url, {'code': otp_stk}, headers=headers)

        daily_updown = pd.read_csv(BytesIO(down.content), encoding='EUC-KR')
        daily_updown['시가총액'] = round(daily_updown['시가총액'] / 100000000, 0)
        daily_updown['거래대금'] = round(daily_updown['거래대금'] / 100000000, 1)
        daily_updown = daily_updown.replace({np.nan: None})
        daily_updown['종목명'] = daily_updown['종목명'].str.strip()
        daily_updown.insert(0, '기준일', biz_day)
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

    @staticmethod
    def get_52_week_high(biz_day, data):
        # 특정 날짜 기준으로 52주 신고가 종목 찾기
        one_year_ago = (datetime.strptime(biz_day, "%Y%m%d") - pd.DateOffset(years=1)).strftime('%Y%m%d')
        past_data = data[data['기준일'] >= one_year_ago]

        high_prices = past_data.groupby('종목명')['시가총액'].max().reset_index()
        today_data = past_data[past_data['기준일'] == biz_day]

        result = today_data.merge(high_prices, on='종목명', suffixes=('_현재', '_52주최고'))
        result['신고가'] = result['시가총액_현재'] == result['시가총액_52주최고']
        result = result[result['신고가']].sort_values(by='시가총액_현재', ascending=False)
        result = result[(result['시가총액_52주최고'] > 2000) & (result['시가'] != 0) & ~(result['종목명'].str.contains('스팩'))& ~(result['종목명'].str.contains('리츠'))]
        
        return result[['종목명']]

if __name__ == '__main__':
    today = datetime.today().strftime('%Y%m%d')
    
    # 1년 전 날짜 계산
    one_year_ago = (datetime.strptime(today, "%Y%m%d") - pd.DateOffset(years=1)).strftime('%Y%m%d')

    # 1년 전부터 오늘까지의 영업일 가져오기
    biz_days = pd.date_range(start=one_year_ago, end=today, freq=BDay()).strftime('%Y%m%d').tolist()

    all_data = pd.DataFrame()
    
    for day in tqdm(biz_days, total=len(biz_days)):
        daily_data = Max.get_price(day)
        all_data = pd.concat([all_data, daily_data])

    results_dict = {}
    
    for day in tqdm(biz_days, total=len(biz_days)):
        result = Max.get_52_week_high(day, all_data)
        results_dict[day] = result['종목명'].tolist()

    # 날짜별 종목 리스트를 데이터프레임으로 변환
    max_length = max(len(lst) for lst in results_dict.values())  # 최대 종목 개수 찾기
    df_dict = {day: lst + [None] * (max_length - len(lst)) for day, lst in results_dict.items()}  # 빈칸 채우기
    df_52_week_highs = pd.DataFrame(df_dict)
    df_52_week_highs.insert(0, "No", range(1, max_length + 1))  # No 컬럼 추가

    # CSV 저장
    df_52_week_highs.to_csv(f'./saved_data/{today}_52주_신고가_리스트.csv', encoding='utf-8-sig', index=False)