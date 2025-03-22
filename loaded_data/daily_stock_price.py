import requests
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import datetime,timedelta
from biz_day import Bizday
import holidays
from pandas.tseries.offsets import CustomBusinessDay
from tqdm import tqdm
import os

class Daily_stock_price:
    def price(biz_day):
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

        otp_stk = requests.post(gen_otp_url,gen_otp,headers=headers).text

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
                        (~daily_updown['소속부'].str.contains('리츠', na=False)) &
                        (~daily_updown['소속부'].str.contains('SPAC', na=False)) &
                        (~daily_updown['소속부'].str.contains('관리', na=False))
                        ]
        daily_updown = daily_updown[['기준일','종목명','시가총액']]
        print(f'[{biz_day}] 종목 {len(daily_updown)}개 로딩 성공')
        return daily_updown
    
    def save(df):
        filename='./거래대금_엑셀/종목별_가격트렌드.csv'  # 엑셀 저장 함수
        try:
            if not os.path.exists(filename):
                df.to_csv(filename, mode='w', encoding='utf-8-sig', index=False)
        except Exception as e:
            print(e)
        finally:    
            print(f"{filename} 파일이 저장되었습니다")


if __name__ == '__main__':
    biz_day = datetime.today().strftime('%Y%m%d')
    period = 20

    df = pd.DataFrame()

    kr_holidays = holidays.Korea(years=[2024, 2025])
    holidays_list = list(kr_holidays.keys())
    kr_business_day = CustomBusinessDay(weekmask="Mon Tue Wed Thu Fri", holidays=holidays_list)

    for i in tqdm(range(period)):
        start_date = datetime.strptime(biz_day, '%Y%m%d')
        target_date = (start_date - (i * kr_business_day)).strftime('%Y%m%d')
        rst_df = Daily_stock_price.price(target_date)
        df = pd.concat([df, rst_df]).drop_duplicates(subset=['기준일','종목명'], keep='last')
    
    # Pivot to reshape the dataframe
    df_pivoted = df.pivot_table(index='종목명', columns='기준일', values='시가총액').reset_index()
    Daily_stock_price.save(df_pivoted)
    print(df_pivoted)