
import pandas as pd
import requests
from io import BytesIO
import pandas as pd
import numpy as np
import parmap
# from ..biz_day import Bizday

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
                        (~daily_updown['소속부'].str.contains('전환', na=False)) &
                        (~daily_updown['소속부'].str.contains('리츠', na=False)) &
                        (~daily_updown['소속부'].str.contains('SPAC', na=False)) &
                        (~daily_updown['소속부'].str.contains('관리', na=False))
                        ]
        daily_updown = daily_updown[['기준일','등락률']]
        daily_updown.set_index(['기준일'],inplace=True)
        daily_updown['등락률'] = daily_updown['등락률'].astype(float)
        print(daily_updown)
        result = []
        for date,changes in daily_updown.iterrows():
            print(date)
            print('===============')
            print(changes)
            up, down, flat = 0, 0, 0  # 상승, 하락, 보합 개수 초기화

            for symbol,val in changes.items():
                if val > 0 :
                    up += 1
                elif val < 0 :
                    down += 1
                else:
                    flat += 1
                result.append([up,down,flat])
        result_df = pd.DataFrame(result, columns=['날짜','상승','하락','유지'])
        result_df.set_index('날짜',inplace=True)
        result_df['합합'] = result_df['상승'].sum()
        print(result_df)
        result_df['상승MA20'] = result_df['상승'].rolling(window=20).sum()
        result_df['하락MA20'] = result_df['하락'].rolling(window=20).sum()

        result_df['ADR'] = round(result_df['상승MA20']/result_df['하락MA20']*100,1)
        print(result_df)
        return result_df

if __name__ == '__main__':
    # day = Bizday.biz_day()
    day = '20250207'
    days = Daily_stockprice.get_kospi_kosdaq(day)
    results = parmap.map(Daily_stockprice.get_kospi_kosdaq, days, pm_processes=4, pm_pbar=True) # 
    new_data = pd.concat(results, ignore_index=True)
    print(new_data)