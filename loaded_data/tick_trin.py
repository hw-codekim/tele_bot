
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



#TICK : 상승종목 수 - 하락종목 수 
#TRIN : (상승종목 수/상승종목 거래대금합) / (하락종목 수/하락종목 거래대금합)

#TICK 이 플러스이

class TickTrin: 
    def tick_trin(biz_day):
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
        # 상승/하락 종목
        up = daily_updown[daily_updown['등락률'] > 0]
        down = daily_updown[daily_updown['등락률'] < 0]

        up_count = len(up)
        down_count = len(down)
        up_amount = up['거래량'].sum()
        down_amount = down['거래량'].sum()
        
        tick = up_count - down_count
        trin = ((up_count / up_amount) / (down_count / down_amount)) if up_amount > 0 and down_amount > 0 else None

        summary = pd.DataFrame([{
            '날짜': biz_day,
            '상승종목 수': up_count,
            '상승거래대금': up_amount,
            '하락종목 수': down_count,
            '하락거래대금': down_amount,
            'TICK': tick,
            'TRIN': round(trin, 2) if trin else None
        }])
        
        return summary

if __name__ == '__main__':
    today = datetime.today().strftime('%Y%m%d')
    today = '20250418'
    df = TickTrin.tick_trin(today)
    
    print(df)