import requests
from bs4 import BeautifulSoup
import re
from io import BytesIO
import pandas as pd
import numpy as np
import time
import re
from datetime import datetime,timedelta
import pandas as pd
import numpy as np
import pandas.tseries.offsets as offsets


class Krx_money:
    
    # if not hasattr(collections, 'Callable'):
    #     collections.Callable = collections.abc.Callable
    
    # def biz_day(): # 네이버에서 날짜가져오기
    #     url = 'https://finance.naver.com/sise/sise_index.naver?code=KOSPI'
    #     res = requests.get(url)
    #     soup = BeautifulSoup(res.text,'html.parser')

    #     parse_day = soup.select_one('#time').text
        
    #     biz_day = re.findall('[0-9]+',parse_day)
    #     biz_day = ''.join(biz_day)
    #     return biz_day

    def daily_money_flow(biz_day,gubun):
        gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
        gen_otp = {
            'locale': 'ko_KR',
            'mktId': 'ALL',
            'invstTpCd': f'{gubun}',
            'strtDd': f'{biz_day}',
            'endDd': f'{biz_day}',
            'share': '1',
            'money': '1',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT02401'
            }

        headers = {
                'Referer':'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
                }

        otp_stk = requests.post(gen_otp_url,gen_otp,headers=headers).text

        down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
        down = requests.post(down_url, {'code':otp_stk}, headers=headers)
        data = pd.read_csv(BytesIO(down.content), encoding='EUC-KR')
        data = data[(~data['종목명'].str.endswith('우')) &
                    (~data['종목명'].str.endswith('우B'))
                    ]
        data['거래대금_매도'] = round(data['거래대금_매도']/100000000,0)
        data['거래대금_매수'] = round(data['거래대금_매수']/100000000,0)
        data['거래대금_순매수'] = round(data['거래대금_순매수']/100000000,0)
        data = data.replace({np.nan:None})
        
        if gubun == '1000': # 금융투자
            data.insert(0,'구분','금융투자')
        elif gubun == '3000': # 투신
            data.insert(0,'구분','투신')
        elif gubun == '3100': # 사모
            data.insert(0,'구분','사모')        
        elif gubun == '6000': # 연기금
            data.insert(0,'구분','연기금')
        elif gubun == '9000': # 외국인
            data.insert(0,'구분','외국인')
        elif gubun == '8000': # 개인
            data.insert(0,'구분','개인')
        data.insert(0,'날짜',biz_day)
        data = data[['날짜','구분','종목명','거래대금_순매수']]
        time.sleep(1)
        return data

if __name__ == '__main__':
    start_date = "20250102"
    end_date = datetime.today().strftime("%Y%m%d")

    # 영업일(주말 제외) 생성
    biz_days = pd.date_range(start=start_date, end=end_date, freq="B")  # "B" = Business day

    # 한국 공휴일 리스트 (직접 추가 필요)
    korean_holidays = ["2025-01-27","2025-01-28","2025-01-29","2025-01-30","2025-02-09", "2025-02-12"]  # 예시: 설날 연휴

    # 공휴일 제외
    biz_days = [day.strftime("%Y%m%d") for day in biz_days if day.strftime("%Y-%m-%d") not in korean_holidays]

    gubuns = ['1000', '3000', '3100', '6000']
    
    # ✅ **병렬 처리를 위해 요청 데이터를 미리 리스트로 만들기**
    all_results = []

    for biz_day in biz_days:
        print(f"Processing {biz_day}...")
        results = [Krx_money.daily_money_flow(biz_day, gubun) for gubun in gubuns]
        df = pd.concat(results, ignore_index=True)
        all_results.append(df)

    # ✅ **모든 데이터를 한 번에 DataFrame으로 변환**
    result_df = pd.concat(all_results, ignore_index=True)

    # ✅ **날짜를 컬럼으로 확장**
    result_pivot = result_df.pivot_table(index='종목명', columns='날짜', values='거래대금_순매수', aggfunc='sum')

    result_pivot.to_clipboard()
    