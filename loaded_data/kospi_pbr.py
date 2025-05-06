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
from loguru import logger
import warnings
# FutureWarning 무시 설정
warnings.filterwarnings("ignore", category=FutureWarning)

class Kospi_pbr:
    # 종목 시세 2024.01.02 부터~ 현재까지
    def kospipbr(target_date):
        try:
            gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
            gen_otp = {
                'locale': 'ko_KR',
                'searchType': 'A',
                'idxIndMidclssCd': '02',
                'trdDd': f'{target_date}',
                'tboxindTpCd_finder_equidx0_0':'', 
                'indTpCd': '',
                'indTpCd2': '',
                'codeNmindTpCd_finder_equidx0_0': '',
                'param1indTpCd_finder_equidx0_0': '',
                'strtDd': '20250327',
                'endDd': '20250403',
                'csvxls_isNo': 'false',
                'name': 'fileDown',
                'url': 'dbms/MDC/STAT/standard/MDCSTAT00701'
                }
            headers = {
                    'Referer':'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
                    }

            otp_code = requests.post(gen_otp_url,gen_otp,headers=headers).text

            down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
            response = requests.post(down_url, {'code': otp_code}, headers=headers)      
            corp_trading_df = pd.read_csv(BytesIO(response.content), encoding='EUC-KR')
            corp_trading_df.columns = corp_trading_df.columns.str.replace(' ', '')
            corp_trading_df.insert(0,'일자',target_date)
        except Exception as e:
            logger.info(f'로딩실패',e)
        return corp_trading_df  

    def save(df):
        filename='./지수_트렌드/코스피PBR.csv'  # 엑셀 저장 함수
        try:
            if not os.path.exists(filename):
                df.to_csv(filename, mode='w', encoding='utf-8-sig', index=False)
        except Exception as e:
            print(e)
        finally:    
            print(f"{filename} 파일이 저장되었습니다")

if __name__ == '__main__':
    # df = Kospi_pbr.kospipbr()
    biz_day = datetime.today().strftime('%Y%m%d')
    period = 365

    df = pd.DataFrame()

    kr_holidays = holidays.Korea(years=[2024, 2025])
    holidays_list = list(kr_holidays.keys())
    kr_business_day = CustomBusinessDay(weekmask="Mon Tue Wed Thu Fri", holidays=holidays_list)

    for i in tqdm(range(period)):
        start_date = datetime.strptime(biz_day, '%Y%m%d')
        target_date = (start_date - (i * kr_business_day)).strftime('%Y%m%d')
        rst_df = Kospi_pbr.kospipbr(target_date)
        df = pd.concat([df, rst_df])
    
    Kospi_pbr.save(df)