import requests
from io import BytesIO
import pandas as pd
import numpy as np
import time
from biz_day import Bizday
from loguru import logger

class BaseCode:
    def base_info(name = None):
        try:
            gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
            gen_otp = {
                    'locale': 'ko_KR',
                    'mktId': 'ALL',
                    'share': '1',
                    'csvxls_isNo': 'false',
                    'name': 'fileDown',
                    'url': 'dbms/MDC/STAT/standard/MDCSTAT01901'
                    }
            headers = {
                    'Referer':'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
                    }
            otp_stk = requests.post(gen_otp_url,gen_otp,headers=headers).text
            down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
            down = requests.post(down_url, {'code':otp_stk}, headers=headers)
            corp_code = pd.read_csv(BytesIO(down.content), encoding='EUC-KR')
            corp_code.columns = corp_code.columns.str.replace(' ','')
            corp_code = corp_code[['표준코드','단축코드','한글종목약명']]
            corp_code.columns = ['표준코드','종목코드','종목명']
            if name == None:
                return corp_code
            corp_code = corp_code.replace({np.nan:None})
            corp_code = corp_code[corp_code['종목명'] == name]
            stCode = corp_code.iloc[0,0]
            stockCode = corp_code.iloc[0,1] 
        except Exception as e:
            logger.info(e)
        return stCode,stockCode

if __name__ == '__main__':
    stockName = 'AJ네트웍스'
    stCode,stockCode = BaseCode.base_info(stockName)
    print(stCode,stockCode)