import requests
from io import BytesIO
import pandas as pd
import numpy as np
from biz_day import Bizday

class Krx_sise:

    def daily_kospi(biz_day):
        gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
        gen_otp = {
            'locale': 'ko_KR',
            'idxIndMidclssCd': '02',
            'trdDd': biz_day,
            'share': '2',
            'money': '4',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT00101'
            }

        headers = {
                'Referer':'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
                }

        otp_stk = requests.post(gen_otp_url,gen_otp,headers=headers).text

        down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
        down = requests.post(down_url, {'code':otp_stk}, headers=headers)
        daily_kospi_value = pd.read_csv(BytesIO(down.content), encoding='EUC-KR')
        
        daily_kospi_value = daily_kospi_value.replace({np.nan:None})
        daily_kospi_value['지수명'] = daily_kospi_value['지수명'].str.replace(' ','')
        daily_kospi_value = daily_kospi_value[~daily_kospi_value['지수명'].str.contains('외국주')]
        daily_kospi_value = daily_kospi_value.apply(pd.to_numeric,errors = 'ignore')
        daily_kospi_value.insert(0,'날짜',biz_day)
        daily_kospi_value = daily_kospi_value[(daily_kospi_value['지수명'] == '코스피')]
        return daily_kospi_value
    
    def daily_kosdaq(biz_day):
        gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
        gen_otp = {
            'locale': 'ko_KR',
            'idxIndMidclssCd': '03',
            'trdDd': biz_day,
            'share': '2',
            'money': '4',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT00101'
            }

        headers = {
                'Referer':'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
                }

        otp_stk = requests.post(gen_otp_url,gen_otp,headers=headers).text

        down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
        down = requests.post(down_url, {'code':otp_stk}, headers=headers)
        daily_kosdaq_value = pd.read_csv(BytesIO(down.content), encoding='EUC-KR')
        daily_kosdaq_value = daily_kosdaq_value.replace({np.nan:None})
        daily_kosdaq_value['지수명'] = daily_kosdaq_value['지수명'].str.replace(' ','')
        daily_kosdaq_value = daily_kosdaq_value[~daily_kosdaq_value['지수명'].str.contains('외국주')]
        daily_kosdaq_value = daily_kosdaq_value.apply(pd.to_numeric,errors = 'ignore')
        daily_kosdaq_value.insert(0,'날짜',biz_day)
        daily_kosdaq_value = daily_kosdaq_value[(daily_kosdaq_value['지수명'] == '코스닥')]
        return daily_kosdaq_value
    
    def merge_sise(biz_day):
        kospi = Krx_sise.daily_kospi(biz_day)
        kosdaq = Krx_sise.daily_kosdaq(biz_day)
        df = pd.concat([kospi,kosdaq])
        df = pd.pivot_table(df,index='날짜',columns='지수명',values='종가',aggfunc='sum')
        return df
    
if __name__ == '__main__':
    start_day = '20241230'
    end_day = Bizday.biz_day()

    start_day_df = Krx_sise.merge_sise(start_day)
    end_day_df = Krx_sise.merge_sise(end_day)
        
    df = pd.concat([start_day_df,end_day_df],axis=0)
    df['1230일코스닥'] = df['코스닥'].shift(1)
    df['1230일코스피'] = df['코스피'].shift(1)

    df['코스닥YTD'] = round(df['코스닥'].diff()/df['코스닥'].iloc[0]*100,1)
    df['코스피YTD'] = round(df['코스피'].diff()/df['코스피'].iloc[0]*100,1)
    
    df.to_clipboard()