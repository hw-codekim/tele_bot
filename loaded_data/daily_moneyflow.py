import requests
from io import BytesIO
import pandas as pd
import numpy as np
from loguru import logger
import warnings

# FutureWarning 무시 설정
warnings.filterwarnings("ignore", category=FutureWarning)

class Krx_money:
    def  daily_money_flow(biz_day,gubun):
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
        data['시가총액'] = round(data['시가총액']/100000000,0)
        data['거래대금'] = round(data['거래대금']/100000000,1)
        data = data.replace({np.nan:None})
        data['종목명'] = data['종목명'].str.strip()
        data.insert(0,'기준일',biz_day)
        data = data[
                        (data['시장구분'] != 'KONEX') &
                        (~data['종목명'].str.endswith('우')) &
                        (~data['종목명'].str.endswith('우B')) &
                        (~data['소속부'].str.contains('전환', na=False)) &
                        (~data['소속부'].str.contains('리츠', na=False)) &
                        (~data['소속부'].str.contains('SPAC', na=False)) &
                        (~data['소속부'].str.contains('관리', na=False))
                        ]
        return data

    def corp_sise(biz_day):
        try:
            gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
            gen_otp = {
                'locale': 'ko_KR',
                'mktId': 'ALL',
                'strtDd': biz_day,
                'endDd': biz_day,
                'adjStkPrc_check': 'Y',
                'adjStkPrc': '2',
                'share': '1',
                'money': '1',
                'csvxls_isNo': 'false',
                'name': 'fileDown',
                'url': 'dbms/MDC/STAT/standard/MDCSTAT01602'
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
            corp_trading_df.sort_values('일자', ascending=True, inplace=True)
            corp_trading_df.reset_index(inplace=True)
            corp_trading_df.drop(columns='index',inplace=True)
            corp_trading_df['시가총액'] = round(corp_trading_df['시가총액']/100000000,0)
            corp_trading_df['거래대금'] = round(corp_trading_df['거래대금']/100000000,0)
            # corp_trading_df = corp_trading_df[(corp_trading_df['종목명'].str.endswith('우'))]# & (~corp_trading_df['종목명'].str.endswith('우B')) &
                    # (~corp_trading_df['종목명'].str.contains('전환', na=False)) & (~corp_trading_df['종목명'].str.contains('스팩', na=False))
                    # ]
        except Exception as e:
            logger.info(f'{biz_day} 로딩실패',e)
        return corp_trading_df 

    
if __name__ == '__main__':
    # t_df = Krx_money.daily_money_flow('20240109','3100')
    s_df = Krx_money.daily_money_flow('20250214','3100')
    s_df.to_clipboard()
        
