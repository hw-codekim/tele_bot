import requests
from io import BytesIO
import pandas as pd
from biz_day import Bizday
from loguru import logger
from baseCode import BaseCode
import warnings
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.font_manager as fm

# FutureWarning 무시 설정
warnings.filterwarnings("ignore", category=FutureWarning)

class Value:
    # 종목 시세 2024.01.02 부터~ 현재까지
    def corp_value(endDd,stockName,stCode,stockCode):
        try:
            gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
            gen_otp = {
                'locale': 'ko_KR',
                'searchType': 2,
                'mktId': 'ALL',
                'trdDd': {endDd},
                'tboxisuCd_finder_stkisu0_0': {stockCode},
                'isuCd': {stCode},
                'isuCd2': 'KR7005930003',
                'codeNmisuCd_finder_stkisu0_0': 'AJ네트웍스',
                'param1isuCd_finder_stkisu0_0': 'ALL',
                'strtDd': '20200102',
                'endDd': {endDd},
                'csvxls_isNo': 'false',
                'name': 'fileDown',
                'url': 'dbms/MDC/STAT/standard/MDCSTAT03502'
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
            corp_trading_df['ROE'] = round(corp_trading_df['PBR']/corp_trading_df['PER']*100,1)
        except Exception as e:
            logger.info(f'{stockName} 로딩실패',e)
        return corp_trading_df  
    
    def get_graph(df):
        plt.rc('font', family='Malgun Gothic')  # Windows
        plt.rcParams['axes.unicode_minus'] = False # 마이너스 기호 깨짐 방지
        
        df['일자'] = pd.to_datetime(df['일자'])
        fig, ax1 = plt.subplots(figsize=(10, 5))

        # 첫 번째 y축 (PBR)
        ax1.plot(df['일자'], df['PBR'], linestyle='-', color='b', label='PBR')
        ax1.set_ylabel("PBR", color='b')
        ax1.tick_params(axis='y', labelcolor='b')
        ax1.grid(True, linestyle='--', alpha=0.5)

        # 두 번째 y축 (ROE)
        ax2 = ax1.twinx()
        ax2.plot(df['일자'], df['ROE'], alpha=0.9, color='r', label='ROE')
        ax2.set_ylabel("ROE", color='r')
        ax2.tick_params(axis='y', labelcolor='r')

        # X축 날짜 가독성 증가
        ax1.xaxis.set_major_locator(mdates.AutoDateLocator())  # 날짜 간격 자동 조정
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))  # 날짜 포맷 설정

        # X축 레이블 간격 조정 (너무 촘촘한 경우)
        interval = max(1, len(df) // 5)  # 전체 데이터 크기에 따라 간격 조절
        ax1.set_xticks(df['일자'].iloc[::interval])  # 일정 간격마다 X축 표시


        # 제목 추가
        plt.title(f"{stockName}_PBR & ROE")

        # 범례 추가
        ax1.legend(loc='upper left')
        ax2.legend(loc='upper right')

        # 그래프 출력
        plt.show()

if __name__ == '__main__':
    day = Bizday.biz_day()
    endDd = day
    stockName = '유바이오로직스'
    stCode,stockCode = BaseCode.base_info(stockName)
    trading_df = Value.corp_value(endDd,stockName,stCode,stockCode)
    print(trading_df)
    Value.get_graph(trading_df)
