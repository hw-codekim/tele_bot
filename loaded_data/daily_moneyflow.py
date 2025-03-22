import requests
from io import BytesIO
import pandas as pd
import numpy as np
import time
from biz_day import Bizday
from loguru import logger
from baseCode import BaseCode
import warnings
import matplotlib.pyplot as plt
from scipy.signal import argrelextrema

# FutureWarning 무시 설정
warnings.filterwarnings("ignore", category=FutureWarning)

class SiseTrade:
    # 종목 시세 2024.01.02 부터~ 현재까지
    def corp_sise(endDd,stockName,stCode,stockCode):
        try:
            gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
            gen_otp = {
                'locale': 'ko_KR',
                'tboxisuCd_finder_stkisu0_6': f'{stockCode}/{stockName}',
                'isuCd': stCode,
                'isuCd2': stockCode,
                'codeNmisuCd_finder_stkisu0_6': f'{stockName}',
                'param1isuCd_finder_stkisu0_6': 'ALL',
                'strtDd': '20240102',
                'endDd': endDd,
                'adjStkPrc_check': 'Y',
                'adjStkPrc': '2',
                'share': '1',
                'money': '1',
                'csvxls_isNo': 'false',
                'name': 'fileDown',
                'url': 'dbms/MDC/STAT/standard/MDCSTAT01701'
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
            corp_trading_df['MA5'] = corp_trading_df['종가'].rolling(window=5).mean()
            corp_trading_df['MA20'] = corp_trading_df['종가'].rolling(window=20).mean()
            corp_trading_df['MA50'] = corp_trading_df['종가'].rolling(window=50).mean()
            corp_trading_df['MA150'] = corp_trading_df['종가'].rolling(window=150).mean()
            # corp_trading_df['Typical_Price'] = (corp_trading_df['고가']+corp_trading_df['저가']+corp_trading_df['종가']) /3
            # corp_trading_df['cumTP'] = (corp_trading_df['Typical_Price']*corp_trading_df['거래량']).cumsum()
            # corp_trading_df['cumVol'] = corp_trading_df['거래량'].cumsum()
            # corp_trading_df['VWAP'] = corp_trading_df['cumTP'] / corp_trading_df['cumVol']
            # '일자'를 datetime 형식으로 변환
            corp_trading_df['일자'] = pd.to_datetime(corp_trading_df['일자'])

            # 원하는 형식(YYYY-MM-DD)으로 변환
            # corp_trading_df['일자'] = corp_trading_df['일자'].dt.strftime('%Y-%m-%d')
        except Exception as e:
            logger.info(f'{stockName} 로딩실패',e)
        return corp_trading_df  
    
    
        # 종목 투자자별 거래대금 2024.01.02 부터~ 현재까지
    def corp_trading(endDd,stockName,stCode,stockCode):
        try:
            gen_otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
            gen_otp = {
                'locale': 'ko_KR',
                'inqTpCd': '2',
                'trdVolVal': '2',
                'askBid': '3',
                'tboxisuCd_finder_stkisu0_1': f'{stockCode}/{stockName}',
                'isuCd': stCode,
                'isuCd2': stockCode,
                'codeNmisuCd_finder_stkisu0_1': f'{stockName}',
                'param1isuCd_finder_stkisu0_1': 'ALL',
                'strtDd': '20240102', # 1년전
                'endDd': endDd,
                'detailView': '1',
                'money': '1',
                'csvxls_isNo': 'false',
                'name': 'fileDown',
                'url': 'dbms/MDC/STAT/standard/MDCSTAT02303'
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
            corp_trading_df['기관'] = corp_trading_df['사모'] + corp_trading_df['투신'] + corp_trading_df['연기금등']
            corp_trading_df = corp_trading_df.set_index('일자')
            corp_trading_df.fillna(0, inplace=True)
            corp_trading_df = corp_trading_df.applymap(lambda x: round(x / 100000000, 1) if isinstance(x, (int, float)) else x)
            corp_trading_df.sort_values('일자', ascending=True, inplace=True)
            corp_trading_df = corp_trading_df[['기관','금융투자','투신', '사모','연기금등','개인','외국인']]
            

            # '일자'를 인덱스로 설정한 데이터프레임
            corp_trading_df.index = pd.to_datetime(corp_trading_df.index)

            # 누적합을 구하고 새로운 컬럼을 추가
            for col in corp_trading_df.columns:
                corp_trading_df[f'{col}_'] = corp_trading_df[col].cumsum()
            
            corp_trading_df = corp_trading_df.reset_index()
            
        except Exception as e:
            logger.info(f'{stockName} 로딩실패',e)
        return corp_trading_df    

    def merge(endDd,stockName,stCode,stockCode):
        trading_df = SiseTrade.corp_trading(endDd,stockName,stCode,stockCode)
        sise_df = SiseTrade.corp_sise(endDd,stockName,stCode,stockCode)
        merge_df = sise_df.merge(trading_df,how='left')
        merge_df['기외합'] = merge_df['기관']+merge_df['외국인'].rolling(window=1).mean()
        merge_df['기관5'] = merge_df['기관'].rolling(window=5).mean()
        merge_df['외국인5'] = merge_df['외국인'].rolling(window=20).mean()
        merge_df['수급오실레이터'] = merge_df['기외합'].rolling(window=5).mean()/merge_df['시가총액']*100
        
        return merge_df[-120:]
    
    def plot_dual_axis(df,stockName):
        plt.rcParams['font.family'] = 'Malgun Gothic'  # 윈도우
        plt.rcParams['axes.unicode_minus'] = False

        x_column = df['일자']
        left_y_column = df[['종가', 'MA20']]
        right_y_column = df['수급오실레이터']

        fig, ax1 = plt.subplots(figsize=(8, 4))

        # 첫 번째 y축 (왼쪽)
        ax1.plot(x_column, left_y_column['종가'], color='#333333', label='종가', linewidth=1.2)
        ax1.plot(x_column, left_y_column['MA20'], color='blue', label='MA20', linewidth=1.2, linestyle='-')  # MA20 선 추가
        ax1.tick_params(axis='y', labelcolor='#333333')
                
        # 두 번째 y축 (오른쪽)
        ax2 = ax1.twinx()
        ax2.plot(x_column, right_y_column, color='red', label='수급오실레이터', linewidth=1.2, linestyle='-')
        ax2.tick_params(axis='y', labelcolor='red')

        # 공통 x축 설정
        ax1.grid(color='lightgray', linestyle='--', linewidth=0.5)
        
        # 범례 설정 (왼쪽 위에 통합)
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left', fontsize=10)

        # 배경 설정
        fig.patch.set_facecolor('white')

        # 그래프 출력
        plt.title(f"[{stockName}] 시가총액과 기관+외인 수급오실레이터")
        plt.tight_layout()
        plt.savefig(F'./수급트렌드/{stockName}_{day}.png', dpi=100)  # 용량을 줄이기 위해 dpi 낮추기
        plt.show()
        

if __name__ == '__main__':
    day = Bizday.biz_day()
    strtDd = '20230102'
    endDd = day
    stockName = 'd'
    stCode,stockCode = BaseCode.base_info(stockName)
    trading_df = SiseTrade.corp_trading(endDd,stockName,stCode,stockCode)
    sise_df = SiseTrade.corp_sise(endDd,stockName,stCode,stockCode)
    df = SiseTrade.merge(endDd,stockName,stCode,stockCode)
    SiseTrade.plot_dual_axis(df,stockName)
    