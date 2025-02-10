import requests
from io import BytesIO
import pandas as pd
import numpy as np
import time
from biz_day import Bizday


class Krx_daily_price:
    def daily_price(biz_day):
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
        daily_updown = daily_updown[['기준일','종목코드','종목명','시가총액','종가','등락률']]
        print(f'[{biz_day}] 종목 {len(daily_updown)}개 로딩 성공')
        time.sleep(1)
        return daily_updown

if __name__ == '__main__':
    # start_day = '20241230'
    # end_day = Bizday.biz_day()

    # start_day_df = Krx_daily_price.daily_price(start_day)
    # end_day_df = Krx_daily_price.daily_price(end_day)
    # df = pd.concat([start_day_df,end_day_df],axis=0)

    # df = df.sort_values(by=['종목명','기준일'],ascending=True)
    # df['1230일종가'] = df.groupby('종목명')['시가총액'].shift(1)
    # df['YTD'] = round(df.groupby('종목명')['시가총액'].diff(1)/df.groupby('종목명')['시가총액'].shift(0) * 100,1)
    # df['YTD'].dropna(inplace=True)
    # df = df.sort_values(by = 'YTD',ascending=False)
    # df = df.head(50)
    # df.to_clipboard()
    
    start_day = '20241230'
    end_day = Bizday.biz_day()

    # 원하는 종목 리스트
    mypick_stock = ['HD현대마린솔루션', 'TYM','산일전기','슈어소프트테크','하이브','한화비전','SAMG엔터','JYP Ent.','SG','브이티',
                    '레뷰코퍼레이션','쏠리드','엠로','에스피소프트','글로벌텍스프리','리메드','세경하이테크','쓰리빌리언','선익시스템']
    # mypick_stock = ['SG']
    # 매주 금요일 기준일 생성
    week_end_dates = pd.date_range(start=start_day, end=end_day, freq='W-FRI').strftime('%Y%m%d')

    # 각 주차별 데이터 저장
    weekly_data = []
    for week_end in week_end_dates:
        df_week = Krx_daily_price.daily_price(week_end)
        df_week['기준일'] = week_end  # 기준일 컬럼 추가
        weekly_data.append(df_week)
    today_data = Krx_daily_price.daily_price(end_day)
    weekly_data.append(today_data)
    # 데이터프레임 결합
    df = pd.concat(weekly_data, axis=0)

    # 원하는 종목만 필터링
    df = df[df['종목명'].isin(mypick_stock)]
    print(df)
    # 정렬
    df = df.sort_values(by=['종목명', '기준일'], ascending=True)

    # 주차별 변동률 계산
    df['이전 시가총액'] = df.groupby('종목명')['시가총액'].shift(1)
    df['주차별 변동률'] = round(df.groupby('종목명')['시가총액'].pct_change(1) * 100, 1)

    # 피벗 테이블 생성 (종목별 + 주차별 변동률)
    df_pivot = df.pivot_table(index='종목명', columns='기준일', values='주차별 변동률')

    # YTD 계산 (첫 주 금요일 ~ 마지막 금요일 변동률)
    ytd_series = round((df.groupby('종목명')['시가총액'].last() - df.groupby('종목명')['시가총액'].first()) / df.groupby('종목명')['시가총액'].first() * 100, 1)

    # df_pivot과 ytd_series 인덱스를 맞춤
    ytd_series = ytd_series.reindex(df_pivot.index)

    # YTD 추가
    df_pivot['YTD'] = ytd_series

    # 종가 추가 (마지막 기준일의 종가 사용)
    closing_price_series = df[df['기준일'] == df['기준일'].max()].set_index('종목명')['종가']

    # df_pivot과 closing_price_series 인덱스를 맞춤
    closing_price_series = closing_price_series.reindex(df_pivot.index)

    # 종가 추가
    df_pivot['종가'] = closing_price_series

    # 원하는 컬럼 순서로 정렬 (YTD → 주차별 변동률 → 종가)
    columns_order = ['YTD'] + [col for col in df_pivot.columns if col not in ['YTD', '종가']] + ['종가']
    df_pivot = df_pivot[columns_order]

    # 클립보드 복사 (엑셀 붙여넣기 가능)
    df_pivot.to_clipboard()

    # 출력 확인
    print(df_pivot)

