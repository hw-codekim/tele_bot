from daily_stock_price import Daily_stock_price
from kospi_kosday import Krx_sise
from datetime import datetime
import pandas as pd
from tqdm import tqdm
import holidays
from pandas.tseries.offsets import CustomBusinessDay, BDay

def calculate_change(start_day, end_day):
    # 1. 기준일과 종료일 지수 데이터 호출
    start_day_df = Krx_sise.merge_sise(start_day)
    end_day_df = Krx_sise.merge_sise(end_day)

    # 2. 두 날짜 데이터 합치기
    df = pd.concat([start_day_df, end_day_df], axis=0)

    # 3. 기준일 지수 추가 (이전 날 지수값)
    df['기준일코스닥'] = df['코스닥'].shift(1)
    df['기준일코스피'] = df['코스피'].shift(1)

    # 4. YTD (연초 대비 수익률) 계산
    df['코스닥증감률'] = round(df['코스닥'].diff() / df['코스닥'].iloc[0] * 100, 1)
    df['코스피증감률'] = round(df['코스피'].diff() / df['코스피'].iloc[0] * 100, 1)

    return df


def calculate_market_cap_change(start_day, end_day):
    # 1. 기준일과 종료일 시가총액 데이터 호출
    stock_start_df = Daily_stock_price.price(start_day)
    stock_end_df = Daily_stock_price.price(end_day)
    
    # 2. 기준일 컬럼이 없을 경우 생성 (정수형으로 통일)
    stock_start_df['날짜'] = int(start_day)
    stock_end_df['날짜'] = int(end_day)
    
    # 3. 종목명 기준 merge
    merged = pd.merge(
        stock_start_df[['종목명', '시가총액', '시장구분']],  # '시장구분' 컬럼 포함
        stock_end_df[['종목명', '시가총액', '시장구분']],  # '시장구분' 컬럼 포함
        on='종목명',
        suffixes=('_start', '_end')
    )

    # 4. 증감률 계산
    merged['증감률(%)'] = ((merged['시가총액_end'] - merged['시가총액_start']) / merged['시가총액_start'] * 100).round(1)

    # 5. 시장구분에 따른 RS 비교
    merged['RS_비교'] = merged.apply(
        lambda row: 'KOSPI RS' if row['시장구분_start'] == 'KOSPI' else 
                    ('KOSDAQ RS' if row['시장구분_start'] == 'KOSDAQ' else 'KOSDAQ RS'),
        axis=1
    )
    
    # 6. 결과 추가
    result = merged[['종목명', '시가총액_end', '증감률(%)','시장구분_start', 'RS_비교']]
    result = result.sort_values('증감률(%)', ascending=False).reset_index(drop=True)
    result.insert(0, '날짜', end_day)
    
    return result


def calculate_rs(stock_df, jisu_df, start_day, end_day):
    # 1. 날짜 정보 추가 (stock_df에 날짜 컬럼을 추가)
    stock_df['날짜'] = pd.to_datetime(stock_df['날짜'], format='%Y%m%d')
    # 2. 지수 데이터에 기준 날짜 추가
    jisu_df['날짜'] = pd.to_datetime(jisu_df['날짜'], format='%Y%m%d')

    # 3. 주식 데이터와 지수 데이터를 날짜를 기준으로 merge
    merged_df = pd.merge(stock_df[['날짜', '종목명', '증감률(%)','시장구분_start','시가총액_end']], 
                         jisu_df[['날짜', '코스닥증감률', '코스피증감률']], 
                         on='날짜', how='left')

    # 4. RS 계산 (각 종목의 증감률 / 코스닥 또는 코스피 증감률)
    # 시장구분에 따라 각각의 RS 계산
    merged_df['RS'] = merged_df.apply(
    lambda row: (row['증감률(%)'] - row['코스닥증감률']) / abs(row['코스닥증감률']) if row['시장구분_start'] == 'KOSDAQ' else 
                (row['증감률(%)'] - row['코스피증감률']) / abs(row['코스피증감률']),
    axis=1).round(1)

    # 5. RS 순위 계산
    merged_df['RS_순위'] = merged_df['RS'].rank(ascending=False, method='min')

    # 6. 결과를 반환 (RS와 RS 순위, 시가총액_end)
    result = merged_df[['날짜', '종목명', 'RS', 'RS_순위', '시가총액_end']]
    result = result.sort_values('RS', ascending=False).reset_index(drop=True)
    result = result[(result['시가총액_end'] >= 5000)&(result['RS'] >= 10)]
    return result


if __name__ == '__main__':
    start_day = '20250223'
    end_day = '20250523'

    jisu_df = calculate_change(start_day, end_day).reset_index()
    stock_df = calculate_market_cap_change(start_day, end_day)

    # RS 계산
    rs_df = calculate_rs(stock_df, jisu_df, start_day, end_day)
    
    rs_df.to_clipboard()  # 최종 결과를 클립보드로 복사
    print(rs_df)  # 최종 결과 출력
