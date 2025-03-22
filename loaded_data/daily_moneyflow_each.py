import requests
from io import BytesIO
import pandas as pd
import numpy as np
import time
from datetime import datetime,timedelta
import holidays
from pandas.tseries.offsets import CustomBusinessDay
from tqdm import tqdm
import os
from openpyxl import load_workbook

class KrxMoney:
    @staticmethod
    def daily_money_flow(biz_day, gubun):
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
            'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
        }

        try:
            otp_stk = requests.post(gen_otp_url, gen_otp, headers=headers).text
            down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
            down = requests.post(down_url, {'code': otp_stk}, headers=headers)
            down.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"파일 다운로드 실패: {e}")
            return pd.DataFrame()

        data = pd.read_csv(BytesIO(down.content), encoding='EUC-KR')

        # 불필요한 종목 제거
        data = data[(~data['종목명'].str.endswith('우')) & (~data['종목명'].str.contains('스팩', na=False)) & (~data['종목명'].str.contains('리츠', na=False))]
        data['거래대금_순매수'] = round(data['거래대금_순매수'] / 100000000, 0)  # 억 단위 변환
        data = data.replace({np.nan: None})

        gubun_dict = {
            '1000': '금융투자', '3000': '투신', '3100': '사모', '6000': '연기금',
            '9000': '외국인', '8000': '개인'
        }
        data.insert(0, '구분', gubun_dict.get(gubun, '기타'))
        data.insert(0, '날짜', biz_day)

        return data[['날짜', '구분', '종목명', '거래대금_순매수']]

    @staticmethod
    def save(df):
        folder_path = '거래대금_엑셀'
        os.makedirs(folder_path, exist_ok=True)

        file_path = os.path.join(folder_path, f"거래대금.xlsx")

        try:
            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                for gubun in df['구분'].unique():
                    df_gubun = df[df['구분'] == gubun]

                    df_pivot = df_gubun.pivot(index=['구분', '종목명'], columns='날짜', values='거래대금_순매수').reset_index()
                    df_pivot.columns = df_pivot.columns.str.strip()

                    df_pivot.to_excel(writer, sheet_name=gubun, index=False)

                # Merge 시트 생성 (각 구분별 최근 날짜 기준 상위 20개 추출)
                selected_date = -1
                
                
                
        
                merged_df_list = []
                for g in df['구분'].unique():
                    unique_dates = sorted(df[df['구분'] == g]['날짜'].unique())  # 해당 구분의 날짜 정렬
                    
                    if len(unique_dates) < 3:  # 데이터가 부족하면 넘어가기
                        continue
                    
                    target_date = unique_dates[selected_date]  # 최신 날짜에서 -1일
                    # target_date = unique_dates[-3]  # 최신 날짜에서 -2일 (원하는 경우)

                    top_20_df = df[(df['구분'] == g) & (df['날짜'] == target_date)].nlargest(20, '거래대금_순매수')
                    merged_df_list.append(top_20_df)

                merged_df = pd.concat(merged_df_list) if merged_df_list else pd.DataFrame()
                
                # 주가 데이터 가져오기

                biz_day = (datetime.today() + timedelta(days=selected_date+1)).strftime('%Y%m%d')
                print(biz_day)
                price_df = get_price(biz_day)[['종목명', '등락률']]
                
                # 병합 및 저장
                merged_df = merged_df.merge(price_df, on='종목명', how='left')
                merged_df = merged_df.sort_values(by='등락률',ascending=False)
                merged_df.to_excel(writer, sheet_name='merge', index=False)

            print("거래대금 엑셀 파일이 저장되었습니다.")
        except Exception as e:
            print("엑셀 저장 오류:", e)


def get_price(biz_day):
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
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
    }
    time.sleep(0.5)
    otp_stk = requests.post(gen_otp_url, gen_otp, headers=headers).text.strip()

    down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
    down = requests.post(down_url, {'code': otp_stk}, headers=headers)

    daily_updown = pd.read_csv(BytesIO(down.content), encoding='EUC-KR')
    daily_updown['등락률'] = daily_updown['등락률'].astype(float)
    daily_updown['종목명'] = daily_updown['종목명'].str.strip()
    daily_updown.insert(0, '기준일', biz_day)

    return daily_updown[['기준일', '종목명', '등락률']]


if __name__ == '__main__':
    biz_day = datetime.today().strftime('%Y%m%d')
    gubuns = ['1000', '3000', '3100', '6000', '9000', '8000']
    period = 20

    df = pd.DataFrame()

    kr_holidays = holidays.Korea(years=[2024, 2025])
    holidays_list = list(kr_holidays.keys())
    kr_business_day = CustomBusinessDay(weekmask="Mon Tue Wed Thu Fri", holidays=holidays_list)

    for i in tqdm(range(period)):
        start_date = datetime.strptime(biz_day, '%Y%m%d')
        target_date = (start_date - (i * kr_business_day)).strftime('%Y%m%d')

        for gubun in gubuns:
            rst_df = KrxMoney.daily_money_flow(target_date, gubun)
            df = pd.concat([df, rst_df]).drop_duplicates(subset=['날짜', '구분', '종목명'], keep='last')

    KrxMoney.save(df)
    print('Excel 저장 완료!')