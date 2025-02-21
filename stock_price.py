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
    start_day = '20241230'
    end_day = Bizday.biz_day()

    # start_day_df = Krx_daily_price.daily_price(start_day)
    # end_day_df = Krx_daily_price.daily_price(end_day)
    # df = pd.concat([start_day_df,end_day_df],axis=0)
    # end_day_df.to_clipboard()
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
    # mypick_stock = ['HD현대마린솔루션', 'TYM','산일전기','슈어소프트테크','하이브','한화비전','SAMG엔터','JYP Ent.','SG','브이티',
    #                 '레뷰코퍼레이션','쏠리드','엠로','에스피소프트','글로벌텍스프리','리메드','세경하이테크','쓰리빌리언','선익시스템','ISC',
    #                 '성광벤드','라이프시맨틱스','에스지헬스케어','비츠로셀','삼성중공업','티에프이','인텔리안테크']
    
    # mypick_stock = [
    # "하이브", "JYP Ent.", "에스엠", "와이지엔터테인먼트", "디어유", "인텔리안테크", "쎄트렉아이", "에이치브이엠", "루미르", 
    # "AP위성", "제노코", "컨텍", "비츠로테크", "클래시스", "에이피알", "원텍", "비올", "하이로닉", "리메드", "레이저옵텍", 
    # "휴젤", "파마리서치", "대웅제약", "메디톡스", "바이오플러스", "휴메딕스", "제테마", "한국비엔씨", "삼성전자", "SK하이닉스", 
    # "티씨케이", "하나머티리얼즈", "원익QnC", "월덱스", "비씨엔씨", "케이엔제이", "코미코", "한솔아이원스", "HPSP", 
    # "주성엔지니어링", "원익IPS", "유진테크", "케이씨텍", "피에스케이", "테스", "브이엠", "파크시스템스", "넥스틴", 
    # "오로스테크놀로지", "펨트론", "인텍플러스", "제우스", "디바이스이엔지", "와이씨", "하나마이크론", "SFA반도체", 
    # "한양디지텍", "윈팩", "두산테스나", "에이팩트", "이수페타시스", "대덕전자", "심텍", "해성디에스", "티엘비", "리노공업", 
    # "ISC", "티에스이", "티에프이", "오킨스전자", "마이크로컨텍솔", "샘씨엔에스", "타이거일렉", "피엠티", "가온칩스", 
    # "에이직랜드", "오픈엣지테크놀로지", "코아시아", "에이디테크놀로지", "네오셈", "퀄리타스반도체", "티엘비", "엑시콘", 
    # "자람테크놀로지", "신성이엔지", "케이엔솔", "GST", "인성정보", "에치에프알", "RFHIC", "쏠리드", "HD현대중공업", 
    # "HD현대미포", "삼성중공업", "한화오션", "HJ중공업", "HD현대마린엔진", "STX엔진", "한화엔진", "한국카본", "동성화인텍", 
    # "한라IMS", "대양전기공업", "세진중공업", "인화정공", "롯데케미칼", "금호석유", "루닛", "뷰노", "HD현대일렉트릭", 
    # "일진전기", "제룡전기", "효성중공업", "LS ELECTRIC", "산일전기", "대한전선", "대원전선", "가온전선", "LS에코에너지", 
    # "LS마린솔루션", "두산에너빌리티", "비에이치아이", "시프트업", "삼성바이오로직스", "셀트리온", "SK바이오팜", "알테오젠", 
    # "바이넥스", "유한양행", "디앤디파마텍"
    # ]
    
    
    
    
    mypick_stock = ['가온전선']
    # 매주 금요일 기준일 생성
    week_end_dates = pd.date_range(start=start_day, end=end_day, freq="W-FRI").strftime("%Y%m%d")

    # 각 주차별 데이터 저장
    weekly_data = []
    for week_end in week_end_dates:
        df_week = Krx_daily_price.daily_price(week_end)
        df_week["기준일"] = week_end  # 기준일 컬럼 추가
        weekly_data.append(df_week)

    # 오늘 데이터 추가
    today_data = Krx_daily_price.daily_price(end_day)
    weekly_data.append(today_data)

    # 데이터프레임 결합
    df = pd.concat(weekly_data, axis=0)

    # 원하는 종목만 필터링
    df = df[df["종목명"].isin(mypick_stock)]

    # 정렬
    df = df.sort_values(by=["종목명", "기준일"], ascending=True)
    print(df)
    # 주차별 변동률 계산
    df["이전 시가총액"] = df.groupby("종목명")["시가총액"].shift(1)
    df["주차별 변동률"] = round(df.groupby("종목명")["시가총액"].pct_change(1) * 100, 1)

    # 피벗 테이블 생성 (종목별 + 주차별 변동률)
    df_pivot = df.pivot_table(index="종목명", columns="기준일", values="주차별 변동률")

    # YTD 계산 (첫 주 금요일 ~ 마지막 금요일 변동률)
    ytd_series = round(
        (df.groupby("종목명")["시가총액"].last() - df.groupby("종목명")["시가총액"].first()) 
        / df.groupby("종목명")["시가총액"].first() * 100, 1
    )

    # df_pivot과 ytd_series 인덱스를 맞춤
    ytd_series = ytd_series.reindex(df_pivot.index)

    # YTD 추가
    df_pivot["YTD"] = ytd_series

    # 최신 종가 선택 (중복 제거)
    closing_price_series = df.sort_values(by="기준일").drop_duplicates(subset="종목명", keep="last").set_index("종목명")["종가"]

    # 중복된 인덱스가 있는지 확인
    if df_pivot.index.duplicated().any():
        print("⚠️ 중복된 종목명 존재:", df_pivot.index[df_pivot.index.duplicated()])

    # reindex 실행 전에 중복 제거
    closing_price_series = closing_price_series[~closing_price_series.index.duplicated(keep="last")]

    # df_pivot 인덱스와 일치하도록 reindex
    closing_price_series = closing_price_series.reindex(df_pivot.index)

    # 종가 추가
    df_pivot["종가"] = closing_price_series

    # 원하는 컬럼 순서로 정렬 (YTD → 주차별 변동률 → 종가)
    columns_order = ["YTD"] + [col for col in df_pivot.columns if col not in ["YTD", "종가"]] + ["종가"]
    df_pivot = df_pivot[columns_order]

    # 클립보드 복사 (엑셀 붙여넣기 가능)
    df_pivot.to_clipboard()

    # 출력 확인
    print(df_pivot)
