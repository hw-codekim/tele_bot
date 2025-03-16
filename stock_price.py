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
    mypick_stock = [
    "하이브", "JYP Ent.", "에스엠", "와이지엔터테인먼트", "디어유", "인텔리안테크", "쎄트렉아이", "에이치브이엠", "루미르", "AP위성", "제노코", "컨텍",
    "비츠로테크", "클래시스", "에이피알", "원텍", "비올", "하이로닉", "리메드", "휴젤", "파마리서치", "대웅제약", "메디톡스", "바이오플러스",
    "휴메딕스", "한국비엔씨", "삼성전자", "SK하이닉스", "솔브레인", "한솔케미칼", "동진쎄미켐", "이엔에프테크놀로지", "티씨케이", "하나머티리얼즈",
    "원익QnC", "월덱스", "비씨엔씨", "케이엔제이", "에스앤에스텍", "에프에스티", "코미코", "한솔아이원스", "HPSP", "주성엔지니어링",
    "원익IPS", "유진테크", "케이씨텍", "피에스케이", "테스", "브이엠", "파크시스템스", "넥스틴", "오로스테크놀로지", "펨트론", "인텍플러스",
    "제우스", "와이씨", "하나마이크론", "SFA반도체", "한양디지텍", "두산테스나", "에이팩트", "이수페타시스", "대덕전자", "심텍", "해성디에스",
    "티엘비", "리노공업", "ISC", "티에스이", "티에프이", "오킨스전자", "샘씨엔에스", "가온칩스", "에이직랜드", "오픈엣지테크놀로지",
    "코아시아", "에이디테크놀로지", "SKC", "필옵틱스", "제이앤티씨", "켐트로닉스", "와이씨켐", "네오셈", "퀄리타스반도체", "티엘비",
    "엑시콘", "자람테크놀로지", "신성이엔지", "케이엔솔", "GST", "에치에프알", "RFHIC", "쏠리드", "HD현대중공업", "HD현대미포", "삼성중공업",
    "한화오션", "HJ중공업", "HD현대마린엔진", "STX엔진", "한화엔진", "한국카본", "동성화인텍", "한라IMS", "대양전기공업", "세진중공업",
    "인화정공", "현대힘스","한화에어로스페이스", "현대로템", "LIG넥스원", "한국항공우주", "한화시스템", "아이쓰리시스템", "엠앤씨솔루션", "롯데케미칼",
    "금호석유", "유니드","루닛", "뷰노", "쓰리빌리언", "온코크로스", "NAVER", "카카오", "더존비즈온", "엠로", "이스트소프트", "솔트룩스",
    "한글과컴퓨터", "카페24", "HD현대일렉트릭", "일진전기", "제룡전기", "효성중공업", "LS ELECTRIC", "산일전기", "대한전선", "대원전선",
    "가온전선", "LS에코에너지", "LS마린솔루션", "두산에너빌리티", "비에이치아이", "시프트업", "크래프톤", "삼성바이오로직스", "바이넥스",
    "에스티팜", "셀트리온", "SK바이오팜", "알테오젠", "유한양행", "디앤디파마텍", "현대엘리베이", "아난티", "브이티", "실리콘투",
    "한국콜마", "코스맥스", "아이패밀리에스씨", "아모레퍼시픽", "토니모리", "마녀공장", "현대차", "기아", "LG에너지솔루션", "삼성SDI",
    "SK이노베이션", "에코프로", "이수스페셜티케미컬", "레이크머티리얼즈", "한농화성", "넥스틸", "세아제강", "세아제강지주", "휴스틸",
    "태광", "하이록코리아", "성광벤드", "SK가스", "포스코인터내셔널", "현대오토에버", "에스오에스랩", "레인보우로보틱스",
    "두산로보틱스", "하이젠알앤엠", "로보티즈", "유일로보틱스", "씨메스", "클로봇", "알에스오토메이션", "뉴로메카", "삼현", "원익홀딩스",
    "대동기어", "전진건설로봇", "HD현대건설기계", "HD현대인프라코어", "SG", "TYM", "HD현대에너지솔루션", "한화솔루션", "SK이터닉스",
    "대명에너지", "HDC현대산업개발", "현대건설", "대우건설", "GS건설", "삼성E&A", "삼성물산", "HMM", "팬오션", "STX그린로지스",
    "삼양식품", "에스앤디", "농심", "빙그레", "풀무원", "롯데웰푸드", "오리온","세경하이테크","선익시스템"
    ]

    # mypick_stock = ['리메드','세경하이테크','JYP Ent.','와이지엔터테인먼트','글로벌텍스프리','휴스틸','넥스틸','현대힘스']
    
    # 매주 금요일 기준일 생성
    week_end_dates = pd.date_range(start=start_day, end=end_day, freq="W-FRI").strftime("%Y%m%d")

    # 각 주차별 데이터 저장
    weekly_data = []
    start_data = Krx_daily_price.daily_price(start_day)
    weekly_data.append(start_data)
    
    for week_end in week_end_dates:
        print(week_end)
        df_week = Krx_daily_price.daily_price(week_end)
        df_week["기준일"] = week_end  # 기준일 컬럼 추가
        weekly_data.append(df_week)
    print(weekly_data)
    
    
    # 금요일 조회시 주석처리 필요 오늘 데이터 추가
    today_data = Krx_daily_price.daily_price(end_day)
    weekly_data.append(today_data)

    # 데이터프레임 결합
    df = pd.concat(weekly_data, axis=0)
    print(df)
    # 원하는 종목만 필터링
    df = df[df["종목명"].isin(mypick_stock)]

    # 정렬
    df = df.sort_values(by=["종목명", "기준일"], ascending=True)
    
    # 주차별 변동률 계산
    df["이전 시가총액"] = df.groupby("종목명")["시가총액"].shift(1)
    print(df)
    df["주차별 변동률"] = round(df.groupby("종목명")["시가총액"].pct_change(1) * 100, 1)
    print(df)
    # 피벗 테이블 생성 (종목별 + 주차별 변동률)
    df_pivot = df.pivot_table(index="종목명", columns="기준일", values="주차별 변동률")
    print(df_pivot)
    ytd_series = round(
        (df.groupby("종목명")["시가총액"].last() - df.groupby("종목명")["시가총액"].first()) 
        / df.groupby("종목명")["시가총액"].first() * 100, 1
    )
    print(ytd_series)

    # df_pivot과 ytd_series 인덱스를 맞춤
    ytd_series = ytd_series.reindex(df_pivot.index)
    print(ytd_series)
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
