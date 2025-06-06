import OpenDartReader
import pandas as pd
from datetime import datetime
import time
import warnings
import requests
import json

warnings.simplefilter("ignore")
api_key = '08d5ae18b24d9a11b7fd67fb0d79c607f1c88464'
dart = OpenDartReader(api_key)
today = datetime.today().strftime("%Y%m%d") #오늘

# 실적 발표 시 DART의 보고서를 참조해서 분기별로 보여 줌. 4분기는 연간으로 보여지니 엑셀에서 추가 작업 필요.
#code : 종목코드
#corp : 종목이름
#fs_div : 분기 1(11013),2(11012),3(11014),4(11011)
#reprt_code : 연결, 개별 ( OFC, CFS)

def company_code(corp_name):
    # 전 종목 가져오기
    url = 'https://comp.fnguide.com/XML/Market/CompanyList.txt'
    res = requests.get(url)
    res.encoding = 'utf-8-sig'
    corp_list = json.loads(res.text)['Co']
    
    # 회사 이름으로 종목 코드 찾기
    for item in corp_list:
        name = item['nm']
        code = item['cd'][1:]  # 'A005930' → '005930'
        if name == corp_name:
            return code
    
    return None

def quater_fin(code,corp,year,reprt_code,fs_div='CFS'):
    # try:
    df = dart.finstate_all(code, year,reprt_code=reprt_code,fs_div=fs_div)
    
    df = df[df['sj_nm'].str.contains('손익')]
    
    df = df[df['account_id'].str.contains('ifrs-full_Revenue')|df['account_id'].str.contains('OperatingIncomeLoss')|df['account_nm'].str.contains('영업수익')]

    df = df[['account_nm','thstrm_amount']]
    
    if reprt_code == '11014': # 3분기
        df.columns = ['항목',f'{year}.3Q']
        df = df.apply(pd.to_numeric,errors = 'ignore')
        df = df.sort_values(by='항목',ascending=True)
    
        df['항목'].iloc[0] = '매출액'
        df['항목'].iloc[1] = '영업이익'
        df.insert(0,'기업',corp)
        df = df.set_index(['기업','항목'])
        df = df.apply(lambda x : round(x/100000000,0))
        
        sales = df.loc[(corp, '매출액'), f'{year}.3Q']
        op = df.loc[(corp, '영업이익'), f'{year}.3Q']
        opm = (op / sales) * 100  # 백분율로 변환
        df.loc[(corp, 'OPM'), f'{year}.3Q'] = opm
        df = df.apply(lambda x : round(x,1))  
        
        
    elif reprt_code == '11012': #반기
        df.columns = ['항목',f'{year}.2Q']
        df = df.apply(pd.to_numeric,errors = 'ignore')
        df = df.sort_values(by='항목',ascending=True)
        df['항목'].iloc[0] = '매출액'
        df['항목'].iloc[1] = '영업이익'
        df.insert(0,'기업',corp)
        df = df.set_index(['기업','항목'])
        df = df.apply(lambda x : round(x/100000000,0))   
        
        sales = df.loc[(corp, '매출액'), f'{year}.2Q']
        op = df.loc[(corp, '영업이익'), f'{year}.2Q']
        opm = (op / sales) * 100  # 백분율로 변환
        df.loc[(corp, 'OPM'), f'{year}.2Q'] = opm
        df = df.apply(lambda x : round(x,1))  
         
         
    elif reprt_code == '11013': # 1분기
        df.columns = ['항목',f'{year}.1Q']
        df = df.apply(pd.to_numeric,errors = 'ignore')
        df = df.sort_values(by='항목',ascending=True)
        df['항목'].iloc[0] = '매출액'
        df['항목'].iloc[1] = '영업이익'
        df.insert(0,'기업',corp)
        df = df.set_index(['기업','항목'])
        df = df.apply(lambda x : round(x/100000000,1))  
        
        sales = df.loc[(corp, '매출액'), f'{year}.1Q']
        op = df.loc[(corp, '영업이익'), f'{year}.1Q']
        opm = (op / sales) * 100  # 백분율로 변환
        df.loc[(corp, 'OPM'), f'{year}.1Q'] = opm
        df = df.apply(lambda x : round(x,1))  
       
       
    elif reprt_code == '11011': # 사업보고서
        df.columns = ['항목',f'{year}.4Q']
        df = df.apply(pd.to_numeric,errors = 'ignore')
        df = df.sort_values(by='항목',ascending=True)
        df['항목'].iloc[0] = '매출액'
        df['항목'].iloc[1] = '영업이익'
        df.insert(0,'기업',corp)
        df = df.set_index(['기업','항목'])
        df = df.apply(lambda x : round(x/100000000,0))    

        sales = df.loc[(corp, '매출액'), f'{year}.4Q']
        op = df.loc[(corp, '영업이익'), f'{year}.4Q']
        opm = (op / sales) * 100  # 백분율로 변환
        df.loc[(corp, 'OPM'), f'{year}.4Q'] = opm
        df = df.apply(lambda x : round(x,1))  

    # if fs_div == 'CFS':
    #     df.insert(0,'구분','연결')
        
    # else: 
    #     df.insert(0,'구분','별도')
    # except Exception as e:
    #     print(corp,e)
    time.sleep(2)
    return df

def year_finance(result_df):
    year_df = result_df[result_df.columns.str.contains('4Q')]
    return year_df
    
def get_all_quarter_data(code, corp):

    years = ['2021','2022', '2023','2024', '2025']
    # years = ['2025']
    reprt_codes = ['11013', '11012', '11014', '11011']  # 1Q, 반기, 3Q, 사업보고서

    result_df = pd.DataFrame()

    for year in years:
        print(f'{year} 조회')
        for reprt_code in reprt_codes:
            for fs_div in ['CFS', 'OFS']:  # 연결 먼저, 없으면 개별
                try:
                    df = quater_fin(code, corp, year, reprt_code, fs_div)
                    result_df = pd.concat([result_df, df], axis=1)
                    break  # 성공했으면 다음으로 넘어감
                except Exception as e:
                    if fs_div == 'OFS':
                        print(f"[{corp}][{year}][{reprt_code}] {fs_div} 실패: {e}")
                    continue

    return result_df

def extract_pure_4q(df):
    # 연도 추출 (예: '2023.4Q' → 2023)
    years = [col[:4] for col in df.columns if '.4Q' in col]
    years = sorted(set(years))

    for year in years:
        try:
            col_1q = f'{year}.1Q'
            col_2q = f'{year}.2Q'
            col_3q = f'{year}.3Q'
            col_4q = f'{year}.4Q'

            if all(col in df.columns for col in [col_1q, col_2q, col_3q, col_4q]):
                for metric in ['매출액', '영업이익']:
                    try:
                        value_4q = (
                            df.loc[(corp, metric), col_4q]
                            - df.loc[(corp, metric), col_1q]
                            - df.loc[(corp, metric), col_2q]
                            - df.loc[(corp, metric), col_3q]
                        )
                        df.loc[(corp, metric), col_4q] = round(value_4q, 1)
                    except:
                        pass

                # OPM(영업이익률) 재계산
                try:
                    sales = df.loc[(corp, '매출액'), col_4q]
                    op = df.loc[(corp, '영업이익'), col_4q]
                    opm = round((op / sales) * 100, 1) if sales != 0 else 0
                    df.loc[(corp, 'OPM'), col_4q] = opm
                except:
                    pass
        except Exception as e:
            print(f"{year} 연도 계산 오류: {e}")
    return df  # ✅ 결과를 반환해야 함

def add_growth_metrics(df, corp):
    cols = df.columns
    metrics = ['매출액', '영업이익']
    
    def calc_change(curr, prev):
        try:
            if pd.isna(curr) or pd.isna(prev):
                return None
            curr = float(curr)
            prev = float(prev)

            if prev < 0 and curr > 0:
                return '흑전'
            elif prev > 0 and curr < 0:
                return '적전'
            elif prev == 0:
                return None
            else:
                return f"{((curr - prev) / abs(prev)) * 100:.1f}"
        except:
            return None

    for metric in metrics:
        yoy_values = []
        qoq_values = []

        for i in range(len(cols)):
            col = cols[i]
            year, quarter = col.split('.')

            # YoY: 전년도 동일 분기
            prev_year = str(int(year) - 1)
            prev_col = f'{prev_year}.{quarter}'
            if prev_col in df.columns:
                prev_val = df.loc[(corp, metric), prev_col]
                curr_val = df.loc[(corp, metric), col]
                yoy = calc_change(curr_val, prev_val)
            else:
                yoy = None
            yoy_values.append(yoy)

            # QoQ: 전분기
            if quarter == '1Q':
                prev_q_col = f'{str(int(year)-1)}.4Q'
            elif quarter == '2Q':
                prev_q_col = f'{year}.1Q'
            elif quarter == '3Q':
                prev_q_col = f'{year}.2Q'
            elif quarter == '4Q':
                prev_q_col = f'{year}.3Q'
            else:
                prev_q_col = None

            if prev_q_col in df.columns:
                prev_val = df.loc[(corp, metric), prev_q_col]
                curr_val = df.loc[(corp, metric), col]
                qoq = calc_change(curr_val, prev_val)
            else:
                qoq = None
            qoq_values.append(qoq)

        df.loc[(corp, f'{metric} YoY'), cols] = yoy_values
        df.loc[(corp, f'{metric} QoQ'), cols] = qoq_values
        
    return df


#fs_div : 분기 1(11013),2(11012),3(11014),4(11011)
if __name__ == '__main__'  :
    # 조회하고 싶은 종목 code 와 corp 변경
    # code = '200130' 
    
    dd=pd.DataFrame()
    
    corps = [
    "삼성전자", "SK하이닉스", "DB하이텍", "솔브레인", "한솔케미칼", "동진쎄미켐", "이엔에프테크놀로지", "티씨케이", "하나머티리얼즈", "원익QnC",
    "월덱스", "비씨엔씨", "케이엔제이", "에스앤에스텍", "에프에스티", "코미코", "한솔아이원스", "HPSP", "주성엔지니어링", "원익IPS",
    "유진테크", "케이씨텍", "피에스케이", "테스", "브이엠", "파크시스템스", "넥스틴", "오로스테크놀로지", "펨트론", "인텍플러스",
    "제우스", "와이씨", "하나마이크론", "SFA반도체", "한양디지텍", "두산테스나", "에이팩트", "이수페타시스", "대덕전자", "심텍",
    "해성디에스", "티엘비", "리노공업", "ISC", "티에스이", "티에프이", "오킨스전자", "마이크로컨텍솔", "샘씨엔에스", "가온칩스",
    "에이직랜드", "오픈엣지테크놀로지", "코아시아", "에이디테크놀로지", "SKC", "필옵틱스", "제이앤티씨", "켐트로닉스", "와이씨켐", "네오셈",
    "퀄리타스반도체", "엑시콘", "자람테크놀로지", "신성이엔지", "케이엔솔", "GST"
    ]
    
    for corp in corps:
        code = company_code(corp)
        print(f'{corp}의 코드는 {code}')
        dff = get_all_quarter_data(code, corp)
        rst =  extract_pure_4q(dff)
        # ab = add_growth_metrics(rst,corp)
        dd = pd.concat([dd,rst])

    dd.reset_index(drop=False,inplace=True)
    dd.sort_values('항목',ascending=False,inplace=True)

    dd.to_excel(f'실적_반도체.xlsx',index=False)    