import requests
import json
import pandas as pd
import os
import re
from tqdm import tqdm
import time
import random

def finance_year_consen(code, name, gubun='D'):
    url = f'https://comp.fnguide.com/SVO2/json/data/01_06/01_A{code}_A_{gubun}.json'
    res = requests.get(url)
    temp = json.loads(res.content)
    df = pd.DataFrame(temp['comp'])

    # 필요한 컬럼만 선택
    df = df[['ACCOUNT_NM', 'D_2', 'D_3', 'D_4', 'D_5', 'D_6', 'D_7']]
    
    # 컬럼명 정리
    df.columns = df.iloc[0]  # 첫 번째 행을 컬럼명으로 사용
    df = df.drop(index=0)  # 첫 번째 행 삭제

    # 기업명과 구분 추가
    df.insert(0, '기업', name)
    df.insert(1, '구분', '연결' if gubun == 'D' else '별도')

    # 컬럼명 통일: `/12(P)` → `/12`
    new_columns = []
    for col in df.columns:
        if re.search(r'/12\(P\)', str(col)):  # `2024/12(P)` 같은 경우
            new_columns.append(re.sub(r'\(P\)', '', col))
        else:
            new_columns.append(col)
    df.columns = new_columns

    # 중복 컬럼 처리 (`2024/12(P)` → `2024/12`로 합쳐진 경우, 첫 번째 값 유지)
    df = df.loc[:, ~df.columns.duplicated(keep='first')]

    # 컬럼 필터링: "/12"가 포함된 컬럼만 유지
    keep_columns = ['기업', '구분', '항목'] + [col for col in df.columns if re.search(r'/12', str(col))]
    df = df[keep_columns]
    # time.sleep(random.uniform(0.5, 1.0))
    return df[:6]

def save(df, filename='종목별_컨센서스.csv'):  # 엑셀 저장 함수
    try:
        if not os.path.exists(filename):
            df.to_csv(filename, mode='w', encoding='utf-8-sig', index=False)
        else:
            df.to_csv(filename, mode='a', encoding='utf-8-sig', header=False, index=False)
    except Exception as e:
        print(e)
    finally:    
        print(f"{filename} 파일이 저장되었습니다")

if __name__ == '__main__':
    url = 'https://comp.fnguide.com/XML/Market/CompanyList.txt'
    res = requests.get(url)
    temp = json.loads(res.content)
    filtered_co = [item for item in temp['Co'] if item.get("gb") == "701" and "스팩" not in item.get("nm", "")]

    ttl_df = pd.DataFrame()
    all_columns = set()  # 컬럼 목록 저장

    for i in tqdm(range(len(filtered_co))):  # 우선 10개만 조회
        try:
            cd = temp['Co'][i]['cd'][1:]
            nm = temp['Co'][i]['nm']
            df = finance_year_consen(cd, nm)
            
            all_columns.update(df.columns)  # 컬럼 목록 저장
            ttl_df = pd.concat([ttl_df, df])  # 데이터 합치기
        except Exception as e:
            print(f'{cd}_{nm}', e)


    save(ttl_df)
    




