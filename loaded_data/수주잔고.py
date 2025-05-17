import pandas as pd

import OpenDartReader
import requests as rq
from bs4 import BeautifulSoup
from html_table_parser import parser_functions as parser
import re
from tqdm import tqdm
import time
import os
import requests
import json
import collections
import collections.abc
if not hasattr(collections, 'Callable'):
    collections.Callable = collections.abc.Callable

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36'}
api_key = '08d5ae18b24d9a11b7fd67fb0d79c607f1c88464'
dart = OpenDartReader(api_key)

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


def oder_backlog(code,name):

    #기간
    lists = dart.list(code, start='2022-01-01', end ='2025-05-30', kind='A') 
    lists['공시'] = lists['report_nm'].str.split(' ').str[1]
    #수준잔고 확인
    
    rcp_no = []
    for i in range(0,len(lists)):
        rcp = lists['rcept_no'][i]
        rcp_no.append(rcp)  
    print(rcp_no)    

    df = pd.DataFrame()
    comment = ['수주잔고','수주잔액']

    for j in tqdm(range(0,len(rcp_no))):

        files = dart.sub_docs(rcp_no[j])
        
        content = [] #
        content_old = []
        if '매출' in files['title']:
            print('1')
            data = files[files['title'].str.contains('매출')]

            try:
                a = data['url'].values[0]
                res = rq.get(a,headers=headers)
                time.sleep(0.5)
                soup=BeautifulSoup(res.text,'html.parser')
                temp = soup.select('body > table')
                
                for v in temp:
                    text = v.get_text()
                    if ('수주잔고' in text) or ('수주잔액' in text) : 
                        content.append(v)
                
                table = parser.make2d(content[0]) #표로 만들어줌
                adata = pd.DataFrame(table) # 필요한부분만 불러옴
                adata.columns = adata.iloc[0]
                adata = adata.iloc[-1:,:]
                adata = adata.dropna(axis=1, how="all")
                adata = adata.iloc[:,-1:]
                adata.columns=['수주잔고']
                adata.insert(0,'날짜',lists['공시'][j])
                adata.insert(0,'기업명',name)
                print(adata)
                df = pd.concat([df,adata])
                time.sleep(2)
            
                
            except:
                print('넘어가자')
                    
        else:     
            print('2')  
            data_old = files[files['title'].str.contains('사업의 내용')]

            try:
                b = data_old['url'].values[0]
                res_old = rq.get(b,headers=headers)
                soup_old=BeautifulSoup(res_old.text,'html.parser')
                tem = soup_old.select('body > table')

                for k in tem:
                    text = k.get_text().strip()

                    if ('수주잔고' in text) or ('수주잔액' in text) : 
                        content_old.append(k)
                
                atable = parser.make2d(content_old[0]) #표로 만들어줌
                adata = pd.DataFrame(atable) # 필요한부분만 불러옴
                adata.columns = adata.iloc[0]
            
                adata = adata.iloc[-1:,:]
                adata = adata.dropna(axis=1, how="all")
                adata = adata.iloc[:,-1:]
                adata.columns=['수주잔고']
                adata.insert(0,'날짜',lists['공시'][j])
                adata.insert(0,'기업명',name)
                df = pd.concat([df,adata])
                time.sleep(2)
            except Exception as e:
                print(f'{e}')
    # df.to_clipboard()
    return df


def pivot_backlog(df):
    df_pivot = df.pivot(index='기업명', columns='날짜', values='수주잔고').fillna(0)
    df_pivot.reset_index(inplace=True)
    return df_pivot

def save_excel(df, filename='./수주잔고/수준잔고.xlsx'):
    # 폴더 없으면 생성
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    if os.path.exists(filename):
        # 기존 파일 있으면 불러와서 합침
        existing_df = pd.read_excel(filename)
        # 합칠 때, 기업명 기준으로 중복 제거하며 병합
        combined_df = pd.concat([existing_df, df]).drop_duplicates(subset=['기업명'], keep='last')
        combined_df.to_excel(filename, index=False)
    else:
        df.to_excel(filename, index=False)
    print(f"{filename} 저장 완료!")

if __name__ == '__main__':
    corps = ['산일전기']

    all_data = pd.DataFrame()

    for corp in corps:
        code = company_code(corp)
        print(f'{corp} 조회 시작')
        df_raw = oder_backlog(code, corp)
        all_data = pd.concat([all_data, df_raw])
        # 피벗 적용
        df_pivot = pivot_backlog(all_data)

        # 엑셀 저장
        save_excel(df_pivot)