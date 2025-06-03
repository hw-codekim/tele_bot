import OpenDartReader
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
from html_table_parser import parser_functions as parser

import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import collections
import collections.abc
if not hasattr(collections, 'Callable'):
    collections.Callable = collections.abc.Callable

api_key = '08d5ae18b24d9a11b7fd67fb0d79c607f1c88464'
dart = OpenDartReader(api_key)
# today = (datetime.today()-timedelta(days=1)).strftime("%Y%m%d") #오늘
today = datetime.today().strftime("%Y%m%d") #오늘
s_today = '20250530'
e_today = '20250602'

dart_list = dart.list(start=s_today,end=e_today)

lists = dart_list[dart_list['report_nm'].str.contains('공급계약체결')&~dart_list['report_nm'].str.contains('기재정정')]
print(f'{e_today}_{len(lists)}개 조회')
def supply(rcp_no,corp):
    
    
    options = webdriver.ChromeOptions()
    options.add_argument("headless")
    driver = webdriver.Chrome(options=options)
    driver.get(f'https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcp_no}')
    driver.switch_to.frame('ifrm')
    html = driver.page_source #지금 현 상태의 page source불러오기
    soup = BeautifulSoup(html,'html.parser')
    tables = soup.find_all('table')#표를 불러옴
    p=parser.make2d(tables[0])#표를 만듬
    tt=pd.DataFrame(p)
    tt = tt[[0,1,2]]
    tt[2] = tt.apply(lambda row: row[1] if pd.isna(row[2]) else row[2], axis=1)
    tt = tt[[1,2]]
    tt = tt.T
    
    if '내용' in tt.iloc[0,0]: # 공급계약은 두가지 타입으로 존재
        tt = tt[[20,0,2,5,6,7,12,13,14,21,20]]
        
        tt.columns = ['계약일자','계약내용','계약금액(원)','최근매출액(원)','매출액대비(%)','계약상대','판매공급지역','시작일','종료일','유보사유','유보기한']
        tt = tt.drop(tt.index[0]).reset_index()
        tt = tt.drop(columns='index')
    else:
        tt = tt[[13,1,2,3,4,6,8,9,10,14,15]]
        # print(tt)
        tt.columns = ['계약일자','계약내용','계약금액(원)','최근매출액(원)','매출액대비(%)','계약상대','판매공급지역','시작일','종료일','유보사유','유보기한']
        tt = tt.drop(tt.index[0]).reset_index()

        tt = tt.drop(columns='index')
    tt.insert(0,'기업',corp)
    time.sleep(1)
    return tt


df = pd.DataFrame()

for i in range(len(lists)):
    try:
        rcp_no = lists['rcept_no'].values[i]
        corp = lists['flr_nm'].values[i]
        supply_df = supply(rcp_no,corp)
        print(f'{i}_{corp} 완료')  
        print(supply_df)
        df = pd.concat([df,supply_df])
    except Exception as e:
        print(f'{corp}',e)
        
file_name = f'./supply_data/공급계약_종합.xlsx'
writer = pd.ExcelWriter(file_name, mode='a', engine='openpyxl', if_sheet_exists='overlay')
df.to_excel(
    writer, 
    sheet_name='Sheet1',
    startcol = 0,
    startrow = writer.sheets['Sheet1'].max_row,
    index=False, 
    na_rep = '',      # 결측값을 ''으로 채우기
    inf_rep = '',     # 무한값을 ''으로 채우기
    header = None
    )
writer.close()