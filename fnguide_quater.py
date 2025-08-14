import requests
import pandas as pd
import time
from datetime import datetime
import os

today = datetime.today().strftime("%Y%m%d") #오늘

def finance(year,month):
    print(year+month)
    url = f'https://comp.fnguide.com/SVO2/common/sp_read_json_cache.asp?cmdText=menu_9_1&IN_gs_ym={year+month}&IN_gs_gb=N&IN_report_gb=X&IN_gb=D&IN_SRC_GB=SVO&_=1733835322785'
    res = requests.get(url)
    data = res.json()
    data = data['data']

    df = pd.DataFrame(data)
    df = df[(df['DA_GB'] == '잠정') | (df['DA_GB'] == '확정')] # 확정 또는 잠정으로 
    df['GICODE'] = df['GICODE'].str.replace('A','')
    df.insert(2,'년도',year)
    df['분기'] = df['년도']+df['GS_GB']
    df = df[['GICODE', 'ITEMNM','ISSUE','분기','REP_GB', 'SALES', 'OPER', 'NET','DA_GB','SALES2','OPER2','NET2']]
    df.columns = ['코드','종목명','켄센','분기','구분','매출','영업이익','순이익','발표','매출YoY','영익YoY','순이익YoY']
    time.sleep(3)
    print(f'{len(df)}개')
    print(df)
    return df

def save(final,today):
        folder_path = '실적'
        os.makedirs(folder_path, exist_ok=True)

        file_path = os.path.join(folder_path, f"./분기실적/분기실적_{today}.xlsx")

        try:
            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                final.to_excel(writer, sheet_name='Sheet1', index=False)
            
            print("엑셀 저장완료!")
        except Exception as e:
            print("엑셀 저장 오류:", e)
        
        
        
# 기존 전체 실적 확인 할때       
# years = ['2019','2020','2021','2022','2023','2024','2025']
# months = ['01','02','03','04','05','06','07','08','09','10','11','12']

# 원하는 연도와 월을 선택
years = ['2025']
months = ['07']

f_df = pd.DataFrame()
for year in years:
    for month in months:
    
        try:
            finance_df = finance(year,month)
            
            # save(finance_df)
            f_df = pd.concat([f_df,finance_df])
            # print(len(f_df))
        except Exception as e:
            print(year+month,e)
f_df = f_df.drop_duplicates(subset=['코드', '종목명', '분기', '구분'])

save(f_df,today)
# print(a)