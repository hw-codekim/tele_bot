import requests as rq
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from biz_day import Bizday
import warnings
import json
import os
import numpy as np
import time
warnings.filterwarnings("ignore")

# http://comp.fnguide.com/ 요약리포트 가져와서 엑셀에 저장

class Report:
    def whynot_report(biz_day):

        today_data = []

        url = f'https://www.whynotsellreport.com/api/reports/from/20240102/to/{biz_day}'
        html = rq.get(url)
        # 필요한 필드만 추출하여 리스트로 만들기
        data = html.json()
        
        extracted_data = []
        for report in data:
            extracted_data.append({
                'id': report['id'],
                'date': report['date'],
                'company_name': report['company_name'],
                'analyst_name': report['analyst_name'],
                'price': report['price'],
                'judge': report['judge'],
                'title': report['title'],
                'description': report['description'],
                'analyst_rank': report['analyst_rank'],
                'stock_code_id': report['stock_code_id'],
                'analyst_id': report['analyst_id'],
            })

    # 데이터프레임 생성
        df = pd.DataFrame(extracted_data)
        df['date'] = pd.to_datetime(df['date'])
        df['date'] = df['date'].dt.strftime('%Y%m%d')
        df = df.rename(columns={'price':'target_price'})
        # df=df[df['company_name'] == '테크윙']
        # print(f'{biz_day} [whynot_report] {len(df)}개 로딩 성공')
        time.sleep(1)
        

        df.sort_values(["company_name", "analyst_name", "date"], ascending=[True, True, False], inplace=True)

        # 이전 목표가 계산
        df["prevision_target_price"] = df.groupby(["company_name", "analyst_name"])["target_price"].shift(-1)

        # 현재 목표가 이름 변경
        df.rename(columns={"target_price": "current_target_price"}, inplace=True)

        # 상승률 계산
        df["rate_of_increase"] = ((df["current_target_price"] - df["prevision_target_price"]) / df["prevision_target_price"] * 100).round(2)
        df["rate_of_increase"] = df["rate_of_increase"].replace([np.inf, -np.inf], np.nan)
        # 오늘(1/31) 보고서만 필터링
        today_df = df[(df["date"] == biz_day) & (df["rate_of_increase"] > 0)].dropna(subset=["prevision_target_price", "rate_of_increase"])
        # print(today_df)
        for _, row in today_df.iterrows():
            today_data.append({
                "기업명": row['company_name'],
                "애널리스트": row['analyst_name'],
                "현재 목표가": f"{int(row['current_target_price']):,}원",
                "이전 목표가": f"{int(row['prevision_target_price']):,}원",
                "상승률": f"{row['rate_of_increase']}%",
                "제목": row['title'],
                "내용": row['description']
            })
        return today_data
    
    def get_company_report(biz_day,name):

        url = f'https://www.whynotsellreport.com/api/reports/from/20240102/to/{biz_day}'
        html = rq.get(url)
        # 필요한 필드만 추출하여 리스트로 만들기
        data = html.json()
        
        extracted_data = []
        for report in data:
            extracted_data.append({
                'id': report['id'],
                'date': report['date'],
                'company_name': report['company_name'],
                'analyst_name': report['analyst_name'],
                'price': report['price'],
                'judge': report['judge'],
                'title': report['title'],
                'description': report['description'],
                'analyst_rank': report['analyst_rank'],
                'stock_code_id': report['stock_code_id'],
                'analyst_id': report['analyst_id'],
            })

    # 데이터프레임 생성
        df = pd.DataFrame(extracted_data)
        df['date'] = pd.to_datetime(df['date'])
        df['date'] = df['date'].dt.strftime('%Y%m%d')
        df = df.rename(columns={'price':'target_price'})
        df=df[df['company_name'] == name]
        time.sleep(1)
        return df

    def save(df): # 엑셀 저장
        df = df.sort_values(by=['company_name','date'],ascending=True)
        try:
            if not os.path.exists('레포트.csv'):
                df.to_csv('레포트.csv', mode='w', encoding='utf-8-sig',index=False)
            else:
                df.to_csv('레포트.csv', mode='a', encoding='utf-8-sig', header=False,index=False)
        except Exception as e:
            print(e)
        finally:    
            print(f"레포트 파일이 저장되었습니다")

if __name__ == '__main__':
    day = Bizday.biz_day()
    name = '글로벌텍스프리'
    df = Report.whynot_report(day)
    ddf = Report.get_company_report(day,name)
    ddf.to_clipboard()
