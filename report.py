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
import matplotlib.pyplot as plt
warnings.filterwarnings("ignore")

# http://comp.fnguide.com/ 요약리포트 가져와서 엑셀에 저장

class Report:
    def whynot_report(biz_day):

        today_data = []

        url = f'https://www.whynotsellreport.com/api/reports/from/20250101/to/{biz_day}'
        html = rq.get(url)
        # 필요한 필드만 추출하여 리스트로 만들기
        data = html.json()
        print(data)
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
        # today_df = df[(df["date"] == biz_day) & (df["rate_of_increase"] > 0)].dropna(subset=["prevision_target_price", "rate_of_increase"])
        today_df = df[(df["date"] == biz_day)].dropna(subset=["prevision_target_price", "rate_of_increase"]) # rate_of_increase 가 0 보다 큰 것만 추출하는걸 뺐음. 전부다 표기
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

        url = f'https://www.whynotsellreport.com/api/reports/from/20250101/to/{biz_day}'
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

    def graph(df,corp_name):
        df.sort_values(by='날짜',ascending=True,inplace=True)
        plt.figure(figsize=(8, 4))
        plt.plot(df['날짜'], df['목표가'], marker='o', linestyle='-', color='b',markersize=4,linewidth=1)

        # 제목, 축 레이블 추가
        plt.title(F'[{corp_name}]_Report_Target Price')

        # X축 날짜 포맷 조정
        plt.xticks(rotation=90)

        # 그리드 추가 (점선, 연한 회색)
        plt.grid(True, linestyle=':', color='lightgray', alpha=0.7)
        
        # tight_layout의 pad 값을 설정해 여백 조정
        # plt.tight_layout(pad=10.0)  # pad 값을 통해 위쪽 여백 조
        
        # 그래프 출력
        plt.tight_layout()
        plt.savefig(F'./레포트트렌드/{corp_name}.png', dpi=100)  # 용량을 줄이기 위해 dpi 낮추기
        plt.show()
if __name__ == '__main__':
    day = Bizday.biz_day()
    name = '원텍'
    
    names = ['티엘비']
    
    rst_df = pd.DataFrame()
    for name in names:
        df = Report.whynot_report(day)
        ddf = Report.get_company_report(day,name)
        rst_df = pd.concat([rst_df,ddf])
        rst_df = rst_df[['date','company_name','analyst_name','target_price','judge','title','description']]
        print(f'{name} 완료')
    # Report.graph(ddf,name)
    rst_df.to_clipboard()
