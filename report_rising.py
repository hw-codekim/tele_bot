import requests as rq
import pandas as pd
from datetime import datetime
import numpy as np
import warnings
from datetime import datetime
warnings.filterwarnings("ignore")


class Report:

    @staticmethod
    def whynot_report_all_rising(biz_day):
        url = f'https://www.whynotsellreport.com/api/reports/from/20250101/to/{biz_day}'
        html = rq.get(url)
        data = html.json()

        # 데이터 추출
        extracted_data = []
        for report in data:
            extracted_data.append({
                'date': report['date'],
                'company_name': report['company_name'],
                'analyst_name': report['analyst_name'],
                'price': report['price'],
            })

        df = pd.DataFrame(extracted_data)
        df['date'] = pd.to_datetime(df['date'])

        # 날짜 내림차순 정렬
        df.sort_values(["company_name", "analyst_name", "date"], ascending=[True, True, False], inplace=True)

        # 이전 목표가 계산
        df["prev_price"] = df.groupby(["company_name", "analyst_name"])["price"].shift(-1)

        # 상승률 계산
        df["rate_of_increase"] = ((df["price"] - df["prev_price"]) / df["prev_price"] * 100).round(2)
        df["rate_of_increase"] = df["rate_of_increase"].replace([np.inf, -np.inf], np.nan)

        # 조건 1: 이전 목표가 < 현재 목표가
        cond1 = (df["prev_price"] < df["price"])

        # 조건 2: 이전 목표가가 0 또는 결측인데 현재 목표가가 0 이상
        cond2 = ((df["prev_price"].isna()) | (df["prev_price"] == 0)) & (df["price"] > 0)

        # 조건을 만족하는 모든 경우 필터링
        rising_df = df[cond1 | cond2].copy()

        # 보기 좋게 정리
        rising_df["현재 목표가"] = rising_df["price"].astype(int).map(lambda x: f"{x:,}원")
        rising_df["이전 목표가"] = rising_df["prev_price"].fillna(0).astype(int).map(lambda x: f"{x:,}원")
        rising_df["상승률"] = rising_df["rate_of_increase"].map(lambda x: f"{x:.2f}%" if pd.notna(x) else "-")
        rising_df["날짜"] = rising_df["date"].dt.strftime('%Y-%m-%d')

        final_df = rising_df[[
            "날짜", "company_name", "analyst_name", "이전 목표가", "현재 목표가", "상승률"
        ]].rename(columns={
            "company_name": "기업명",
            "analyst_name": "애널리스트"
        })

        return final_df.reset_index(drop=True)


# 사용 예시
if __name__ == "__main__":
    biz_day = datetime.today().strftime('%Y%m%d')
    df = Report.whynot_report_all_rising(biz_day)

    # 전체 출력
    print(df.to_string(index=False))

    # 엑셀 저장
    df.to_excel(f"목표가상승_전체리포트_{biz_day}.xlsx", index=False)
