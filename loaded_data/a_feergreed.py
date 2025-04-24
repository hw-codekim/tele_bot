import fear_and_greed
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import os

# 오늘 값 가져오기
data_today = fear_and_greed.get()
today = datetime.utcnow().strftime('%Y-%m-%d')
today_value = data_today.value

# GitHub에서 과거 데이터 불러오기 (1년치)
url = "https://raw.githubusercontent.com/whit3rabbit/fear-greed-data/main/fear_greed.csv"
df_past = pd.read_csv(url)

# 형식 맞추기
df_past = df_past.rename(columns={"date": "date", "fgi": "value"})
df_past["date"] = pd.to_datetime(df_past["date"])
df_today = pd.DataFrame([{"date": pd.to_datetime(today), "value": today_value}])

# 과거 + 오늘 데이터 병합 (중복 제거)
df_total = pd.concat([df_past, df_today])
df_total = df_total.drop_duplicates(subset="date").sort_values("date")

# 시각화
plt.figure(figsize=(12, 6))
plt.plot(df_total["date"], df_total["value"], marker='o', linestyle='-', color='green')
plt.title("Fear & Greed Index (과거 + 오늘 포함)")
plt.xlabel("날짜")
plt.ylabel("지수 값")
plt.grid(True)
plt.tight_layout()
plt.show()