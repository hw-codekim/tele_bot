import requests
from bs4 import BeautifulSoup
import asyncio
from telegram import Bot
import pandas as pd
import numpy as np
from report import Report
from biz_day import Bizday
import json

with open('bot_key.json', 'r') as file:
    data = json.load(file)
    
BOT_TOKEN = data['BOT_TOKEN']
CHAT_ID = data['CHAT_ID']

# Bot 객체 생성
bot = Bot(token=BOT_TOKEN)


# 네이버 뉴스에서 제목과 링크 크롤링

def naver_news(content):
    data = []
    for i in range(3):
        response = requests.get(f"https://search.naver.com/search.naver?where=news&sm=tab_jum&query={content}&start={i}1")
        html = response.text
        soup = BeautifulSoup(html, 'html.parser')
        articles = soup.select(".list_news > li")

        for article in articles:
            title = article.select_one(".news_tit").text
            link = article.select_one(".news_tit").attrs['href']
            data.append(f"{title}\n{link}")
    return data 



# 텔레그램으로 뉴스 보내기
async def send_news_via_telegram(news_list):
    for news in news_list:
        await bot.send_message(chat_id=CHAT_ID, text=news)

async def send_report_telegram(report):
    all_reports = ""
    for entry in report:
        all_reports += (
            f"📌 기업명 : {entry['기업명']}\n" +
            f"*애널리스트 : {entry['애널리스트']}\n" +
            f"*현재 목표가 : {entry['현재 목표가']}\n" +
            f"*이전 목표가 : {entry['이전 목표가']}\n" +
            f"*상승률 : {entry['상승률']}\n" +
            f"*제목 : {entry['제목']}\n" +
            f"*내용 : {entry['내용']}\n" +
            "-" * 50 + "\n"
        )
     # 메시지가 4096자를 초과하면 분할하여 전송
    max_length = 4096
    while len(all_reports) > max_length:
        # 4096자를 넘어가는 시점에서 마지막 '\n'을 기준으로 메시지를 나눠보자
        split_point = all_reports.rfind("-" * 50, 0, max_length)
        if split_point == -1:  # 만약 \n이 없다면 그냥 최대 길이로 잘라
            split_point = max_length

        # 잘라서 보내기
        await bot.send_message(chat_id=CHAT_ID, text=all_reports[:split_point])
        all_reports = all_reports[split_point:].lstrip()  # 남은 부분 처리

    # 남은 메시지 전송
    if all_reports:
        await bot.send_message(chat_id=CHAT_ID, text=all_reports)

    # await bot.send_message(chat_id=CHAT_ID, text=all_reports)

# 메인 함수
async def main():

    # news_list = naver_news('스타게이트')  # 뉴스 크롤링
    day = Bizday.biz_day()
    report = Report.whynot_report(day)
    
    await send_report_telegram(report)  # 텔레그램으로 전송

# 비동기 실행
if __name__ == "__main__":
    asyncio.run(main())

    