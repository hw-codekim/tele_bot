import requests
from bs4 import BeautifulSoup
import asyncio
from telegram import Bot
import pandas as pd
import numpy as np
from report import Report
from biz_day import Bizday
import json
from datetime import datetime
import os

# GitHub Actions에서는 환경변수, 로컬에서는 bot_key.json
BOT_TOKEN = os.environ.get('BOT_TOKEN')
CHAT_ID = os.environ.get('CHAT_ID')

if not BOT_TOKEN or not CHAT_ID:
    # 로컬에서 bot_key.json 읽기
    with open('bot_key.json', 'r') as file:
        data = json.load(file)
        BOT_TOKEN = data['BOT_TOKEN']
        CHAT_ID = data['CHAT_ID']

bot = Bot(token=BOT_TOKEN)

## =========== 삭제 예정=========
# with open('bot_key.json', 'r') as file:
#     data = json.load(file)

# BOT_TOKEN = data['BOT_TOKEN']
# CHAT_ID = data['CHAT_ID']

# # Bot 객체 생성
# bot = Bot(token=BOT_TOKEN)
# ===================

# 네이버 뉴스에서 제목과 링크 크롤링

#=============== 레포트 상향 =====================
async def send_report_telegram(report,day):
    try:
        filtered_report = [entry for entry in report if entry['날짜'] == day]
        
        if not filtered_report:
            # 텔레그램으로 "오늘 보고서 없음" 전송
            await bot.send_message(chat_id=CHAT_ID, text=f"📅 {day} 날짜의 보고서가 없습니다.")
            print(f"⚠️ {day} 날짜의 보고서 없음")
            return
        

        all_reports = ""
        today_str = datetime.now().strftime("%Y-%m-%d")
        # today_str = '2025-05-2'
        
        for entry in report:
            slope_raw = entry['상승률'].strip()

            # 이모지 조건 처리
            if "-" in slope_raw:
                slope = f"⬇ {slope_raw}"  # 하락
            elif slope_raw in ["0%", "0.0%", "0.00%"]:
                slope = f"➖ {slope_raw}"  # 보합
            else:
                slope = f"⬆ {slope_raw}"  # 상승
            
            all_reports += (
                f"📅 날짜: {entry['날짜']}\n"
                f"😀 기업명 : {entry['기업명']}\n" +
                f"*애널리스트 : {entry['애널리스트']}\n" +
                f"*현재 목표가 : {entry['현재 목표가']}\n" +
                f"*이전 목표가 : {entry['이전 목표가']}\n" +
                f"*상승률 : {slope}\n" +
                f"*제목 : {entry['제목']}\n" +
                f"*내용 : {entry['내용']}\n" +
                "-" * 50 + "\n"
            )
        # 메시지가 4096자를 초과하면 분할하여 전송
        max_length = 4096
        while len(all_reports) > max_length:
            try:
                # 4096자를 초과하는 부분 찾기
                split_point = all_reports.rfind("-" * 50, 0, max_length)
                
                # 적절한 위치를 찾지 못한 경우, 안전하게 최대 길이로 설정
                if split_point == -1:
                    split_point = max_length
                
                # 분할된 메시지가 비어있는지 확인
                message_chunk = all_reports[:split_point].strip()
                if not message_chunk:
                    print("🚨 분할된 메시지가 비어있음. 루프 종료.")
                    break
                
                message_with_date = f"📅 날짜: {today_str}\n\n{message_chunk}"
                
                # 디버깅 출력 (전송 전)
                print(f"📩 메시지 전송 (길이: {len(message_chunk)})")
                await bot.send_message(chat_id=CHAT_ID, text=message_chunk)
                
                # 남은 부분 업데이트
                all_reports = all_reports[split_point:].lstrip()

            except Exception as e:
                print(f"❌ 예외 발생: {e}")
                break  # 루프 중단

        # 남은 메시지 전송
        if all_reports:
            message_with_date = f"📅 날짜: {today_str}\n\n{all_reports}"
            await bot.send_message(chat_id=CHAT_ID, text=all_reports)
    except Exception as e:
        print(e)
#=============== 주요종목과 지수YTD =====================

# 메인 함수
async def main():

    # news_list = naver_news('스타게이트')  # 뉴스 크롤링
    day = Bizday.biz_day()
    # day = '20251008'
    report = Report.whynot_report(day)

    await send_report_telegram(report,day)  # 텔레그램으로 전송

# 비동기 실행
if __name__ == "__main__":
    asyncio.run(main())
