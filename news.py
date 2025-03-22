import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta
import os
import pandas as pd

def convert_relative_date(date_str):
    """'2일 전' 같은 상대 날짜를 'YYYY.MM.DD' 형식으로 변환"""
    today = datetime.today()

    match = re.search(r'(\d+)', date_str)  # 숫자 찾기
    if not match:
        return "날짜 없음"  # 숫자가 없으면 기본값 반환

    num = int(match.group())

    if "시간" in date_str:
        converted_date = today - timedelta(hours=num)
    elif "분" in date_str:
        converted_date = today - timedelta(minutes=num)
    elif "일" in date_str:
        converted_date = today - timedelta(days=num)
    else:
        return "날짜 없음"  # 다른 형식이면 기본값 반환

    return converted_date.strftime("%Y.%m.%d")

def naver_news(content):
    data = []
    for i in range(1):
        response = requests.get(f"https://search.naver.com/search.naver?where=news&sm=tab_jum&query={content}&start={i}1")
        html = response.text
        soup = BeautifulSoup(html, 'html.parser')
        articles = soup.select(".list_news > li")

        for article in articles:
            title = article.select_one(".news_tit").text
            link = article.select_one(".news_tit").attrs['href']
            
            # 🟢 'info_group' 클래스를 사용하여 날짜 찾기
            date = "날짜 없음"
            info_group = article.select_one(".info_group")
            if info_group:
                info_text = info_group.text.strip()
                
                # 🟢 YYYY.MM.DD 형식이면 그대로 사용
                date_match = re.search(r'\d{4}\.\d{2}\.\d{2}', info_text)
                if date_match:
                    date = date_match.group()
                # 🟢 상대적 날짜 ('X일 전', 'X시간 전') 변환
                elif "전" in info_text:
                    date = convert_relative_date(info_text)

            data.append([title, link, date])
    
    df = pd.DataFrame(data,columns=['제목','링크','날짜'])
    return df

def save(df,today):  # 엑셀 저장 함수
    filename = f'./뉴스/종목_뉴스_{today}.csv'
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
    today = datetime.today().strftime('%Y%m%d')

    stocks = ['하나머티리얼즈','이엔에프테크놀로지','유진테크','에스앤에스텍','심텍','가온칩스','유니드','글로벌텍스프리']
    merge_df = pd.DataFrame()
    for stock in stocks:
        news_df = naver_news(stock)
        news_df.insert(0,'종목',stock)
   
        merge_df = pd.concat([merge_df,news_df])
  
    save(merge_df,today)
    