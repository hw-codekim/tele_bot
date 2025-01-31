import requests
from bs4 import BeautifulSoup

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
            data.append([title, link])
    print(data)

naver_news('김')
