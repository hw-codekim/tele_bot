import requests
from bs4 import BeautifulSoup
import collections
import re

class Bizday:

    if not hasattr(collections, 'Callable'):
        collections.Callable = collections.abc.Callable
    
    def biz_day(): # 네이버에서 날짜가져오기
        url = 'https://finance.naver.com/sise/sise_index.naver?code=KOSPI'
        res = requests.get(url)
        soup = BeautifulSoup(res.text,'html.parser')

        parse_day = soup.select_one('#time').text
        
        biz_day = re.findall('[0-9]+',parse_day)
        biz_day = ''.join(biz_day)
        return biz_day