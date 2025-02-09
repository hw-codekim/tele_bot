
import pandas as pd
import yfinance as yf

# S&P 500 구성 종목 가져오기 (위키피디아 사용)
class Snp500_Adr:
    def get_snp500_adr():
        wiki_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        sp500_table = pd.read_html(wiki_url, header=0)[0]  # 첫 번째 테이블 선택
        
        # 티커(symbol) 목록 추출
        ticker = sp500_table['Symbol'].tolist()
        # ticker = ['AAPL','META','NVDA']
        data = yf.download(ticker,period="251d").diff()['Close'].iloc[-250:]  # 최근 5일 데이터를 가져옵니다
        print(data)
        result = []
        for date,changes in data.iterrows():

            up, down, flat = 0, 0, 0  # 상승, 하락, 보합 개수 초기화

            for symbol,val in changes.items():
                if val > 0 :
                    up += 1
                elif val < 0 :
                    down += 1
                else:
                    flat += 1
            result.append([date.strftime("%Y-%m-%d"),up,down,flat])
        result_df = pd.DataFrame(result, columns=['날짜','상승','하락','유지'])
        result_df['상승MA20'] = result_df['상승'].rolling(window=20).sum()
        result_df['하락MA20'] = result_df['하락'].rolling(window=20).sum()

        result_df['ADR'] = round(result_df['상승MA20']/result_df['하락MA20']*100,1)
        return result_df
    

if __name__ == '__main__':
    df = Snp500_Adr.get_snp500_adr()
    print(df)