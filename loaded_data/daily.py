from daily_stock_price import Daily_stock_price
from daily_moneyflow_each import KrxMoney
from datetime import datetime,timedelta
import holidays
from pandas.tseries.offsets import CustomBusinessDay
from tqdm import tqdm
import os
import pandas as pd



class Main:
    def daily():
        biz_day = datetime.today().strftime('%Y%m%d')
        # biz_day = '20250521'
        gubuns = ['1000', '3000', '3100', '6000', '9000', '8000']
        period = 120

        
        price_df = pd.DataFrame()
        money_df = pd.DataFrame()

        kr_holidays = holidays.Korea(years=[2024, 2025])
        holidays_list = list(kr_holidays.keys())
        kr_business_day = CustomBusinessDay(weekmask="Mon Tue Wed Thu Fri", holidays=holidays_list)

        for i in tqdm(range(period)):
            start_date = datetime.strptime(biz_day, '%Y%m%d')
            target_date = (start_date - (i * kr_business_day)).strftime('%Y%m%d')
            print(target_date)
            price_rst_df = Daily_stock_price.price(target_date)
            price_df = pd.concat([price_df, price_rst_df]).drop_duplicates(subset=['기준일','종목명'], keep='last')
        
            for gubun in gubuns:
                money_rst_df = KrxMoney.daily_money_flow(target_date, gubun)
                money_df = pd.concat([money_df, money_rst_df]).drop_duplicates(subset=['날짜', '구분', '종목명'], keep='last')

        price_df_pivoted = price_df.pivot_table(index='종목명', columns='기준일', values='시가총액').reset_index()
        
        Daily_stock_price.save(price_df_pivoted)
        # price_df_pivoted.to_clipboard()
        print('종목별 거래가격 저장 완료!')
        KrxMoney.save(money_df)
        print('종목별 거래대금 저장 완료!')

if __name__ == '__main__'  :
    try:
        Main.daily()
    except Exception as e:
        print(e)
    

