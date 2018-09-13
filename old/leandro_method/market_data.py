

import pandas as pd
import datetime 
class MarketData:
    def __init__(self, api_key):
        pass
    
    #@abstractmethod
    def get_symbols(self):
        pass


# TODO - this classe Should be moved to another file 
from pycharts import CompanyClient, IndicatorClient, MutualFundClient

class YCharts(MarketData):
    def __init__(self, api_key, exchange):
        self.company_client = CompanyClient(api_key)
        self.exchange = exchange
    
    def get_symbols(self):
        companies = self.company_client.get_securities(exchange=self.exchange)
        return pd.DataFrame(companies["response"])

    
    def get_ohlc_from_stock(
        self, 
        stock, 
        startdate=None,
        enddate=None
        ):
        
        now = datetime.datetime.now()
        if not startdate == None:
            startdate = startdate
        else:
            startdate = now - datetime.timedelta(days=50)

        now = datetime.datetime.now()
        if not enddate == None:
            enddate = enddate
        else:
            enddate = now 
            
        
        series_rsp = self.company_client.get_series(
            [stock], 
            ['open_price', 'high_price', 'low_price', 'close_price', 'volume'],
            query_start_date=startdate, 
            query_end_date=enddate
        )
        #print(self.metric_to_series(series_rsp, stock, "price"))
        df_close = DataFrame(
            self.metric_to_series(series_rsp, stock, "close_price"))
        df_open = DataFrame(
            self.metric_to_series(series_rsp, stock, "open_price"))
        df_low =  DataFrame(
            self.metric_to_series(series_rsp, stock, "low_price"))
        df_high =  DataFrame(
            self.metric_to_series(series_rsp, stock, "high_price"))
        df_volume =  DataFrame(
            self.metric_to_series(series_rsp, stock, "volume"))
        df = df_open \
            .merge(df_high, right_index=True, left_index=True) \
            .merge(df_low, right_index=True, left_index=True) \
            .merge(df_close, right_index=True, left_index=True) \
            .merge(df_volume, right_index=True, left_index=True) 
        
        # set columns 
        df.columns = ["open", "high", "low", "close", "volume"]
        return df


if __name__=="__main__":
    y = YCharts("z5+uL3rqVhZywltNCXDPhQ", "NYSE")
    for i in y.get_symbols().iterrows():
        print (y.get_ohlc_from_stock(i[1].symbol))
        print (y.get_split_from_stock(i[1].symbol))
        count=0
        if count == 10: 
            break
        else:
            count+=1
        break