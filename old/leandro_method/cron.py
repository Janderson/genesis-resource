import schedule, time, os
from market_data import YCharts

def import_symbols():
    print("calcular sinais!!!")
    mk_data = YCharts("z5+uL3rqVhZywltNCXDPhQ", "NYSE")
    for i in mk_data.get_symbols().iterrows():
        print (mk_data.get_ohlc_from_stock(i[1].symbol))
        print (mk_data.get_split_from_stock(i[1].symbol))
        count=0
        if count == 10: 
            break
        else:
            count+=1
        break

def import_data_from_mkt_data():
    print("fired!")
    import_symbols()

if __name__=="__main__":

    # put schedules here
    #schedule.every().day.at("18:45").do(import_data_from_mkt_data)
    schedule.every(10).seconds.do(import_data_from_mkt_data)

    while True:
        schedule.run_pending()
        time.sleep(50)
