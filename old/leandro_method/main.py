from sqlalchemy import create_engine
import os
import sys
import datetime
import pandas as pd
import numpy as np

engine = ""

def pct(n1, n2):
    if n2 == 0:
        return 0
    return ((n1 - n2) / (n2 * 100))*100


def symbols():
    return pd.read_sql_query("select distinct ticker from eod_us_stock_prices", con = engine)

def vol_rel(quotes):
    return (quotes.adj_volume/quotes.adj_volume.rolling(window=21).mean()).iloc[-1]

def financial_vol(quotes):
    return (quotes.adj_volume * quotes.adj_close).iloc[-1]

def calc_force_index_term(quotes, term):
    return (quotes.adj_close.iloc[-1] - min(quotes.adj_close.tail(term))) / (max(quotes.adj_close.tail(term)) - min(quotes.adj_close.tail(term)))

def force_index(quotes):
    return calc_force_index_term(quotes, 5) + calc_force_index_term(quotes, 10) + calc_force_index_term(quotes, 21) + calc_force_index_term(quotes, 50) + calc_force_index_term(quotes, 200)

def average_volatility(quotes, term = 21):
    return (np.log(quotes.adj_close / quotes.adj_close.shift(1))).rolling(term).std().rolling(term).mean().tail(1)*100*np.sqrt(252)

def last_top_or_bottom_volatility(quotes):
    ticker = get_ticker(quotes)
    tb = top_bottoms(ticker, limit = 1)

    if tb.empty:
        return 0

    last_top_or_bottom_date = tb.iloc[0].date
    offset = quotes[quotes['date'] == tb.iloc[0].date].index[0]
    return 0
    # return average_volatility(quotes[0:-offset])

def sma_21_distance(quotes):
    close = quotes.iloc[-1].adj_close
    sma21 = quotes.adj_close.rolling(window=21).mean().iloc[-1]
    dist = abs((((close + abs(sma21 - close))/close) - 1))
    dist = ((close - sma21) / sma21) * 10000
#    print("close: " + str(close))
#    print("sma21: " + str(sma21))
    resp = (dist / average_volatility(quotes))[0]
#    print(resp)
    return round(resp, 2)

def sma_values(quotes, period):
    if len(quotes.index) < 200:
        return False
    
    sma = quotes.adj_close.rolling(window=period).mean()
    sma_last = sma.iloc[-1]
    sma_prior = sma.iloc[-2]
    sma_asc = sma_last > sma_prior
    sma_desc = sma_last < sma_prior
    return { 'data': sma, 'last': sma_last, 'prior': sma_prior, 'asc': sma_asc, 'desc': sma_desc }

def correction_bullish(quotes):
    ticker = get_ticker(quotes)
    close = quotes.iloc[-1].adj_close
    tb = top_bottoms(ticker)

    if len(tb.index) < 4:
        return 0

    # Bullish trend
    # Find the first bottom, going backwards
    first_bottom_index = 0
    if tb.iloc[0].pattern == -1:
        first_bottom = tb.iloc[0].price
    else:
        first_bottom = tb.iloc[1].price
        first_bottom_index = 1

    # Top between the first bottom
    top_between = tb.iloc[first_bottom_index+1].price
    next_bottom = tb.iloc[first_bottom_index+2].price
    offset = quotes[quotes['date'] == tb.iloc[first_bottom_index+2].date].index[0]
    
    high_leg_distance = pct(top_between, next_bottom)*100
    high_leg_price_distance = top_between - next_bottom
    top_from_close_price_distance = top_between - close
    next_bottom_vh = float(average_volatility(quotes[0:-offset]))

    correction = 0
    if high_leg_distance > next_bottom_vh * 0.2:
        correction = (top_from_close_price_distance * 100) / high_leg_price_distance

    # print('correction: ' + str(correction))
     
    return correction

def correction_bearish(quotes):
    ticker = get_ticker(quotes)
    close = quotes.iloc[-1].adj_close
    tb = top_bottoms(ticker)

    if len(tb.index) < 4:
        return 0

    # Bullish trend
    # Find the first top, going backwards
    first_top_index = 0
    if tb.iloc[0].pattern == 1:
        first_top = tb.iloc[0].price
    else:
        first_top = tb.iloc[1].price
        first_top_index = 1

    # Bottom between the first top
    bottom_between = tb.iloc[first_top_index+1].price
    next_top = tb.iloc[first_top_index+2].price
    offset = quotes[quotes['date'] == tb.iloc[first_top_index+2].date].index[0]
    
    high_leg_distance = pct(bottom_between, next_top)*100
    high_leg_price_distance = bottom_between - next_top
    bottom_from_close_price_distance = close - bottom_between
    next_top_vh = float(average_volatility(quotes[0:-offset]))
    dist_20_pct_vh = next_top_vh * 0.2

    # print(high_leg_price_distance)
    # print(top_from_close_price_distance)
    
    correction = 0
    if high_leg_distance > next_top_vh * 0.2:
        correction = (bottom_from_close_price_distance * 100) / high_leg_price_distance

    return correction

def correction_recovery(quotes):
    return 0

def relative_amplitude(quotes):
    return ((quotes.adj_high/quotes.adj_low) / (quotes.adj_high/quotes.adj_low).rolling(window=21).mean()).iloc[-1]

def body(quotes):
    return ((quotes.adj_open.iloc[-1] - quotes.adj_close.iloc[-1]) / (quotes.adj_high.iloc[-1] - quotes.adj_low.iloc[-1])) * -1

def breakout_moviment(quotes):
    ticker = get_ticker(quotes)
    tb = top_bottoms(ticker, limit = 1)
    if tb.empty:
        return 0
    last_top_or_bottom = tb.iloc[0].price
    close = quotes.iloc[-1].adj_close
    dist = (close - last_top_or_bottom) / (last_top_or_bottom * 100)
    resp = (dist / average_volatility(quotes))[0]
    return round(resp, 2)

def breakout(quotes):
    periods = [5, 10, 21, 50, 100, 200]
    ind = 0 
    ticker = get_ticker(quotes)
    tb = top_bottoms(ticker)
    for period in periods:
        if len(quotes.index) < period:
            continue
        top_bottom = tb[tb['date'] > quotes.iloc[-period].date]
        top_breakout = (quotes.adj_high.iloc[-1] > top_bottom[top_bottom['pattern'] == 1]['price'])
        bottom_breakout = (quotes.adj_low.iloc[-1] < top_bottom[top_bottom['pattern'] == -1]['price'])

        if top_breakout.any():
            ind = ind + 1

        if bottom_breakout.any():
            ind = ind - 1

    return ind


def trend(quotes):
    ticker = get_ticker(quotes)
    date = get_last_date(quotes)
    tp = top_bottoms(ticker, date, 3)
    high = quotes.iloc[-1].adj_high
    low = quotes.iloc[-1].adj_low
    close = quotes.iloc[-1].adj_close
    trend = 0
    trend_pos = 0
    trend_neg = 0
    
    if len(tp.index) < 3:
        return 0

    bottom_top_bottom = tp.iloc[0].pattern == -1 and tp.iloc[1].pattern == 1 and tp.iloc[2].pattern == -1
    top_bottom_top = tp.iloc[0].pattern == 1 and tp.iloc[1].pattern == -1 and tp.iloc[2].pattern == 1
    
    asc = tp.iloc[0].price > tp.iloc[2].price
    desc = tp.iloc[0].price < tp.iloc[2].price

    sma_21 = sma_values(quotes, 21)
    sma_50 = sma_values(quotes, 50)
    sma_200 = sma_values(quotes, 200)
    
    # A. Calculate Tops and Bottoms
    
    # Top Bottom Top
    if top_bottom_top and desc and low < tp.iloc[1].price:
        trend_neg = trend_neg - 1
        
    if top_bottom_top and asc and high > tp.iloc[0].price:
        trend_pos = trend_pos + 1
        
    # Bottom Top Bottom
    if bottom_top_bottom and asc and high > tp.iloc[1].price:
        trend_pos = trend_pos + 1
        
    if bottom_top_bottom and desc and low < tp.iloc[0].price:
        trend_neg = trend_neg - 1
    
        
    # C. SMA21 asc or desc
    if sma_21 and sma_50 and sma_200:
        # B. SMA21 x close
        if close > sma_21['last']:
            trend_pos = trend_pos + 1

        if close < sma_21['last']:
            trend_neg = trend_neg - 1

        # C. SMA21 x SMA50
        if sma_21['asc'] and sma_50['asc'] and sma_21['last'] > sma_50['last']:
            trend_pos = trend_pos + 1
    
        if sma_21['desc'] and sma_50['desc'] and sma_21['last'] < sma_50['last']:
            trend_neg = trend_neg - 1
        
        # D. SMA50 X SMA200
        if sma_50['asc'] and sma_200['asc'] and sma_50['last'] > sma_200['last']:
            trend_pos = trend_pos + 1
    
        if sma_50['desc'] and sma_200['desc'] and sma_50['last'] < sma_200['last']:
            trend_neg = trend_neg - 1
        
        # E. SMA21 asc / desc
        if sma_21['asc']:
            trend_pos = trend_pos + 1
            
        if sma_21['desc']:
            trend_neg = trend_neg - 1

    if trend_neg <= -3:
       trend = trend_neg

    if trend_pos >= 3:
       trend = trend_pos 

    return trend
    

# Posicao dos candles eh invertida!!!
# candle atual: quotes.loc[idx, 'adj_high']
# candle anterior: quotes.loc[idx+1, 'adj_high']
# candle posterior: quotes.loc[idx-1, 'adj_high']

def is_top(quotes, idx):
    if idx >= quotes.tail(1).index[0]:
        return False
    if idx < 1:
        return False

    vol_h = float(average_volatility(quotes[quotes.date <= quotes.loc[idx, 'date']].iloc[::-1]))

    high = quotes.loc[idx, 'adj_high']
    high_back_1 = quotes.loc[idx+1, 'adj_high']
    high_next_1 = quotes.loc[idx-1, 'adj_high']
    low = quotes.loc[idx, 'adj_low']
    low_back_1 = quotes.loc[idx+1, 'adj_low']
    low_next_1 = quotes.loc[idx-1, 'adj_low']

    if high > high_back_1 and high > high_next_1 and low > low_next_1:
        dist = 100 - ( (low_next_1*100) / high)
        if dist >= (0.1*vol_h):
            return high 

    # Confirmation in the next candle
    if idx+1 >= quotes.tail(1).index[0]:
        return False

    if idx-2 < 0:
        return False

    high_back_2 = quotes.loc[idx+2, 'adj_high']
    high_next_2 = quotes.loc[idx-2, 'adj_high']
    low_back_2 = quotes.loc[idx+2, 'adj_low']
    low_next_2 = quotes.loc[idx-2, 'adj_low']

    if high > high_back_1 and high > high_next_1 and high > high_next_2 and low > low_back_1 and low > low_next_2:
        dist = 100 - ( (low_next_2*100) / high)
        if dist >= (0.1*vol_h):
            return high


    # +3 candles 
    if idx+2 >= quotes.tail(1).index[0]:
        return False

    if idx-3 < 0:
        return False

    high_back_3 = quotes.loc[idx+3, 'adj_high']
    high_next_3 = quotes.loc[idx-3, 'adj_high']
    low_back_3 = quotes.loc[idx+3, 'adj_low']
    low_next_3 = quotes.loc[idx-3, 'adj_low']

    if high > high_back_1 and high > high_next_1 and high > high_next_2 and high > high_next_3  and low > low_next_3:
        dist = 100 - ( (low_next_3*100) / high)
        if dist >= (0.1*vol_h):
            return high

    return False

def is_bottom(quotes, idx):
    if idx >= quotes.tail(1).index[0]:
        return False
    if idx < 1:
        return False     

    vol_h = float(average_volatility(quotes[quotes.date <= quotes.loc[idx, 'date']]))

    high = quotes.loc[idx, 'adj_high']
    high_back_1 = quotes.loc[idx+1, 'adj_high']
    high_next_1 = quotes.loc[idx-1, 'adj_high']
    low = quotes.loc[idx, 'adj_low']
    low_back_1 = quotes.loc[idx+1, 'adj_low']
    low_next_1 = quotes.loc[idx-1, 'adj_low']

    # if low < low_back_1 and low < low_next_1 and high < high_back_1 and high < high_next_1:
    if low < low_back_1 and low < low_next_1 and high < high_next_1:
        dist = ( (100 * high_next_1) / low) - 100
        if dist >= (0.1*vol_h):
            return low

    # Confirmation in the next candle
    if idx+1 >= quotes.tail(1).index[0]:
        return False

    if idx-2 < 0:
        return False

    high_back_2 = quotes.loc[idx+2, 'adj_high']
    high_next_2 = quotes.loc[idx-2, 'adj_high']
    low_back_2 = quotes.loc[idx+2, 'adj_low']
    low_next_2 = quotes.loc[idx-2, 'adj_low']

    if low < low_back_1 and low < low_next_1 and low < low_next_2 and high < high_back_1 and high < high_next_2:
        dist = ( (100 * high_next_2) / low) - 100
        if dist >= (0.1*vol_h):
            return low

    # Next 3 candles
    if idx+2 >= quotes.tail(1).index[0]:
        return False

    if idx-3 < 0:
        return False

    high_back_3 = quotes.loc[idx+3, 'adj_high']
    high_next_3 = quotes.loc[idx-3, 'adj_high']
    low_back_3 = quotes.loc[idx+3, 'adj_low']
    low_next_3 = quotes.loc[idx-3, 'adj_low']

    if low < low_back_1 and low < low_next_1 and low < low_next_3 and high < high_back_1 and high < high_next_3:
        dist = ( (100 * high_next_2) / low) - 100
        if dist >= (0.1*vol_h):
            return low

    return False

def get_ticker(quotes):
    return str(quotes.iloc[-1].ticker).strip()

def get_last_date(quotes):
    return str(quotes.iloc[-1].date)

def calculate_top_bottom(quotes):
    if len(quotes.index) == 0:
       return

    ticker = get_ticker(quotes)
    quotes = quotes.iloc[::-1]
    query = "delete from top_bottoms where ticker = '" + ticker + "'"
    engine.execute(query)

    previous_pattern = 0
    for idx in reversed(quotes.index):
        save_data = False
        pattern = 0
        price = 0
        date = 0 
#        print('reverse idx: ' + str(idx))
#        print(quotes.loc[idx, 'date'])

        if previous_pattern != 1:
            if is_top(quotes, idx):
                price = quotes.loc[idx, 'adj_high']
                date = quotes.loc[idx, 'date']
                save_data = True
                pattern = 1
                previous_pattern = pattern

        if previous_pattern != -1:
            if is_bottom(quotes, idx):
                if quotes.loc[idx, 'date'] != date:
                    price = quotes.loc[idx, 'adj_low']
                    date = quotes.loc[idx, 'date']
                    save_data = True
                    pattern = -1
                    previous_pattern = pattern


        if save_data:
            query = ("insert into top_bottoms (ticker, date, pattern, price) values ("
             "'" + ticker + "'" + "," 
             "'" + str(date) + "'" + ","
             "'" + str(int(pattern)) + "'" + ","
             "'" + str(price) + "')")
            engine.execute(query)
            save_data = False

def top_bottoms(ticker, date = str(datetime.date.today()), limit = 1000):
    return pd.read_sql_query("select * from top_bottoms where ticker='" + ticker + "' and date <= '" + date  + "' order by date desc limit " + str(limit), con = engine)

def calculate_top_bottom_breakouts(quotes, limit, date = str(datetime.date.today())):
    ticker = get_ticker(quotes)
    tb = top_bottoms(ticker, date, limit)
    tb_breakout = 0
    for index, row in tb.iterrows():
        if row['pattern'] == 1:
            if quotes.iloc[-1].high > row['price']:
                tb_breakout += 1
    
        if row['pattern'] == -1:
            if quotes.iloc[-1].low < row['price']:
                tb_breakout += -1
        
    return tb_breakout

def since_top_bottom(quotes, date = str(datetime.date.today())):
    tb = top_bottoms(get_ticker(quotes), date = str(datetime.date.today()))
    if tb.empty:
        return 0
    else:
        return quotes[quotes['date'] == tb.iloc[0].date].index[0]


def calculate(quotes):
    if len(quotes.index) == 0:
       return

    last_date = str(quotes.date[0])

    ticker = get_ticker(quotes)

    query = ("insert into leandro_method (ticker, date, financial_vol, relative_vol, force_index, body, top_bottom_last_breakout, top_bottom_breakout, since_top_bottom, sma_21_distance, relative_amplitude, breakout_moviment, breakout, trend, correction_bullish, correction_bearish, last_top_or_bottom_volatility, average_volatility) values ("
             "'" + ticker + "'" + "," 
             "'" + last_date + "'" + ","
             + str(int(financial_vol(quotes))) + ","
             "'" + str(vol_rel(quotes)) + "'" + ","
             "'" + str(force_index(quotes)) + "'" + ","
             "'" + str(body(quotes)) + "'" + ","
             "'" + str(calculate_top_bottom_breakouts(quotes, 2, last_date)) + "'" + ","
             "'" + str(calculate_top_bottom_breakouts(quotes, 6, last_date)) + "'" + ","
             "'" + str(since_top_bottom(quotes, last_date)) + "'" + ","
             "'" + str(sma_21_distance(quotes)) + "'" + ","
             "'" + str(relative_amplitude(quotes)) + "'" + ","
             "'" + str(breakout_moviment(quotes)) + "'" + ","
             "'" + str(breakout(quotes)) + "'" + ","
             "'" + str(trend(quotes)) + "'" + ","
             "'" + str(correction_bullish(quotes)) + "'" + ","
             "'" + str(correction_bearish(quotes)) + "'" + ","
             "'" + str(last_top_or_bottom_volatility(quotes)) + "'" + ","
             "'" + str(float(average_volatility(quotes))) + "'"
             ") on conflict(ticker, date) do update set "
             "relative_vol = excluded.relative_vol,"
             "force_index = excluded.force_index,"
             "body = excluded.body,"
             "top_bottom_breakout = excluded.top_bottom_breakout,"
             "top_bottom_last_breakout = excluded.top_bottom_last_breakout,"
             "since_top_bottom = excluded.since_top_bottom,"
             "sma_21_distance = excluded.sma_21_distance,"
             "relative_amplitude = excluded.relative_amplitude,"
             "breakout_moviment = excluded.breakout_moviment,"
             "average_volatility = excluded.average_volatility,"
             "correction_bullish = excluded.correction_bullish,"
             "correction_bearish = excluded.correction_bearish,"
             "last_top_or_bottom_volatility = excluded.last_top_or_bottom_volatility,"
             "breakout = excluded.breakout,"
             "trend = excluded.trend,"
             "financial_vol = excluded.financial_vol")
    engine.execute(query) 

market_data_connect()


if len(sys.argv) == 1:
    date = str(datetime.date.today())
else:
    date = sys.argv[1]

# test_ticker = 'BKNG'
# q = quote(test_ticker, date)
# calculate_top_bottom(q)
# calculate(q)
# exit()

print('Fetching equities list...')
equities = symbols()

print('Calculating indicators...')
for index, row in equities.iterrows():
   print(row['ticker'])
   q = quote(row['ticker'], date)
   calculate_top_bottom(q)
   calculate(q)


# for index, row in ticker_list
#     df = pd.read_sql_query("select * from eod_us_stock_prices where ticker='" + ticker_list['ticker'] + "' order by date desc limit 200 " + order, con = engine)
# df = pd.read_sql_query("select * from eod_us_stock_prices where ticker='TSLA' and date <= '2017-11-19' order by date asc", con = engine)
