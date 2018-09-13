import os, sys
path = os.path.dirname(os.path.realpath(__file__)) + "/../../../"
print(path)

import sys
import socket
import pandas as pd
from time import sleep
from datetime import datetime
from numba import jit, void, int_, double
class IQFeedConnection():

    def __init__(self, host = "localhost", port=9100 ):
        self.host = host
        self.port = port 
        self.time_individual = []


    def open_connection(self,  client_name):
        # Define server host, port and symbols to download
        date_before = datetime.now()

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(4)
        self.sock.connect(
            (self.host, self.port)
        )
        #self.set_client_name("Genesis_Backend_{}".format(client_name))



    def close_connection(self):
        self.sock.close()

    """
    Read the information from the socket, in a buffered
    fashion, receiving only 4096 bytes at a time.

    Parameters:
    recv_buffer - Amount in bytes to receive per read
    """
    @jit
    def read_msg(self, recv_buffer=4096, endmsg = "\n", print_buffer=False):
        buffer = ""
        data = ""
        while True:
            data = self.sock.recv(recv_buffer)
            if print_buffer:
                print(data)
            
            buffer += str(data.decode("utf-8"))
            # Check if the end message string arrives
            #print(".", end="")
            if endmsg in buffer:

                break

        # Remove the end message string
        buffer = buffer.replace("!ENDMSG!", "")
        return buffer

    def send_msg_binary(self, message, endmsg = "\n"):
        self.sock.sendall((message+endmsg).encode('utf-8'))

    def send_command(self, command, param1, param2, param3, param4=None, param5=None, param6=None, param7=None):
        self._last_command = "{command},{param1}{param2}{param3}{param4}{param5}{param6}{param7},\r\n".format(
            command=command, 
            param1=NoneOrStr(param1, ","), param2=NoneOrStr(param2, ","), 
            param3=NoneOrStr(param3, ","), param4=NoneOrStr(param4, ","),
            param5=NoneOrStr(param5, ","), param6=NoneOrStr(param6, ","),
            param7=NoneOrStr(param7, ",")
        )
        self.sock.sendall(self._last_command.encode('utf-8'))
        return self.read_msg()

    @property
    def last_command(self):
        return self._last_command

    def set_client_name(self, client_name):
        message = "S,SET CLIENT NAME,{name},\r\n".format(
            name=client_name, 
            #param4=param4, param6=param6
        )
        self.sock.sendall(message.encode('utf-8'))

    def get_hour_cmd(self, ticker, period, qtd_days):
        try:
            data = self.send_command(
                "HID", ticker, period, qtd_days, qtd_days*15, "100000"
            )
            return data
        except Exception as e:
            print("error to bring data: {} --> {}".format(ticker, e))
            return None
        

    def get_hour_cmd(self, ticker, period, qtd_days):
        try:
            data = self.send_command(
                "HID", ticker, period, qtd_days, qtd_days*15, "100000"
            )
            return data
        except Exception as e:
            print("error to bring data: {} --> {}".format(ticker, e))
            return None

    def get_daily_cmd(self, ticker, qtd_days):
        try:
            data = self.send_command(
                "HDX", ticker, qtd_days, "0"
            )
            return data
        except Exception as e:
            print("error to bring data: {} --> {}".format(ticker, e))
            return None

    def get_today_ticks(self, ticker):
        try:
            market_session_start = "093000"
            market_session_end = "160000"
            start_date_str = datetime.now().strftime("%Y%m%d {}".format(market_session_start))
            end_date_str = datetime.now().strftime("%Y%m%d {}".format(market_session_end))
            ticks_data = self.send_command("HTT", ticker, start_date_str, end_date_str)
            return ticks_data
        except Exception as e:
            print("error to bring data: {} --> {}".format(ticker, e))
            return None


    def download_data(self, ticker, period):
        try:
            data = self.send_command("HID", ticker, period, 20, 50, 0)
            return data
        except Exception as e:
            print("error to bring data: {} --> {}".format(ticker, e))
            return None


    def has_data(self, data):
        return data!=None and not ("!NO_DATA!" in data)


    def add_time(self, time_before):
        self.time_individual.append(datetime.now() - time_before)
        dtime = pd.DataFrame(self.time_individual, columns=["time"])


    def set_listen_stock(self, stock):
        if not hasattr(self, "stocks_listeing"):
            self.stocks_listeing = []
        if (stock not in self.stocks_listeing) and len(self.stocks_listeing)<=200:
            self.send_msg_binary("t{}\n".format(stock))
            self.stocks_listeing.append(stock)

    def set_delisten_stock(self, stock):
        self.send_msg_binary("r{}\n".format(stock))
        self.stocks_listeing.remove(stock)

    def set_listen_ohlc_daily(self):
        self.send_msg_binary("""
            S,CURRENT UPDATE FIELDNAMES,Most Recent Trade,Most Recent Trade Size,Most Recent Trade Time,Most Recent Trade Market Center,Total Volume,Bid,Bid Size,Ask,Ask Size,Open,High,Low,Close,Message Contents,Most Recent Trade Conditions\n
        """)
    def set_protocol(self):
        self.send_msg_binary(
            "S,SET PROTOCOL,6.0\n"
        )
        self.send_msg_binary(
            "S,CURRENT PROTOCOL,6.0\n"
        )


    def get_intraday_data(self, ticker):
        #self.set_listen_stock(ticker)
        data = iq.read_msg(endmsg="\n")
        data = data.split(",")
        print(data[0:5])
        dict_data = None
        if data[0]=="Q":
            dict_data = {
                "ticker" : data[1],
                "open" : data[11],
                "high" : data[12],
                "low" : data[13],
                "close" : data[2],
                "volume" : data[6],
            }
            dict_data = pd.DataFrame([dict_data])
        return dict_data
def rotate(l, n):
    return l[n:] + l[:n]

if __name__ == "__main__":
    symbols = pd.read_csv("../genesis-backend/genesis/data/collection_symbols.csv")
    symbols = list(symbols.tail(10000).symbol.values)
    iq = IQFeedConnection(host="localhost", port=5009)

    iq.open_connection("")
    iq.set_protocol()
    iq.set_listen_ohlc_daily()
    dfglobal = None
    count=0
    while True:
        for key, symbol in enumerate(symbols):
            if key>=200:
                break
            #print("{}--{}".format(key, symbol))
            iq.set_listen_stock(symbol)
        try:
            df = iq.get_intraday_data("")
            if df is not None:
                remove_symbol = str(df.ticker.values[0])
                symbols.remove(remove_symbol)
                symbols = rotate(symbols, 20)
                iq.set_delisten_stock(remove_symbol)
                #print(symbols)
                if dfglobal is None:
                    dfglobal = df
                else:
                    dfglobal = dfglobal.append(df)
                print(dfglobal[["ticker", "open", "high", "low", "close", "volume"]].groupby("ticker").last())
                print(dfglobal.shape, len(symbols))
        except Exception as e:
            print("exp {}".format(e)) 
        count+=1
        if count>=250:
            count=0
            dfglobal[["ticker", "open", "high", "low", "close", "volume"]].groupby("ticker").last().to_csv("d:\data.csv")
        #open_connection(add_time, "A{}".format(keyA), symbols)
    iq.close_connection()