import os, sys
path = os.path.dirname(os.path.realpath(__file__)) + "/../../../"
print(path)

import sys
import socket
import pandas as pd
from time import sleep
from datetime import datetime
from numba import jit, void, int_, double


class IqFeedMarketData:
    """
    Read the information from the socket, in a buffered
    fashion, receiving only 4096 bytes at a time.

    Parameters:
    sock - The socket object
    recv_buffer - Amount in bytes to receive per read
    """
    @jit
    def read_msg(sock, recv_buffer=4096, endmsg = "!ENDMSG!", print_buffer=False):
        buffer = ""
        data = ""
        while True:
            data = sock.recv(recv_buffer)
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

    def send_msg_binary(socket, message):
        socket.sendall(message.encode('utf-8'))

    def send_msg(socket, command, param1, param2, param3, param4):
        message = "{command},{param1},{param2},{param3},{param4},\r\n".format(
            command=command, 
            param1=param1, param2=param2, 
            param3=param3, param4=param4,
            #param4=param4, param6=param6
        )
        socket.sendall(message.encode('utf-8'))
        return read_msg(socket)

    def set_client_name(socket, client_name):
        message = "S,SET CLIENT NAME,{name},\r\n".format(
            name=client_name, 
            #param4=param4, param6=param6
        )
        socket.sendall(message.encode('utf-8'))

        
        
    def download_data(socket, ticker, period, receive_data_func):
        print("queue symbol: %s..." % ticker)
        sock = socket
        # Construct the message needed by IQFeed to retrieve data
        # Open a streaming socket to the IQFeed server locally

        # Send the historical data request
        # message and buffer the data
        #data = read_historical_data_socket(sock)
        try:

            data = send_msg(socket, "HID", ticker, period, 10, 50)
            receive_data_func(data)
            import ipdb; ipdb.set_trace()
            df.to_csv("d:/data/ifeed/%s.csv" % ticker)

            return data
        except Exception as e:
            print("error to bring data: {} --> {}".format(ticker, e))
            return None
        # Remove all the endlines and line-ending
        # comma delimiter from each record

        # Write the data stream to disk
        #f = open("d:/data/ifeed/%s.csv" % ticker, "w")
        #f.write(data)
        #f.close()



    def convert_pandas_from_data(data):
        dataframe = pd.DataFrame([i for i in data.split("\r\n") if i not in ["", ","]])
        dataframe = dataframe[dataframe.columns[0]].str.split(",", expand=True)
        dataframe.rename({"0":"date","1":"ask"}, inplace=True)
        return dataframe

    def chunks(l, n):
        """Yield successive n-sized chunks from l."""
        for i in range(0, len(l), n):
            yield l[i:i + n]

    import sys
    time_individual = []

    def add_time(time_before):
        time_individual.append(datetime.now() - time_before)
        dtime = pd.DataFrame(time_individual, columns=["time"])
        print("media: {} --> {}".format(dtime.time.mean(), dtime.tail()) )

    def handle_data(data):
        print("handle data")
        df = convert_pandas_from_data(data)
        print (df.tail())

    def open_connection(add_time_callback, client_name, symbols, date_started):
        # Define server host, port and symbols to download
        host = "localhost"  # Localhost
        port = 9100  # Historical data socket port

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        sock.connect((host, port))
        set_client_name(sock, "Genesis_Backend_{}".format(client_name))
        #import ipdb; ipdb.set_trace()
        # Download each symbol to disk

        for keyA, symbol in enumerate(symbols):
            date_before_individual = datetime.now()
            data = download_data(sock, symbol, 3600, handle_data)

            print("list {}".format(client_name))
            """while True:
                try:
                    read_msg(sock, endmsg="", recv_buffer=1, print_buffer=True)
                except Exception as e:
                    print(e)"""
            
            add_time(date_before_individual)
            elapsed_time_total = datetime.now() - date_started
            print("\n elapsed_time_temp {} \r\n ".format(elapsed_time_total) )
            #sleep(0.5)
        sock.close()

from multiprocessing import Process


if __name__ == "__main__":
    df_symbols = pd.read_csv("..\genesis\data\collection_symbols.csv")
    date_started = datetime.now()
    process_num  = 1
    chunk_size = round(df_symbols.shape[0] / process_num)
    for keyA, symbols in enumerate(list(chunks([i for i in df_symbols.tail(10000).symbol.values.tolist()], chunk_size))):
        p = Process(target=open_connection, args=(add_time,"A{}".format(keyA), symbols, date_started))
        p.start()
    p.join()
        #open_connection(add_time, "A{}".format(keyA), symbols)
    elapsed_time_total = datetime.now() - date_started
    print("calculate process done \n elapsed_time_total {}".format(elapsed_time_total)) 
