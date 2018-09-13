import os, sys
path = os.path.dirname(os.path.realpath(__file__)) + "/../../../"
print(path)

import sys
import socket
import pandas as pd
from time import sleep
from datetime import datetime
from numba import jit, void, int_, double

#df_symbols =  pd.read_csv("data/collection_symbols.csv")

#print(df_symbols.tail())
@jit
def read_msg(sock, recv_buffer=4096, endmsg = "!ENDMSG!"):
    """
    Read the information from the socket, in a buffered
    fashion, receiving only 4096 bytes at a time.

    Parameters:
    sock - The socket object
    recv_buffer - Amount in bytes to receive per read
    """
    buffer = ""
    data = ""
    while True:
        data = sock.recv(recv_buffer)
        
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

    
    #G3nEs1%
    
def download_data(socket, ticker, period):
    print("queue symbol: %s..." % ticker)
    sock = socket
    # Construct the message needed by IQFeed to retrieve data
    # Open a streaming socket to the IQFeed server locally

    # Send the historical data request
    # message and buffer the data
    #data = read_historical_data_socket(sock)
    #import ipdb; ipdb.set_trace()
    try:
        #df = pd.read_csv(f, sep=",")
        data = send_msg_binary(socket, "HID,{ticker},{period},1,100,000000,230000,1,{ticker},50,\r\n".format(
            ticker=ticker, period=period))

        #data = send_msg(socket, "HID", ticker, period, 1, 50)
        #convert_pandas_from_data(data).to_csv("d:/data/ifeed/%s.csv" % ticker)
    except Exception as e:
        print("error to bring data: {} --> {}".format(ticker, e))
        return 
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
if __name__ == "__main__":
    # Define server host, port and symbols to download
    host = "127.0.0.1"  # Localhost
    port = 9100  # Historical data socket port
    date_before = datetime.now()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(2)
    sock.connect((host, port))
    param = sys.argv[1]
    print(param)
    set_client_name(sock, "Genesis_Backend_{}".format(param))
    #import ipdb; ipdb.set_trace()
    df_symbols = pd.read_csv("data\collection_symbols.csv")
    # Download each symbol to disk

    """for i in range(500):
        symbol = "APPL"
        download_data(sock, symbol, 3600)
        data = read_msg(sock)
        print(data)"""

    for keyA, symbols in enumerate(list(chunks([i for i in df_symbols.symbol.values.tolist()], 100))):
        for keyB, symbol in enumerate(symbols):
            key = (keyA+1)*(keyB)
            download_data(sock, symbol, 3600)
        data = read_msg(sock, endmsg="{},!ENDMSG!,\r\n".format(symbols[-1]))
        print(data)
        #print(data)
        elapsed_time_total = datetime.now() - date_before
        print("\n elapsed_time_temp {}".format(elapsed_time_total)) 

    elapsed_time_total = datetime.now() - date_before
    print("calculate process done \n elapsed_time_total {}".format(elapsed_time_total)) 
        #sleep(0.5)
    sock.close()
