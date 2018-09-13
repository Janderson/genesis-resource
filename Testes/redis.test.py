import redis
import pandas as pd
from os import listdir
from datetime import datetime
path = "data/eoddata/"

redisConn = redis.StrictRedis(host="localhost", port=6379, db=0)
date_before = datetime.now()

def store_redis(filename):
    print("store redis {}".format(filename))
    df = pd.read_csv("{path}{filename}".format(
        path=path, filename=file_name
    ))
    if df is None:
        return 

    df.drop(["ticker"], inplace=True, axis=1)
    df.drop(df.columns[0], axis=1, inplace=True)
    df.set_index("date", inplace=True)
    redisConn.set(file_name.replace(".csv", ""), df.to_msgpack(compress='zlib'))


big_data_redis = redisConn.get("big_csv")

if big_data_redis is None:
    big_df = None
    for index, file_name in enumerate(listdir(path)):
        redis_data = redisConn.get(file_name.replace(".csv", ""))

        if redis_data is None:
            store_redis(file_name)
        else:
            print("get from redis ({}) --> {}".format(index, file_name))
            df = pd.read_msgpack(redis_data)
            if big_df is None:
                big_df = df
            else:
                big_df.append(df)
    #redisConn.set("big_csv", big_df.to_msgpack(compress='zlib'))

#big_df = pd.read_msgpack(big_data_redis)
elapsed_time_total = datetime.now() - date_before

print("elapsed_time_load {}".format(elapsed_time_total)) 

elapsed_time_total = datetime.now() - date_before
print("elapsed_time_total {}".format(elapsed_time_total)) 
