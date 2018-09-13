from sqlalchemy import create_engine
import datetime
import pandas as pd
import numpy as np


class DataBase:

    """
        Make the connection into database and delivery to
        var self.engine
    """
    def __init__(self, db_user, db_pass, db_host, db_port, db_database):
        self.db_user = db_user
        self.db_pass = db_pass
        self.db_host = db_host
        self.db_port = db_port
        self.db_database = db_database

    def connect(self):
        self.engine = create_engine(
            'postgresql://{user}:{pass}@{host}:{port}/{database}'.format(
                **{"user": self.db_user,
                "pass": self.db_pass,
                "host": self.db_host,
                "port": self.db_port,
                "database": self.db_database})
            )


    """
        Read All symbols from database
    """
    def symbols(self):
        return pd.read_sql_query(
            "select distinct ticker from eod_us_stock_prices", 
            con = self.engine
        )


    def quotes_to_dataframe(
        ticker, 
        last_date = str(datetime.date.today()), 
        limit = '200', 
        order = 'desc'
    ):
        df = pd.read_sql_query(
            "select * from eod_us_stock_prices where ticker='{}'" + 
            "and date <= '{}' order by date {} limit {}", con = self.engine).iloc[::-1].format(
                ticker,
                last_date,
                order,
                limit
            )
        return df



if __name__=="__main__":
    d = DataBase(
        "scientist", 
        'Trader#Science881', 
        'quotes.c23ubmdma2jt.us-east-1.rds.amazonaws.com',
        5432,
        'quotes'
        )

    d.connect()
    print(d.symbols())