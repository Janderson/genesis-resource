from genesis.shared.domain_model import DomainModel
from genesis.entities.EODData import EODData
from pandas import DataFrame, Series
import pandas as pd

class CollectionEODData(DomainModel):

    def __init__(self):
        self.internal_df = None
        #self.internal_df.set_index(["date"], inplace=)
    
    def get_from_index(self, index):
        return self.internal_df.iloc[index]

    def get_from_date(self, date):
        return self.internal_df[self.internal_df.index == date]

    def from_df(self, dataframe):
        
    def to_df(self):
        return self.internal_df

    def persist(self):
        self.internal_df.to_csv("collect.csv")

    def add_or_update(self, EODDataItem):
        if isinstance(EODDataItem, EODData):
            eoddata_df = EODDataItem.to_df()
            internal_df = self.internal_df

            print("=======================")
            if internal_df is None:
                internal_df = eoddata_df
            elif internal_df[internal_df.index == eoddata_df.index.min()].empty:
                internal_df = internal_df.append(eoddata_df)
            else:
                internal_df[internal_df.index == eoddata_df.index.min()] = eoddata_df.values
            print("###################################")
            self.internal_df = internal_df
            self.update_after_add()
            print(self.internal_df.tail())
            print("------------------------------------")
        else:
            raise Exception("Not EODData")

    def update_after_add(self):
        self.internal_df.sort_index(inplace=True, ascending=True)
 
