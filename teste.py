# -*- coding: utf-8 -*-
"""
Spyder Editor

Este é um arquivo de script temporário.
"""

import pandas as pd
from pandas import Series, DataFrame
import numpy as np

import cufflinks as cf
import pandas as pd
import qgrid as qg
#indicator = calc(df)

from plotly.offline import download_plotlyjs, init_notebook_mode,plot,iplot

init_notebook_mode()

from genesis.persistence.CollectionEODDataPersistence import CollectionEODDataPersistence
from genesis.calc.indicators import *
from genesis.calc.methods import *
from genesis.calc.common import *


collection = CollectionEODDataPersistence.get(
    exchange="NYSE", ticker="AA"
)
quotes = collection.to_df().reset_index()
quotes.tail()

def ZIGZAG_LEANDRO_V2(quotes, parameters = {}):
    O, H, L, C, V = unpack_quotes(quotes)
    ###### METASTOCK LINE ###### 
    #   candidato_fundo = L<Ref(L, -1);
    #   candidato_topo = H>Ref(H, -1);
    ############################
    candidato_fundo = (L<REF(L, -1)).astype("int64");
    candidato_topo = (H>REF(H, -1)).astype("int64");    
    
    ###### METASTOCK LINE ###### 
    #   novo_topo_fundo = candidato_topo + (-candidato_fundo);
    ############################
    novo_topo_fundo = candidato_topo + (-candidato_fundo);
    exchange = IF( 
        (novo_topo_fundo==1 & REF(VALUEWHEN( novo_topo_fundo!=0, novo_topo_fundo)==-1, -1))  |
        (novo_topo_fundo==-1 & REF(VALUEWHEN(novo_topo_fundo!=0, novo_topo_fundo)==1, -1) ) 
    , 1, 0); 

    return (exchange)

quotes["zz"] = ZIGZAG_LEANDRO_V2(quotes)


#O, H, L, C, V = unpack_quotes(quotes)

quotes["zig"] = ZIGZAG_LEANDRO_V2(quotes)

quotes[["zig", "close"]].iplot(kind="bar")




quotes["zz"] = ZIGZAG_LEANDRO_V2(quotes)

def BARSSINCE(serie):
    ret = pd.Series(dtype=float, index=serie.index)
    ret[serie] = 0
    ret[serie == False] = 1
    bars_count = ret.cumsum()
    return bars_count - bars_count[serie].reindex(bars_count.index).ffill()


"""
 function: VALUEWHEN
 Description:
    Returns the value of the ARRAY when the EXPRESSION was true on the n -th most recent occurrence. 
"""
def VALUEWHEN(condition, values_array, number=1):
    #newarray = np.where(condition, values_array, np.nan)
    arr_index = np.where(condition, condition.index, np.nan)
    
    vw_df = pd.DataFrame(arr_index, index=condition.index)
    vw_df.columns = ["valuewhen"]
    vw_df["condition"] = condition
    vw_df["new_index"] = vw_df["valuewhen"].groupby(
        vw_df.valuewhen
    ).max().shift(number-1).dropna()
    vw_df["val"] = values_array
    vw_df["new_value"] = np.take(
        vw_df["val"].values, 
        vw_df.new_index.fillna(0).astype("int64")
    )
    vw_df["new_value"] = np.where(
        np.isnan(vw_df.new_index), 
        np.nan, 
        vw_df.new_value
    )
    #newarray = arr_index
    newarray = pd.Series(vw_df.new_value, index=condition.index)
    newarray.fillna(method="ffill", inplace=True)
    return newarray


def HIGHESTSINCE_2(condition_serie, values_array, number=1):
    #newarray = np.where(condition, values_array, np.nan)
    arr_index = np.where(condition_serie, condition_serie.index, np.nan)
    
    vw_df = pd.DataFrame(arr_index, index=condition_serie.index)
    vw_df.columns = ["valuewhen"]
    vw_df["condition"] = condition_serie
    vw_df["new_index"] = vw_df["valuewhen"].groupby(
        vw_df.valuewhen
    ).max().shift(number-1).dropna()
    vw_df["val"] = values_array
    """vw_df["new_value"] = np.take(
        vw_df["val"].values, 
        vw_df.new_index.fillna(0).astype("int64")
    )
    vw_df["new_value"] = np.where(
        np.isnan(vw_df.new_index), 
        np.nan, 
        vw_df.new_value
    )"""
    #newarray = arr_index
    #newarray = pd.Series(vw_df.new_value, index=condition_serie.index)
    
    vw_df["maxsince"] = vw_df.groupby(
        vw_df["condition"].cumsum()
    )["val"].cummax() #[["val", "new_index"]].groupby("new_index", as_index=False).max() #newarray.fillna(method="ffill")
    return vw_df.maxsince


quotes["highest"] = HHV(quotes.high, 3)

#quotes[["date", "close", "zz", "highest"]].tail(50)

#HHV

quotes["zz_eq1"] = BARSSINCE(quotes.zz == 1)
quotes["m_since1"] = HIGHESTSINCE_2( 
    (quotes.zz == 1), 
    quotes.high, number=1
)
quotes["m_since2"] = HIGHESTSINCE_2( 
    (quotes.zz == 1), 
    quotes.high, number=2
)


quotes["exchange"] = ZIGZAG_LEANDRO_V2(quotes)



#quotes.close.rolling(window=20).max()
#quotes["aaaa"] = quotes.close.rolling(window=20).max()
#quotes
#quotes
#.tail(200)