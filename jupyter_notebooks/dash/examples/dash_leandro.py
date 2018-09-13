# downloaded from: https://github.com/plotly/dash-table-experiments/blob/master/usage.py

import dash
from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
import dash_table_experiments as dt
import json
import pandas as pd
import numpy as np
import plotly

# libs maths
from pandas import Series, DataFrame
import pandas as pd
import numpy as np
import sys

sys.path.append("../")

# lib graficas
import cufflinks as cf
import plotly.plotly as py
import plotly.graph_objs as go



from genesis.persistence.CollectionEODDataPersistence import CollectionEODDataPersistence

app = dash.Dash()

app.scripts.config.serve_locally = True
# app.css.config.serve_locally = True

def convert_indicators(indicators):
    indicators["exchange_ticker"]= indicators["exchange"] + "_" +  indicators["ticker"]
    indicators = indicators.drop("Unnamed: 0", axis=1)

    indicators = indicators.pivot_table(
        index="exchange_ticker", columns="name", values="value"
    )
    return indicators.reset_index()

DF_INDICATORS = convert_indicators(pd.read_csv("data/indicators.csv", sep=";", decimal=","))




DF_SIMPLE = pd.DataFrame({
    'x': ['A', 'B', 'C', 'D', 'E', 'F'],
    'y': [4, 3, 1, 2, 3, 6],
    'z': ['a', 'b', 'c', 'a', 'b', 'c']
})

ROWS = [
    {'a': 'AA', 'b': 1},
    {'a': 'AB', 'b': 2},
    {'a': 'BB', 'b': 3},
    {'a': 'BC', 'b': 4},
    {'a': 'CC', 'b': 5},
    {'a': 'CD', 'b': 6}
]


app.layout = html.Div([
    html.H4('Screener Metodo Leandro'),
    dt.DataTable(
        rows=DF_INDICATORS.to_dict('records'),

        # optional - sets the order of columns
        columns=sorted(DF_INDICATORS.columns),

        row_selectable=True,
        filterable=True,
        sortable=True,
        selected_row_indices=[],
        id='datatable-gapminder'
    ),
    html.Div(id='selected-indexes'),
    dcc.Graph(
        id='graph-gapminder'
    ),
], className="container")

@app.callback(
    Output('datatable-gapminder', 'selected_row_indices'),
    [Input('graph-gapminder', 'clickData')],
    [State('datatable-gapminder', 'selected_row_indices')])
def update_selected_row_indices(clickData, selected_row_indices):
    print("update_selected_row_indices --> {}", clickData)
    if clickData:
        for point in clickData['points']:
            if point['pointNumber'] in selected_row_indices:
                selected_row_indices.remove(point['pointNumber'])
            else:
                selected_row_indices.append(point['pointNumber'])
    return selected_row_indices


@app.callback(
    Output('graph-gapminder', 'figure'),
    [Input('datatable-gapminder', 'rows'),
     Input('datatable-gapminder', 'selected_row_indices')])
def update_figure(rows, selected_row_indices):
    dff = pd.DataFrame(rows)
    print("\n\n\n\n{} --> {}".format(dff.tail(), selected_row_indices))

    if len(selected_row_indices)<=0:
        return

    exchange, ticker = dff.exchange_ticker.iloc[selected_row_indices[0]].split("_")
    print("exchange: {}".format(ticker))

    collection = CollectionEODDataPersistence.get(exchange=exchange, ticker=ticker)
    ohlc_df = collection.to_df()

    fig = plotly.tools.make_subplots(
        rows=2, cols=1,
        subplot_titles=('VolRel', 'GDP Per Capita'),
        shared_xaxes=True)
    marker = {'color': ['#0074D9']*len(dff)}
    for i in (selected_row_indices or []):
        marker['color'][i] = '#FF851B'
    fig.append_trace({
        'x': ohlc_df['date'],
        'y': ohlc_df['close'],
        'type': 'bar',
        'marker': marker
    }, 1, 1)
    fig.append_trace({
        'x': ohlc_df['date'],
        'y': ohlc_df['open'],
        'type': 'bar',
        'marker': marker
    }, 2, 1)
    fig['layout']['showlegend'] = False
    fig['layout']['height'] = 800
    fig['layout']['margin'] = {
        'l': 40,
        'r': 10,
        't': 60,
        'b': 200
    }
    fig['layout']['yaxis3']['type'] = 'log'
    return fig


app.css.append_css({
    'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'
})

if __name__ == '__main__':
    app.run_server(debug=True)