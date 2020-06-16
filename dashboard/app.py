import dash
import plotly.graph_objs as go
import chart_studio.plotly as py
import pandas as pd
import flask
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import time
import datetime
from datetime import datetime

from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy, DowngradingConsistencyRetryPolicy
from cassandra.query import tuple_factory
from cassandra import ConsistencyLevel
import cassandra.util

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

server = flask.Flask(__name__)
app = dash.Dash(__name__, server=server, external_stylesheets=external_stylesheets)

def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)

def to_str(date):
    return str(date)

def to_str(date, hour):
    return str(date) + " {hour:02d}".format(hour=hour)

def to_str(date, hour, minute):
    return str(date) + " {hour:02d}".format(hour=hour) + ":{minute:02d}".format(minute=minute)

def to_str(date, hour, minute, second):
    return str(date) + " {hour:02d}".format(hour=hour) + ":{minute:02d}".format(minute=minute) + ":{second:02d}".format(second=second)

def normalize_hundred(score):
    if score*100 > 10000: return score
    else: return score*100

def normalize_ten(score):
    if score*10 > 10000: return score
    else: return score*10

def get_df(time, subreddit, sentiment):
   
    '''if time == "five_day" or time == "current":
        query = "SELECT date, hour, minute, price, score FROM reddit_bitcoin_{time}_{subreddit}_{sentiment}".format(time=time, subreddit=subreddit, sentiment=sentiment)
    elif time == "six_month" or time == "three_month" or time == "one_month":
        query = "SELECT date, hour, price, score FROM reddit_bitcoin_{time}_{subreddit}_{sentiment}".format(time=time, subreddit=subreddit, sentiment=sentiment)
    else:
        query = "SELECT date, price, score FROM reddit_bitcoin_{time}_{subreddit}_{sentiment}".format(time=time, subreddit=subreddit, sentiment=sentiment)'''

    query = "SELECT date, hour, minute, second, score FROM reddit_streaming_test3"
    time = "current"

    bitcoin_reddit_rows = session.execute(query)
    bitcoin_reddit_df = bitcoin_reddit_rows._current_rows

    if time == "current":
        bitcoin_reddit_df['date'] = bitcoin_reddit_df.apply(lambda x: to_str(x.date, x.hour, x.minute, x.second), axis = 1)
    elif time == "five_day":
        bitcoin_reddit_df['date'] = bitcoin_reddit_df.apply(lambda x: to_str(x.date, x.hour, x.minute), axis = 1)
        #bitcoin_reddit_df['score'] = bitcoin_reddit_df['score'].apply(normalize_hundred)
    elif time == "six_month" or time == "three_month" or time == "one_month":
        bitcoin_reddit_df['date'] = bitcoin_reddit_df.apply(lambda x: to_str(x.date, x.hour), axis = 1)
        #bitcoin_reddit_df['score'] = bitcoin_reddit_df['score'].apply(normalize_hundred)
    else:
        bitcoin_reddit_df['date'] = bitcoin_reddit_df.apply(lambda x: to_str(x.date), axis = 1)
    bitcoin_reddit_df = bitcoin_reddit_df.sort_values('date')

    return bitcoin_reddit_df

def show_dashboard(session):

    #streaming = session.execute('SELECT date, hour, minute, second, score FROM reddit_streaming_test3')
    #streaming_df = streaming._current_rows
    #print(streaming_df)

    app.layout = html.Div([
        
	dcc.Dropdown(
            id='time-dropdown',
            options=[
                {'label': '1 DAY', 'value': 'current'},
		{'label': '5 DAY', 'value': 'five_day'},
 		{'label': '1 MONTH', 'value': 'one_month'},
                {'label': '3 MONTH', 'value': 'three_month'},
                {'label': '6 MONTH', 'value': 'six_month'},
                {'label': '1 YEAR', 'value': 'one_year'},
                {'label': '3 YEAR', 'value': 'three_year'},
                {'label': '5 YEAR', 'value': 'five_year'},
		{'label': '10 YEAR', 'value': 'ten_year'}
            ],
            value='one_month'
        ),
       	dcc.Dropdown(
            id='subreddit-dropdown',
            options=[
                {'label': 'All', 'value': 'all'},
                {'label': 'Bitcoin', 'value': 'bitcoin'},
                {'label': 'CryptoCurrency', 'value': 'cryptocurrency'},
                {'label': 'Ethereum', 'value': 'ethereum'},
                {'label': 'Tether', 'value': 'tether'}
            ],
            value='all'
        ), 
	dcc.Dropdown(
            id='sentiment-dropdown',                                                                                                                                     
            options=[
                {'label': 'No', 'value': 'no_sentiment'},
                {'label': 'Yes', 'value': 'with_sentiment'},
            ],
            value='no_sentiment'
        ),
        dcc.Graph(id='bitcoin-reddit-graph')
    ])    

@app.callback(
    Output('bitcoin-reddit-graph', 'figure'),
    [Input('time-dropdown', 'value'),
     Input('subreddit-dropdown', 'value'),
     Input('sentiment-dropdown', 'value')])
def update_figure(time, subreddit, sentiment):

    bitcoin_reddit_df = get_df(time, subreddit, sentiment)

    '''bitcoin = go.Scatter(
        x=bitcoin_reddit_df['date'],
        y=bitcoin_reddit_df['price'],
        name = "Bitcoin",
        line = dict(color = '#17BECF'),
        opacity = 0.8)'''

    reddit = go.Scatter(
        x=bitcoin_reddit_df['date'],
        y=bitcoin_reddit_df['score'],
        name = "Reddit",
        line = dict(color = '#7F7F7F'),
        opacity = 0.8)    

    data = [reddit]
        
    layout = dict(
        title='Bitcoin_Reddit_Relation',
        xaxis=dict(
            rangeselector=dict(
                buttons=list([

                    dict(count=30,
                         label='1m',
                         step='hour',
                         stepmode='backward'),
                    dict(count=180,
                         label='6m',
                         step='day',
                         stepmode='backward'),
                    dict(count=365,
                         label='1y',
                         step='day',
                         stepmode='backward'),
                    dict(count=1095,
                         label='3y',
                         step='week',
                         stepmode='backward'),
                    dict(count=1825,
                         label='5y',
                         step='week',
                         stepmode='backward'),
                    dict(step='all')
                ])
            ),
            rangeslider=dict(),
            type='date'
        )
    )

    print(data)

    print(layout)

    return {
        'data' : data,
        'layout' : layout
    }

def get_cassandra_session():

    '''profile = ExecutionProfile(
        load_balancing_policy=WhiteListRoundRobinPolicy(['127.0.0.1']),
        retry_policy=DowngradingConsistencyRetryPolicy(),
        consistency_level=ConsistencyLevel.LOCAL_QUORUM,
        serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
        request_timeout=15,
    )'''

    cluster = Cluster(['10.0.0.5', '10.0.0.14'])
    session = cluster.connect('bitcoin_reddit')
    session.row_factory = pandas_factory
    session.default_fetch_size = None
    return session 

if __name__ == '__main__':
    session = get_cassandra_session()
    show_dashboard(session)
    app.run_server(host="0.0.0.0", debug=True)
