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

from pyorbital.orbital import Orbital
satellite = Orbital('TERRA')

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

server = flask.Flask(__name__)
app = dash.Dash(__name__, server=server, external_stylesheets=external_stylesheets)

def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)

def to_str(date):
    return str(date)

def to_str_hour(date, hour):
    return str(date) + " {hour:02d}".format(hour=hour)

def normalize_hundred(score):
    if score*100 > 10000: return score
    else: return score*100

def normalize_ten(score):
    if score*10 > 10000: return score
    else: return score*10

def current_get_df():

    bitcoin_query = "SELECT date, hour, price FROM bitcoin_streaming_test_new"
    reddit_query = "SELECT date, hour, score FROM reddit_streaming_test_new"

    bitcoin_df = session.execute(bitcoin_query)._current_rows
    reddit_df = session.execute(reddit_query)._current_rows

    bitcoin_df['date'] = bitcoin_df.apply(lambda x: to_str_hour(x.date, x.hour), axis = 1)
    reddit_df['date'] = reddit_df.apply(lambda x: to_str_hour(x.date, x.hour), axis = 1)

    bitcoin_reddit_df = pd.merge(left=bitcoin_df, right=reddit_df, left_on='date', right_on='date')

    print(bitcoin_reddit_df)

    bitcoin_reddit_df = bitcoin_reddit_df.sort_values('date')

    return bitcoin_reddit_df

def past_get_df(time, subreddit, sentiment):
   
    if time == "one_month" or time == "five_day":
        query = "SELECT date, hour, price, score FROM {time}_{subreddit}_{sentiment}".format(time=time, subreddit=subreddit, sentiment=sentiment)
    else:
        query = "SELECT date, price, score FROM {time}_{subreddit}_{sentiment}".format(time=time, subreddit=subreddit, sentiment=sentiment)

    print(query)
    bitcoin_reddit_rows = session.execute(query)
    bitcoin_reddit_df = bitcoin_reddit_rows._current_rows

    if time == "one_month" or time == "five_day":
        bitcoin_reddit_df['date'] = bitcoin_reddit_df.apply(lambda x: to_str_hour(x.date, x.hour), axis = 1)
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
                {'label': 'Ripple', 'value': 'ripple'}
            ],
            value='all'
        ), 
	dcc.Dropdown(
            id='sentiment-dropdown',                                                                                                                                     
            options=[
                {'label': 'No', 'value': 'no_nlp'},
                {'label': 'Yes', 'value': 'with_nlp'},
            ],
            value='no_sentiment'
        ),
        dcc.Graph(id='bitcoin-reddit-graph'),
	dcc.Graph(id='bitcoin-reddit-bar_plot'),
	dcc.Graph(id='bitcoin-reddit-stream-graph'),
        dcc.Interval(
            id='interval-component',
            interval=1*1000, # in milliseconds
            n_intervals=0
        )
    ])    

@app.callback(Output('bitcoin-reddit-stream-graph', 'figure'),
              [Input('interval-component', 'n_intervals')])
def update_graph_live(n):

    bitcoin_reddit_df = current_get_df()

    bitcoin = go.Scatter(
        x=bitcoin_reddit_df['date'],
        y=bitcoin_reddit_df['price'],
        name = "Bitcoin",
        line = dict(color = '#17BECF'),
        opacity = 0.8)

    reddit = go.Scatter(
        x=bitcoin_reddit_df['date'],
        y=bitcoin_reddit_df['score'],
        name = "Reddit",
        line = dict(color = '#7F7F7F'),
        opacity = 0.8)

    data = [bitcoin,reddit]

    layout = dict(
        title='Bitcoin_Reddit_Real_Time',
        xaxis=dict(
            rangeselector=dict(
                buttons=list([
		    dict(step='all')
		])
            ),
            rangeslider=dict(),
            type='date'
        )
    )

    return {
        'data' : data,
        'layout' : layout
    }

@app.callback(
    Output('bitcoin-reddit-bar_plot', 'figure'),
    [Input('time-dropdown', 'value'),
     Input('subreddit-dropdown', 'value'),                                                                                                                                                        Input('sentiment-dropdown', 'value')]) 
def update_figure(time, subreddit, sentiment):

    query = "SELECT bitcoin_spike, bitcoin_reddit_spike FROM spike WHERE interval = {time} AND subreddit = {subreddit} AND nlp = {sentiment}"\
             .format(time=time, subreddit=subreddit, sentiment=sentiment)

    spike_rows = session.execute(query)
    
    #print(

    reddit_relevant_count = bitcoin_reddit_spike
    reddit_unrelevant_count = bitcoin_spike - bitcoin_reddit_spike

    reddit_relevant = go.Bar(
        x=['spike_count'],
        y=[reddit_relevant_count],
        name='reddit_relevant'
    )
    
    reddit_unrelevant = go.Bar(
        x=['spike_count'],
        y=[reddit_unrelevant_count],
        name='other'
    )

    data = [reddit_relevant, reddit_unrelevant]

    layout=go.Layout(barmode='stack')

    return {
        'data' : data,
        'layout' : layout
    }

@app.callback(
    Output('bitcoin-reddit-graph', 'figure'),
    [Input('time-dropdown', 'value'),
     Input('subreddit-dropdown', 'value'),
     Input('sentiment-dropdown', 'value')])
def update_figure(time, subreddit, sentiment):

    bitcoin_reddit_df = past_get_df(time, subreddit, sentiment)

    bitcoin = go.Scatter(
        x=bitcoin_reddit_df['date'],
        y=bitcoin_reddit_df['price'],
        name = "Bitcoin",
        line = dict(color = '#17BECF'),
        opacity = 0.8)

    reddit = go.Scatter(
        x=bitcoin_reddit_df['date'],
        y=bitcoin_reddit_df['score'],
        name = "Reddit",
        line = dict(color = '#7F7F7F'),
        opacity = 0.8)    

    data = [bitcoin,reddit]
        
    layout = dict(
        title='Bitcoin_Reddit_Relation',
        xaxis=dict(
            rangeselector=dict(
                buttons=list([
                    dict(count=180,
                         label='6m',
                         step='day',
                         stepmode='backward'),
                    dict(step='all')
                ])
            ),
            rangeslider=dict(),
            type='date'
        )
    )

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
