import dash
import plotly.graph_objs as go
import chart_studio.plotly as py
import pandas as pd
import flask
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import time
from datetime import datetime, timedelta

from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy, DowngradingConsistencyRetryPolicy
from cassandra.query import tuple_factory
from cassandra import ConsistencyLevel
import cassandra.util

# stylesheet
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

# specify flask as the server
server = flask.Flask(__name__)
# configure server and stylesheet for dash app
app = dash.Dash(__name__, server=server, external_stylesheets=external_stylesheets)

# transform cassandra query result to pandas dataframe
def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)

# transform cassandra date type to python datatime
def to_date(date):
    datetime_str = str(date)
    return datetime.strptime(datetime_str, '%Y-%m-%d')

# transform cassandra date type to python datatime
def to_date_hour(date, hour):
    datetime_str = str(date) + " {hour:02d}".format(hour=hour)
    return datetime.strptime(datetime_str, '%Y-%m-%d %H')

# transform cassandra date type to str
def to_str(date):
    return str(date)

# transform cassandra date type to str
def to_str_hour(date, hour):
    return str(date) + " {hour:02d}".format(hour=hour)

# transform time str to number of days
def get_total_day(time):

    # num str to int
    num_dict = {'one': 1, 'three': 3, 'five': 5, 'six': 6, 'ten': 10}
    # time period to day
    time_dict = {'day': 1, 'month': 30, 'year': 365 }

    # split the time by "_" into two part
    num_str, period_str = time_split = time.split("_")
    # transform num_str to int
    num = num_dict[num_str]
    # transform period_str to int
    period = time_dict[period_str]
    # total days equal num * period
    day = num * period

    return day

# get scatter plot data and layout
def get_data_layout(bitcoin_reddit_df, title):
    
    # scatter for bitcoin
    bitcoin = go.Scatter(
        x=bitcoin_reddit_df['date'],
        y=bitcoin_reddit_df['price'],
        name = "Bitcoin",
        line = dict(color = '#17BECF'),
        opacity = 0.8)

    # scatter for reddit
    reddit = go.Scatter(
        x=bitcoin_reddit_df['date'],
        y=bitcoin_reddit_df['score'],
        name = "Reddit",
        line = dict(color = '#7F7F7F'),
        opacity = 0.8)

    # add both of them to list
    data = [bitcoin,reddit]

    # layout for rangeslider
    layout = dict(
        title=title,
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

    return (data, layout)

# get the dataframe for real time data
def current_get_df(subreddit, sentiment):

    # select data, hour, price from bitcoin streaming table
    bitcoin_query = "SELECT date, hour, price FROM bitcoin_streaming_test5"
    # select data, hour, price from reddit streaming table
    reddit_query = "SELECT date, hour, score FROM reddit_streaming_test5_{subreddit}_{sentiment}" \
                    .format(subreddit=subreddit, sentiment=sentiment)

    # get bitcoin dataframe 
    bitcoin_df = session.execute(bitcoin_query)._current_rows
    # get reddit dataframe
    reddit_df = session.execute(reddit_query)._current_rows

    # transform date column to str
    bitcoin_df['date'] = bitcoin_df.apply(lambda x: to_str_hour(x.date, x.hour), axis = 1)
    reddit_df['date'] = reddit_df.apply(lambda x: to_str_hour(x.date, x.hour), axis = 1)

    # merge two dataframe on date
    bitcoin_reddit_df = pd.merge(left=bitcoin_df, right=reddit_df, left_on='date', right_on='date')
    # sort the merge dataframe by date
    bitcoin_reddit_df = bitcoin_reddit_df.sort_values('date')

    return bitcoin_reddit_df

# get the dataframe for historical data
def past_get_df(time, subreddit, sentiment):
  
    # one month and five day uses time period to hour
    if time == "one_month" or time == "five_day":

        # select date, hour, bitcoin price, reddit score from database
        query = "SELECT date, hour, price, score FROM {time}_{subreddit}_{sentiment}" \
        .format(time=time, subreddit=subreddit, sentiment=sentiment)

        # get reddit bitcoin dataframe  
        bitcoin_reddit_df = session.execute(query)._current_rows

        # change date and hour column to python datatime type
        bitcoin_reddit_df['date'] = bitcoin_reddit_df.apply(lambda x: to_date_hour(x.date, x.hour), axis = 1)

    # other uses time period to day
    else:

        # select date, bitcoin price, reddit score from database
        query = "SELECT date, price, score FROM {time}_{subreddit}_{sentiment}" \
        .format(time=time, subreddit=subreddit, sentiment=sentiment)

        # get reddit bitcoin dataframe
        bitcoin_reddit_df = session.execute(query)._current_rows

        # change cassandra date type to python datatime type
        bitcoin_reddit_df['date'] = bitcoin_reddit_df.apply(lambda x: to_date(x.date), axis = 1)


    # transform time str to total days
    day = get_total_day(time
            )
    # get the max date in dataframe
    max_date = bitcoin_reddit_df['date'].max()

    # get the min date within time interval
    min_date = max_date - timedelta(days=day)

    # mask for date larger than min date
    mask = bitcoin_reddit_df['date'] > min_date

    # filter out all the relevant data
    bitcoin_reddit_df['date'] = bitcoin_reddit_df[mask]

    # sort dataframe by date
    bitcoin_reddit_df = bitcoin_reddit_df.sort_values('date')

    # return dataframe
    return bitcoin_reddit_df

# show dashboard
def show_dashboard(session):

    app.layout = html.Div([
        html.Div([
            # dropdown for different time interval
            dcc.Dropdown(
                id='time-dropdown',
                options=[
                    {'label': '5 DAY', 'value': 'five_day'},
                    {'label': '1 MONTH', 'value': 'one_month'},
                    {'label': '3 MONTH', 'value': 'three_month'},
                    {'label': '6 MONTH', 'value': 'six_month'},
                    {'label': '1 YEAR', 'value': 'one_year'},
                    {'label': '3 YEAR', 'value': 'three_year'},
                    {'label': '5 YEAR', 'value': 'five_year'}
                ],
                value='five_year'
            ),
            # dropdown for differnt subreddit
            dcc.Dropdown(
                id='subreddit-dropdown',
                options=[
                    {'label': 'All', 'value': 'all'},
                    {'label': 'All_Below', 'value': 'all_below'},
                    {'label': 'Bitcoin', 'value': 'bitcoin'},
                    {'label': 'CryptoCurrency', 'value': 'cryptocurrency'},
                    {'label': 'Ethereum', 'value': 'ethereum'},
                    {'label': 'Ripple', 'value': 'ripple'}
                ],
                value='all'
            ), 
            # dropdown for using sentiment analysis or not
            dcc.Dropdown(
                id='sentiment-dropdown',                                                                                                                                     
                options=[
                    {'label': 'No NLP', 'value': 'no_nlp'},
                    {'label': 'With NLP', 'value': 'with_nlp'},
                ],
                value='no_nlp'
            ),
        ],style={'width':'15%','display':'inline-block', 'vertical-align': 'left', "margin-left": "20px"}),

        html.Div([
            # graph for historical data
            dcc.Graph(id='bitcoin-reddit-graph'),
        ],style={'width':'80%','display':'inline-block', 'vertical-align': 'middle'}),

        html.Div([
        html.Div([
            # graph for spike count
	    dcc.Graph(id='bitcoin-reddit-bar_plot'),
        ],style={'width':'30%','display':'inline-block', 'vertical-align': 'left', "margin-right": "20px"}),

        html.Div([
            #graph for real time data
	    dcc.Graph(id='bitcoin-reddit-stream-graph'),
        ],style={'width':'60%','display':'inline-block', 'vertical-align': 'right'}),
        ]),
        # real time update component
        dcc.Interval(
            id='interval-component',
            interval=5*1000, # in milliseconds
            n_intervals=0
        )
    ])    

# control stream graph by real time component and subreddit, sentiment dropdown
@app.callback(Output('bitcoin-reddit-stream-graph', 'figure'),
              [Input('interval-component', 'n_intervals'),
               Input('subreddit-dropdown', 'value'),                                                                                                                                                        Input('sentiment-dropdown', 'value')])
def update_graph_live(n, subreddit, sentiment):

    # get real time dataframe
    bitcoin_reddit_df = current_get_df(subreddit, sentiment)

    # get data and layout
    data, layout = get_data_layout(bitcoin_reddit_df, 'Bitcoin_Reddit_Real_Time')

    return {
        'data' : data,
        'layout' : layout
    }

# control bar plot by time, subreddit and sentiment dropdown
@app.callback(
    Output('bitcoin-reddit-bar_plot', 'figure'),
    [Input('time-dropdown', 'value'),
     Input('subreddit-dropdown', 'value'),                                                                                                                                                        Input('sentiment-dropdown', 'value')]) 
def update_figure(time, subreddit, sentiment):

    # select bitcoin spike count and reddit relevant bitcoin spike count
    query = "SELECT bitcoin_spike, bitcoin_reddit_spike FROM spike" + \
            " WHERE interval = '{time}' AND subreddit = '{subreddit}' AND nlp = '{sentiment}'" \
             .format(time=time, subreddit=subreddit, sentiment=sentiment)

    # get dataframe for spike count
    spike_df = session.execute(query)._current_rows

    # convert dataframe to list
    spike_list = spike_df.values.tolist()

    # get bitcoin spike count and reddit relevant bitcoin spike count
    bitcoin_spike, reddit_relevant_count = spike_list[0][0], spike_list[0][1]

    # get reddit unrelevant bitcoin spike count
    reddit_unrelevant_count = bitcoin_spike - reddit_relevant_count

    # bar plot for reddit unrelevant count
    reddit_relevant = go.Bar(
        x=['spike_count'],
        y=[reddit_unrelevant_count],
        name='other'
    )

    # bar plot for reddit relevant count
    reddit_unrelevant = go.Bar(
        x=['spike_count'],
        y=[reddit_relevant_count],
        name='reddit_relevant'
    )

    # add both of them to list
    data = [reddit_relevant, reddit_unrelevant]

    # layout for bitcoin spike
    layout=go.Layout(title='Bitcoin_Spike', barmode='stack')

    return {
        'data' : data,
        'layout' : layout
    }

# historical data graph controlled by time, subreddit, sentiment dropdown
@app.callback(
    Output('bitcoin-reddit-graph', 'figure'),
    [Input('time-dropdown', 'value'),
     Input('subreddit-dropdown', 'value'),
     Input('sentiment-dropdown', 'value')])
def update_figure(time, subreddit, sentiment):

    # get historical data dataframe
    bitcoin_reddit_df = past_get_df(time, subreddit, sentiment)

    # get data and layout
    data, layout = get_data_layout(bitcoin_reddit_df, 'Bitcoin_Reddit_Historical')

    return {
        'data' : data,
        'layout' : layout
    }

# get cassandra session for executing query
def get_cassandra_session():

    # two seeds in Cassandra
    cluster = Cluster(['10.0.0.5', '10.0.0.6'])
    
    # connect to bitcoin_reddit key space
    keyspace = 'bitcoin_reddit'
    session = cluster.connect(keyspace)

    # specify pandas factory as the method to convert cassandra query result
    session.row_factory = pandas_factory

    session.default_fetch_size = None
    return session 

if __name__ == '__main__':
    # get cassandra session
    session = get_cassandra_session()
    # show dash board
    show_dashboard(session)
    # start the server
    app.run_server(host="0.0.0.0", debug=True, port = 80)
