import dash
import plotly.graph_objs as go
import chart_studio.plotly as py
import pandas as pd
import flask
import dash_core_components as dcc
import dash_html_components as html

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

server = flask.Flask(__name__)
app = dash.Dash(__name__, server=server, external_stylesheets=external_stylesheets)

def show_dashboard():

    df = pd.read_csv("finance-charts-apple.csv")

    trace_high = go.Scatter(
        x=df.Date,
        y=df['AAPL.High'],
        name = "AAPL High",
        line = dict(color = '#17BECF'),
        opacity = 0.8)

    trace_low = go.Scatter(
        x=df.Date,
        y=df['AAPL.Low'],
        name = "AAPL Low",
        line = dict(color = '#7F7F7F'),
        opacity = 0.8)

    data = [trace_high,trace_low]

    layout = dict(
        title='Time Series with Rangeslider',
        xaxis=dict(
            rangeselector=dict(
                buttons=list([
                    dict(count=1,
                         label='1m',
                         step='month',
                         stepmode='backward'),
                    dict(count=6,
                         label='6m',
                         step='month',
                         stepmode='backward'),
                    dict(step='all')
                ])
            ),
            rangeslider=dict(),
            type='date'
        )
    )

    fig = dict(data=data, layout=layout)

    app.layout = html.Div([
    dcc.Graph(id='my-graph', figure=fig)
    ])    

def get_cassandra_session():

    '''profile = ExecutionProfile(
        load_balancing_policy=WhiteListRoundRobinPolicy(['127.0.0.1']),
        retry_policy=DowngradingConsistencyRetryPolicy(),
        consistency_level=ConsistencyLevel.LOCAL_QUORUM,
        serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
        request_timeout=15,
        row_factory=tuple_factory
    )'''

    cluster = Cluster(['10.0.0.5', '10.0.0.14'])
    #cluster = Cluster(['10.0.0.12', '10.0.0.14'], execution_profiles={EXEC_PROFILE_DEFAULT: profile})
    session = cluster.connect('cycling')
    session.row_factory = pandas_factory
    session.default_fetch_size = None
    return session 

if __name__ == '__main__':
    show_dashboard()
    app.run_server(host="0.0.0.0", debug=True)
