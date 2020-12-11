#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pymysql.cursors
import pymysql
import pandas as pd
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
import plotly.express as px
import matplotlib.pyplot as plt
import plotly.graph_objs as go
from dash.dependencies import Input, Output, State
import datetime
# from datetime import datetime
import dash_bootstrap_components as dbc


# # Get all stock codes and stock company names

# In[2]:


def all_stock_codes_and_stock_company_names():
    db_host = 'finalprojectgroup4-550.c5zurnhs7may.us-east-1.rds.amazonaws.com'
    db_user = 'final550group4'
    db_password = 'final550group4'
    db_database = 'hsi'

    connection = pymysql.connect(host=db_host,
                                 user=db_user,
                                 password=db_password,
                                 db=db_database,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    dbcursor = connection.cursor()
    query="""
    SELECT sit.code, sit.company_name
    FROM stock_info_table sit;"""
    
    out_df = pd.read_sql(query, connection)
    connection.close()
    return out_df
    


# In[3]:


hsi_code_name_df = all_stock_codes_and_stock_company_names()


# In[ ]:





# In[4]:


def all_participant_info():
    db_host = 'finalprojectgroup4-550.c5zurnhs7may.us-east-1.rds.amazonaws.com'
    db_user = 'final550group4'
    db_password = 'final550group4'
    db_database = 'hsi'

    connection = pymysql.connect(host=db_host,
                                 user=db_user,
                                 password=db_password,
                                 db=db_database,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    dbcursor = connection.cursor()
    query="""
    SELECT * FROM participant_info_table;"""
    
    out_df = pd.read_sql(query, connection)
    connection.close()
    return out_df
    


# In[5]:


participant_info_df = all_participant_info()


# In[ ]:





# In[ ]:





# In[6]:


def get_stock_value(company_name, start_date, end_date):
    db_host = 'finalprojectgroup4-550.c5zurnhs7may.us-east-1.rds.amazonaws.com'
    db_user = 'final550group4'
    db_password = 'final550group4'
    db_database = 'hsi'

    connection = pymysql.connect(host=db_host,
                                 user=db_user,
                                 password=db_password,
                                 db=db_database,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    dbcursor = connection.cursor()
    query = """
        WITH sit AS(select sit.code
        from stock_info_table as sit
        where sit.company_name = '{}')
        SELECT svt.code, svt.date, svt.high, svt.low, svt.open, svt.close
        FROM sit 
        INNER JOIN stock_value_table svt ON sit.code=svt.code
        WHERE svt.date BETWEEN '{}' AND '{}';
    """.format(company_name, start_date, end_date)
    
    df = pd.read_sql(query,connection)
    
    
    fig = go.Figure(
        data=[
                go.Candlestick(
                    x=df['date'],
                    open=df['open'], 
                    high=df['high'],
                    low=df['low'], 
                    close=df['close'],
                    name=company_name + " Value"
                )
             ],
        layout = go.Layout(
            autosize=True,
            title=company_name,
            yaxis_title='HK Dollar',
            xaxis_title='Date',
            template="plotly_white",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        ))

    
    query_10days_moving_avg = """
        WITH sit AS(select sit.code
        from stock_info_table as sit
        where sit.company_name = '{}')
        SELECT svt.code, svt.date, AVG(svt.close) OVER (ORDER BY svt.date DESC RANGE INTERVAL 10 DAY PRECEDING) AS 10d_moving
        FROM sit 
        INNER JOIN stock_value_table svt ON sit.code=svt.code
        WHERE svt.date BETWEEN '{}' AND '{}';
    """.format(company_name, start_date, end_date)
    
    
    
    
    df_10days_moving_avg = pd.read_sql(query_10days_moving_avg,connection)
    
    
    
    fig.add_trace(
            go.Scattergl(
                x = df_10days_moving_avg['date'],
                y = df_10days_moving_avg['10d_moving'],
                mode = 'lines',
                fillcolor="red",
                hoverinfo='skip',
                marker = dict(
                    line = dict(width = 0.5),
                    color="royalblue"
                ),
                    name="10Days Moving Average",
            )
        )
    
    connection.close()
    return df,fig


# In[7]:


df,fig = get_stock_value('Hong Kong and China Gas Company Limited',datetime.date(2019, 1, 1), datetime.date(2020, 2, 1))


# In[8]:


fig


# In[ ]:





# In[9]:


def get_stock_company_description(company_name):
    db_host = 'finalprojectgroup4-550.c5zurnhs7may.us-east-1.rds.amazonaws.com'
    db_user = 'final550group4'
    db_password = 'final550group4'
    db_database = 'hsi'

    connection = pymysql.connect(host=db_host,
                                 user=db_user,
                                 password=db_password,
                                 db=db_database,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    dbcursor = connection.cursor()
    query = """
        SELECT sit.company_description
        FROM stock_info_table sit
        WHERE sit.company_name='{}'
    """.format(company_name)
    dbcursor.execute(query)
    connection.close()
    return dbcursor.fetchone()['company_description']


# In[10]:


temp = get_stock_company_description('Hong Kong and China Gas Company Limited')


# In[11]:


def get_participant_amount(company_name, start_date, end_date):
    db_host = 'finalprojectgroup4-550.c5zurnhs7may.us-east-1.rds.amazonaws.com'
    db_user = 'final550group4'
    db_password = 'final550group4'
    db_database = 'hsi'
    connection = pymysql.connect(host=db_host,
                                 user=db_user,
                                 password=db_password,
                                 db=db_database,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    dbcursor = connection.cursor()
    
    query = """
        WITH sit AS(select sit.code
        from stock_info_table as sit
        where sit.company_name = '{}')
        SELECT pit.participant_name, pat.date, pat.amount
        FROM sit
        INNER JOIN participant_amount_table pat on sit.code=pat.code
        INNER JOIN participant_info_table pit on pit.participant_id = pat.participant_id
        WHERE pat.date BETWEEN '{}' AND '{}';
    """.format(company_name, start_date, end_date)

    
    out_df = pd.read_sql(query,connection)
    connection.close()
    amount_df = out_df.groupby(['date','participant_name']).first().unstack(level=-1)
    
    fig = go.Figure(layout = go.Layout(
            autosize=True,
            title=company_name,
            yaxis_title='Amount',
            xaxis_title='Date',
            template="plotly_white"
        ))
    for c in amount_df.columns:
        fig.add_trace(
            go.Scattergl(
                x = amount_df.index,
                y = amount_df[c],
                mode = 'lines',
                marker = dict(
                    line = dict(width = 1)
                ),
                name=c[1],
                text=c[1]
            )
        )

    
    
    return fig, out_df['participant_name'].unique().tolist()


# In[ ]:





# In[ ]:





# In[ ]:





# In[12]:


def generate_participant_FormGroup(participant_name,participant_address):
    return dbc.FormGroup(
                [
                    html.H3(f'Stakeholder Name/Company: {participant_name}'),
                    html.Br(),
                    html.H3(f'Stakeholder Address: {participant_address}')
                ]
            ),
    


# In[13]:


fig, amount_df= get_participant_amount('Hong Kong and China Gas Company Limited', datetime.date(2019, 1, 1), datetime.date(2020, 2, 1))


# In[ ]:





# In[14]:


stock_options = (hsi_code_name_df['company_name'].apply(lambda x:str(x))).tolist()
stock_options = [{'label':i, 'value':i}for i in stock_options]


# In[ ]:





# In[ ]:





# # Navigation Bar

# In[15]:


navbar = dbc.NavbarSimple(
    children=[
        dbc.NavItem(dbc.NavLink("Stock Candlestick Graph", href="/")),
        dbc.NavItem(dbc.NavLink("Stakeholder Graph", href="/stakeholder")),
        dbc.NavItem(dbc.NavLink("News Article", href="/newspage")),
        dbc.NavItem(dbc.NavLink("Southbound Analysis", href="/sba")),
        dbc.NavItem(dbc.NavLink("Winner Loser Tracker Page", href="/winnerloserpage")),
        

        
    ],
    brand="S-Penn",
    brand_href="/",
    color="primary",
    dark=True,
)


# # Control Panel

# In[16]:



controls = dbc.Card(
    [
        dbc.FormGroup(
            [
                html.H3("Hang Seng Index Stock"),
                
                html.Div(
                    children=dcc.Dropdown(
                        id="stock_company",
                        options=stock_options,style={'fontColor':'black'}
                    ),
                    
                
                
                )
                
            ]
        ),
        dbc.FormGroup(
            [
                html.H5("Please select a start date and end date"),
                dcc.DatePickerRange(
                    id='my_date_picker',
                    min_date_allowed=datetime.date(2019, 1, 2),
                    max_date_allowed=datetime.date(2020, 9, 30),
                    start_date=datetime.date(2019,1,2),
                    end_date = datetime.date(2020,9,30)
                )
            ]
        ),
    ],
    body=True,
)




# # Stock info

# In[17]:



stock_info = dbc.Card(
    [
        dbc.FormGroup(
            [
                html.H3("Description"),
                html.P(id='stock_company_description_id')
            ]
        ),
    ],
    body=True,
)



# # Stock Statistic Info

# In[18]:


def get_percentage_change(company_name, start_date, end_date):
    db_host = 'finalprojectgroup4-550.c5zurnhs7may.us-east-1.rds.amazonaws.com'
    db_user = 'final550group4'
    db_password = 'final550group4'
    db_database = 'hsi'

    connection = pymysql.connect(host=db_host,
                                 user=db_user,
                                 password=db_password,
                                 db=db_database,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    dbcursor = connection.cursor()
    
    
    query1 = """
        CREATE TEMPORARY TABLE temp_start
        WITH sit AS(select sit.code
        from stock_info_table as sit
        where sit.company_name = '{}')
        SELECT svt.date, svt.code, svt.close
        FROM sit 
        INNER JOIN stock_value_table svt ON sit.code=svt.code
        WHERE (svt.date = '{}')
        ;
    """.format(company_name, start_date)
    
    query2 = """
        CREATE TEMPORARY TABLE temp_end
        WITH sit AS(select sit.code
        from stock_info_table as sit
        where sit.company_name = '{}')
        SELECT svt.date, svt.code, svt.close
        FROM sit 
        INNER JOIN stock_value_table svt ON sit.code=svt.code
        WHERE (svt.date = '{}')
        ;
    """.format(company_name, end_date)
    
    query3 = """
        SELECT 100*(temp_end.close - temp_start.close)/temp_start.close AS per_change
        FROM temp_start, temp_end;
    """
    
    query4 = """
    DROP TEMPORARY TABLE temp_start;
    """
    
    query5 = """
    DROP TEMPORARY TABLE temp_end;
    """
    
    dbcursor.execute(query1)
    dbcursor.execute(query2)
    df = pd.read_sql(query3,connection)
    dbcursor.execute(query4)
    dbcursor.execute(query5)
    
    connection.close()
    return round(df['per_change'][0],3)


# In[19]:


def get_min(company_name, start_date, end_date):
    db_host = 'finalprojectgroup4-550.c5zurnhs7may.us-east-1.rds.amazonaws.com'
    db_user = 'final550group4'
    db_password = 'final550group4'
    db_database = 'hsi'

    connection = pymysql.connect(host=db_host,
                                 user=db_user,
                                 password=db_password,
                                 db=db_database,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    dbcursor = connection.cursor()
    query = """
        WITH sit AS(select sit.code
        from stock_info_table as sit
        where sit.company_name = '{}')
        SELECT svt.code, svt.date, MIN(svt.low) AS MIN
        FROM sit 
        INNER JOIN stock_value_table svt ON sit.code=svt.code
        WHERE svt.date BETWEEN '{}' AND '{}';
    """.format(company_name, start_date, end_date)
    
    df = pd.read_sql(query,connection)
    connection.close()
    return df['MIN'][0]


# In[20]:


def get_max(company_name, start_date, end_date):
    db_host = 'finalprojectgroup4-550.c5zurnhs7may.us-east-1.rds.amazonaws.com'
    db_user = 'final550group4'
    db_password = 'final550group4'
    db_database = 'hsi'

    connection = pymysql.connect(host=db_host,
                                 user=db_user,
                                 password=db_password,
                                 db=db_database,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    dbcursor = connection.cursor()
    query = """
        WITH sit AS(select sit.code
        from stock_info_table as sit
        where sit.company_name = '{}')
        SELECT svt.code, svt.date, MAX(svt.high) AS MAX
        FROM sit 
        INNER JOIN stock_value_table svt ON sit.code=svt.code
        WHERE svt.date BETWEEN '{}' AND '{}';
    """.format(company_name, start_date, end_date)
    
    df = pd.read_sql(query,connection)
    connection.close()
    
    return df['MAX'][0]


# In[21]:


def generate_stock_statistic_info(company_name, start_date, end_date):
    
    stock_max = get_max(company_name, start_date, end_date)
    stock_min = get_min(company_name, start_date, end_date)
    p_change  = get_percentage_change(company_name, start_date, end_date)
    
    
    stock_statistic = dbc.Card(
        [
            dbc.FormGroup(
                [
                    html.H2("Statistic"),
                    html.H5(f'Max:{stock_max}'),
                    html.H5(f'Min:{stock_min}'),
                    html.H5(f'Percentage Change:{p_change} %'),
                    html.Br(),
                    
                ]
            ),
        ],
        body=True,
    )
    return stock_statistic


# In[22]:


fig, amount_df= get_participant_amount('Hong Kong and China Gas Company Limited', datetime.date(2019, 1, 1), datetime.date(2020, 2, 1))


# In[ ]:





# In[23]:


def generate_ListGroup(title, time, url):
    return dbc.Card(
        dbc.CardBody(
            [
                html.H3(title),
                html.P(time),
                dbc.CardLink("Learn More", href=url)
            
            ]
        )
    )
    


# In[24]:


from pygooglenews import GoogleNews
gn = GoogleNews()
def generate_stock_news(company_name):
    
    result = gn.search(company_name)
    stock_news = []
    for i in range(0, len(result['entries'])):
        temp=[]
        temp.append(result['entries'][i]['title'])
        temp.append(result['entries'][i]['published'])
        temp.append(result['entries'][i]['link'])
        stock_news.append(temp)
        
    df = pd.DataFrame(stock_news, columns=['title','published','link'])
    df['published'] = df['published'].apply(lambda x:x[0:-4])
    df['published']=pd.to_datetime(df['published'], format='%a, %d %b %Y %X')
    df['published'] = df['published'].apply(lambda x:x.strftime('%Y-%m-%d'))
    df = df.sort_values(by='published',ascending=False)
    stock_news = df.values.tolist()
        
    
    
    
    
    return [generate_ListGroup(i[0],i[1],i[2]) for i in stock_news]


# In[ ]:





# In[25]:


def generate_participant_FormGroup(participant_name, participant_address):
    return dbc.Container(
                dbc.FormGroup(
                [
                    html.H5(f'Stakeholder Name/Company: {participant_name}'),
                    html.H5(f'Stakeholder Address: {participant_address}'),
                    html.Br(),
                ]
            ),
    
    
    )


# In[26]:


def generate_stakeholder_info(df):
    df = participant_info_df[participant_info_df['participant_name'].isin(df)]
    info = df[['participant_name','participant_address']].values.tolist()
    return dbc.Card([generate_participant_FormGroup(i[0],i[1]) for i in info], body=True)
    
    


# In[ ]:





# In[ ]:





# In[ ]:





# # Southbound Analysis

# In[27]:


def get_sba(company_name):
    db_host = 'finalprojectgroup4-550.c5zurnhs7may.us-east-1.rds.amazonaws.com'
    db_user = 'final550group4'
    db_password = 'final550group4'
    db_database = 'hsi'

    connection = pymysql.connect(host=db_host,
                                 user=db_user,
                                 password=db_password,
                                 db=db_database,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    dbcursor = connection.cursor()
    query = """
    SELECT 
        pit.participant_name AS Stakeholder,
        (t5.close * t2.amount / 7.75) AS current_position_usd,
        (t5.close * (t2.amount - t3.amount) / 7.75) AS position_change_1m,
        (t5.close * (t2.amount - t4.amount) / 7.75) AS position_change_6m
    FROM (
    SELECT distinct pat.participant_id, 
            pat.code,
            '2020-09-29' AS latest_date,
            DATE_SUB('2020-09-29', INTERVAL 20 DAY) AS one_months_ago,
            DATE_SUB('2020-09-29', INTERVAL 120 DAY) AS six_months_ago
    FROM 
        participant_amount_table AS pat
    WHERE 
        pat.code = (SELECT sit.code FROM stock_info_table AS sit WHERE sit.company_name = '{}')) AS t1
    INNER JOIN participant_amount_table AS t2 ON t2.participant_id = t1.participant_id AND t2.code = t1.code AND t2.date = t1.latest_date
    INNER JOIN participant_amount_table AS t3 ON t3.participant_id = t1.participant_id AND t3.code = t1.code AND t3.date = t1.one_months_ago
    INNER JOIN participant_amount_table AS t4 ON t4.participant_id = t1.participant_id AND t4.code = t1.code AND t4.date = t1.six_months_ago
    INNER JOIN stock_value_table AS t5 ON t5.code = t1.code AND t5.date = t1.latest_date
    INNER JOIN participant_info_table AS pit ON pit.participant_id = t1.participant_id
    WHERE t1.code = (SELECT sit.code FROM stock_info_table AS sit WHERE sit.company_name = '{}')
    ORDER BY position_change_1m DESC
    ;
    """.format(company_name, company_name)
    
    df = pd.read_sql(query,connection)
    connection.close()
    
    df.columns = ['Stakeholder', 'Current Amount (usd)', "1 Month Change", "6 Months Change"]
    
    
    
    return dbc.Table.from_dataframe(df, striped=True, bordered=True, hover=True)


# In[ ]:





# In[ ]:





# # Winner and Loser Tracker Page

# In[28]:


def get_winner_and_loser_tracker_page():
    db_host = 'finalprojectgroup4-550.c5zurnhs7may.us-east-1.rds.amazonaws.com'
    db_user = 'final550group4'
    db_password = 'final550group4'
    db_database = 'hsi'

    connection = pymysql.connect(host=db_host,
                                 user=db_user,
                                 password=db_password,
                                 db=db_database,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    dbcursor = connection.cursor()
    query = """
        WITH t1 AS (
        SELECT
        code,
        '2020-09-29' AS latest_date,
        DATE_SUB('2020-09-29', INTERVAL 20 DAY) AS one_months_ago,
        DATE_SUB('2020-09-29', INTERVAL 120 DAY) AS six_months_ago
        FROM hsi.stock_value_table
        GROUP BY code
        )
        SELECT
        t1.code,
            t5.company_name,
            ((t2.close / t3.close) - 1) AS percent_return_1m,
            ((t2.close / t4.close) - 1) AS percent_return_6m
        FROM t1
        LEFT JOIN hsi.stock_value_table t2 ON t2.code = t1.code AND t2.date = t1.latest_date
        LEFT JOIN hsi.stock_value_table t3 ON t3.code = t1.code AND t3.date = t1.one_months_ago
        LEFT JOIN hsi.stock_value_table t4 ON t4.code = t1.code AND t4.date = t1.six_months_ago
        LEFT JOIN hsi.stock_info_table t5 ON t5.code = t1.code
        ORDER BY percent_return_1m DESC;
    """
    
    df = pd.read_sql(query,connection)
    connection.close()
    
    df = df[['company_name','percent_return_1m','percent_return_6m']]
    
    df.columns = ["Company", "1 Month Percent Return", "6 Months Percent Return"]
    
    
    
    return dbc.Table.from_dataframe(df, striped=True, bordered=True, hover=True)


# In[ ]:





# In[ ]:





# # Frontend

# In[29]:


app = dash.Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = html.Div(
    children=[
    dcc.Location(id='url',refresh=False),
    navbar,
    dbc.Container(id='whole_page',children=[]),
    html.Div(id="sb_table"),
    dbc.Container([
        
        html.Br(),
        dbc.Row(
            [
                dbc.Col(controls,md=4),
                dbc.Col(id="graph_area",md=8)
            ]
        ),
    ],
    fluid=True),
    dbc.Container([
        html.Br(),
        dbc.Row(
            [
                dbc.Col(stock_info,md=4),
                dbc.Col(id='additional_info_area',
                        md=8),
            ],
        ),
    ],
    fluid=True)


])


# # Backend

# In[30]:


@app.callback([Output('graph_area','children'),
               Output('stock_company_description_id','children'),
               Output('additional_info_area','children')
               ],
             [Input('url','pathname'),
              Input('stock_company','value'),
              Input('my_date_picker','start_date'),
              Input('my_date_picker','end_date')])
def refresh(pathname, value, start_date, end_date):
    if(pathname=='/stakeholder'):
        fig, amount_df = get_participant_amount(value, start_date, end_date)
        return dcc.Graph(figure=fig), get_stock_company_description(value), generate_stakeholder_info(amount_df)
    elif(pathname=='/newspage'):
        return generate_stock_news(value), None, None
    elif(pathname=='/'):
        df, fig = get_stock_value(value, start_date, end_date)
        return dcc.Graph(figure=fig), get_stock_company_description(value), generate_stock_statistic_info(value, start_date, end_date)
    elif(pathname=='/sba'):
        return get_sba(value), None, None
    elif(pathname=='/winnerloserpage'):
        return get_winner_and_loser_tracker_page(), None, None


# In[ ]:





# In[ ]:





# In[31]:


app.run_server(debug=False)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




