import pandas as pd
import numpy as np
import gmail_data_collection as gdc
import gmail_processing as gdp
from operator import itemgetter
from multiprocessing import Pool
from functools import partial
from statsmodels.tsa.statespace.sarimax import SARIMAX
from gmail_traffic_forecaster import Forecaster

HOURLY_PERIOD = 24
WEEKLY_PERIOD = 7

'''{'aic': 13503.050049614645,
 'params': {'D': 1, 'P': 1, 'Q': 2, 'd': 1, 'p': 0, 'q': 10}}'''
DEFAULT_HOURLY_PARAMS = {'p':0,'d':1,'q':24,'P':1,'D':1,'Q':2}
DEFAULT_WEEKLY_PARAMS = {'p':3,'d':1,'q':6,'P':0,'D':1,'Q':0}

def test_stationarity(timeseries):
    '''
    Performs Dickey Fuller test for stationarity on the timeseries.
    Creates a plot of the data, along with the rolling mean and rolling standard deviation
    for visual verification.

    Arguments:
        data - time series data        
    '''

    #Determine rolling statistics
    rolmean = timeseries.rolling(window=7, center=False).mean()
    rolstd = timeseries.rolling(window=7, center=False).std()

    #Plot rolling statistics:
    fig = plt.figure(figsize=(12, 8))
    orig = plt.plot(timeseries, color='blue',label='Original')
    mean = plt.plot(rolmean, color='red', label='Rolling Mean')
    std = plt.plot(rolstd, color='black', label = 'Rolling Std')
    plt.legend(loc='best')
    plt.title('Rolling Mean & Standard Deviation')
    plt.show()
    
    dftest = adfuller(timeseries,autolag='AIC')
    dfoutput = pd.Series(dftest[0:4], index=['Test Statistic','p-value','#Lags Used','Number of Observations Used'])
    for key,value in dftest[4].items():
        dfoutput['Critical Value (%s)'%key] = value
    print dfoutput

def run_grid_search(data, params):
    '''
    Perform grid search on seasonal arima model
    
    Arguments:
        data - the time series data
        params - a list of SARIMAX parameters. (A list of lists)
    
    Returns:
        A dictionary with the optimal aic and parameters
    '''
    best_aic = np.inf
    best_params = None
    
    for pm in params:
        p,d,q,P,D,Q = tuple(pm)
        model = SARIMAX(data, order=(p,d,q), seasonal_order=(P,D,Q,HOURLY_PERIOD), simple_differencing=True,enforce_stationarity=False,
        enforce_invertibility=False).fit()
        print 'p:{},d:{},q:{},P:{},D:{},Q:{}, AIC:{}'.format(p,d,q,P,D,Q,model.aic)
        if model.aic < best_aic:
            best_aic = model.aic
            best_params = {'p':p,'d':d,'q':q, 'P':P,'D':D, 'Q':Q}

    return {'params':best_params,'aic':best_aic}

def parallel_grid_search_arima(ts, params):
    '''
    Perform parallel grid searches on arima model
    
    Arguments:
        ts - the time series
        params - a dictionary of SARIMAX parameters
    
    Returns:
        The parameters that produces most optimal aic 
    '''
    pool = Pool(processes=4)
    p_list = []
    best_aic = np.inf
    best_params = None
    
    partial_srch = partial(run_grid_search,ts)
    
    for lp in params['p']:
        for lq in params['q']:
            for ld in params['d']:
                for bp in params['P']:
                    for bd in params['D']:
                        for bq in params['Q']:
                            if bp == 0 and bq == 0:
                                continue
                            p_list.append([lp,ld,lq,bp,bd,bq])
    results = pool.map(partial_srch, [p_list])
    
    sorted_results = sorted(results, key=itemgetter('aic'))

    return sorted_results[0]
    
def build_hourly_arima_model(ts, params=None):
    '''
    Builds an arima model that forecast hourly email traffic.
    
    Arguments:
        ts - a time series
        params - parameters to pass to the SARIMAX training algorithms
    
    Returns:
        an arima model
    '''    
    if params == None:
       params = DEFAULT_HOURLY_PARAMS
    
    p = params['p']
    d = params['d']
    q = params['q']
    P = params['Q']
    D = params['D']
    Q = params['Q']
    
    return SARIMAX(train, order=(p,d,q), seasonal_order=(P,D,Q,HOURLY_PERIOD), simple_differencing=True,enforce_stationarity=False,
         enforce_invertibility=False).fit()

def build_weekly_arima_model(ts, params=None):
    '''
    Builds an arima model that forecast weekly email traffic.
    
    Arguments:
        ts - a time series
        params - parameters to pass to the SARIMAX training algorithms
    
    Returns:
        an arima model
    '''
    
    if params == None:
       params = DEFAULT_WEEKLY_PARAMS
    
    p = params['p']
    d = params['d']
    q = params['q']
    P = params['Q']
    D = params['D']
    Q = params['Q']
    
    return SARIMAX(train, order=(p,d,q), seasonal_order=(P,D,Q,WEEKLY_PERIOD)).fit()
    
def update_models(date, wkly_model, hrly_model):

    # Collect the latest messages from the inbox and convert it into a format we can work with
    messages = gdp.messages_to_dataframe(gdc.collect_messages(date))
    
    # Remove all google hangout chat messages and messages that were sent by the user
    messages = messages[~messages['is_sent'] & ~messages['is_chat']]
    
    # Retrieve daily and hourly time series data used to train the old models,
    # and trim off the oldest seasonal data. 
    
    # Todo: Instead of storing the data into a file, store data in MongoDB or postgres.
    # That way we don't need to get rid of any data.
    with open('../data/daily_ts.pkl', 'r') as f:
        daily_ts = pickle.load(f)[7:]
    
    with open('../data/hourly_ts.pkl', 'r') as f:
        hourly_ts = pickle.load(f)[24:]
    
    # Append the recent data to the time series.
    daily_ts = pd.concat([daily_ts, gdp.aggregate_mail_counts(messages, by='day')],axis=0)
    hourly_ts = pd.concat([hourly_ts, gdp.aggregate_mail_counts(messages, by='hour')],axis=0)
    
    # Update models
    wkly_model.update(build_weekly_arima_model(daily_ts))
    hrly_model.update(build_hourly_arima_model(hourly_ts))
    
    # Save new data to files. 
    with open('../data/daily_ts.pkl', 'w') as f:
        pickle.dump(daily_ts,f)
    
    with open('../data/hourly_ts.pkl', 'w') as f:
        pickle.dump(hourly_ts,f)
        
    print 'Model updated'
