import pandas as pd
import numpy as np
import cPickle as pickle
from multiprocessing import Pool
from functools import partial
from statsmodels.tsa.statespace.sarimax import SARIMAX
from multiprocessing import Pool
from operator import itemgetter
from datetime import datetime
import pytz

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

    #Determing rolling statistics
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

def get_unique_labels(data):
    '''
    Gets the unique message labels
    
    Arguments:
        messages - a list of lists of message labels
    
    Returns:
        the unique list of message labels
    '''
    msg_labels = set()
    for lst in data:
        if type(lst) == list:
            for l in lst:
                msg_labels.add(l)
    return list(msg_labels)

def has_label(data, label):
    '''
    Checks if a message label is contained in each list of lists
    
    Arguments:
        messages - a list of lists of message labels
    
    Returns:
        a list of booleans
    '''
    results = []
    for lst in data:
        if lst:
            if label in lst:
                results.append(True)
            else:
                results.append(False)
        else:
            results.append(False)
    return results

def threads_to_dataframe(threads):
    '''
    Converts the list of threads obtained from the GMAIL API into a pandas data frame.
    
    Arguments:
        threads - a list of threads retrieved from the GMAIL API
    
    Returns:
        a pandas data frame
    '''
    history_id = []
    thread_id = []
    snippet = []
    
    for t in threads:
        history_id.append(t['historyId'])
        thread_id.append(t['id'])
        snippet.append(t['snippet'])
    
    df_threads = pd.DataFrame({'id':thread_id, 'snippet':snippet, 'history_id':history_id})
    
    return df_threads

def messages_to_dataframe(messages):
    '''
    Converts the list of messages obtained from the GMAIL API into a pandas dataframe.
    
    Arguments:
        threads - a list of messages retrieved from the GMAIL API
    
    Returns:
        a pandas data frame
    '''
    history_id = []
    msg_id = []
    internal_date = []
    label_ids = []
    payload_body = []
    payload_filename = []
    payload_headers = []
    payload_parts = []
    payload_part_id = []
    payload_mime_type = []
    size_estimate = []
    snippet = []
    thread_id = []
    
    for m in messages:
        # Check for NoneTypes and integers. The GMAIL API will return None for any message it could not
        # find in the user's inbox. 
        if  type(m) == dict:
            history_id.append(m['historyId'])
            msg_id.append(m['id'])
            
            internal_date.append(m['internalDate'])
            
            if 'labelIds' in m:
                label_ids.append(m['labelIds'])
            else:
                label_ids.append(None)
                
            payload_body.append(m['payload']['body'])
            
            if m['payload']['filename'] == '':
                payload_filename.append(None)
            else:
                payload_filename.append(m['payload']['filename'])
            
            if 'headers' in m['payload']:
                payload_headers.append(m['payload']['headers'])
            else:
                payload_headers.append(None)

            if 'parts' in m['payload']:
                payload_parts.append(m['payload']['parts'])
            else:
                payload_parts.append(None)

            if 'partId'in m['payload']:
                if m['payload']['partId'] == '':
                    payload_part_id.append(None)
                else:
                    payload_part_id.append(m['payload']['partId'])
            else:
                payload_part_id.append(None)

            payload_mime_type.append(m['payload']['mimeType'])
            size_estimate.append(m['sizeEstimate'])
            snippet.append(m['snippet'])
            thread_id.append(m['threadId'])
    
    df = pd.DataFrame(
    {
        'history_id': history_id,
        'msg_id': msg_id,
        'internal_date': internal_date,
        'label_ids' : label_ids,
        'payload_body' : payload_body,
        'payload_filename' : payload_filename,
        'payload_headers' : payload_headers,
        'payload_parts' : payload_parts,
        'payload_part_id' : payload_part_id,
        'payload_mime_type' : payload_mime_type,
        'size_estimate' : size_estimate,
        'snippet' : snippet,
        'thread_id' : thread_id
    })
    
    msg_labels = get_unique_labels(df['label_ids'])
    
    for lbl in msg_labels:
       df['is_'+lbl.lower()] = has_label(df['label_ids'], lbl)
    
    df['year'] = df['internal_date'].apply(lambda x: time.gmtime(int(x)/1000).tm_year)
    df['month'] = df['internal_date'].apply(lambda x: time.gmtime(int(x)/1000).tm_mon)
    df['day'] = df['internal_date'].apply(lambda x: time.gmtime(int(x)/1000).tm_mday)
    df['hour'] = df['internal_date'].apply(lambda x: time.gmtime(int(x)/1000).tm_hour)
    df['min'] = df['internal_date'].apply(lambda x: time.gmtime(int(x)/1000).tm_min)
    df['sec'] = df['internal_date'].apply(lambda x: time.gmtime(int(x)/1000).tm_sec)
    df['wday'] = df['internal_date'].apply(lambda x: time.gmtime(int(x)/1000).tm_wday)
    df['yday'] = df['internal_date'].apply(lambda x: time.gmtime(int(x)/1000).tm_yday)
    df['isdst'] = df['internal_date'].apply(lambda x: time.gmtime(int(x)/1000).tm_isdst)
    df['date'] = df['internal_date'].apply(lambda x: datetime.fromtimestamp(int(x)/1000,tz=pytz.utc))
    
    return df.drop('label_ids', axis=1)

def build_hourly_arima_model(ts, params=None):
    '''
    Builds an arima model that forecast hourly email traffic.
    
    Arguments:
        ts - a time series
        params - parameters to pass to the SARIMAX training algorithms
    
    Returns:
        an arima model
    '''
    hourly_agg = ts[['year','month','day','hour','msg_id']].groupby(['year','month','day','hour']).count()
    
    hourly_index = pd.date_range(rcvd_messages['date'].min().floor('H'), rcvd_messages['date'].max().floor('H'), freq='H')
    hourly_counts = pd.Series(0, index=hourly_index)
    
    for dt in hourly_index:
        try:
            hourly_counts[dt] = hourly_agg.ix[dt.year,dt.month,dt.day,dt.hour]
        except:
            hourly_counts[dt] = 0
    
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
    daily_agg = ts[['year','month','day','msg_id']].groupby(['year','month','day']).count()
    
    daily_index = pd.date_range(ts['date'].min(), ts['date'].max(), freq='D')
    daily_counts = pd.Series(0, index=daily_index)
    
    for dt in daily_index:
        try:
            daily_counts[dt] = daily_agg.ix[dt.year,dt.month,dt.day]
        except:
            daily_counts[dt] = 0
    
    if params == None:
       params = DEFAULT_WEEKLY_PARAMS
    
    p = params['p']
    d = params['d']
    q = params['q']
    P = params['Q']
    D = params['D']
    Q = params['Q']
    
    return SARIMAX(train, order=(p,d,q), seasonal_order=(P,D,Q,WEEKLY_PERIOD)).fit()
         
if __name__ == '__main__':

    with open('threads.pkl', 'r') as f:
        threads = pickle.load(f)
    df = threads_to_dataframe(threads)
    df.to_csv('gmail_threads.csv', encoding='utf-8')
    
    with open('emails.pkl', 'r') as f:
        messages = pickle.load(f)
    
    df = messages_to_dataframe(messages)
    df.to_csv('gmail_messages.csv', encoding='utf-8')