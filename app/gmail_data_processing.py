import pandas as pd
import numpy as np
import cPickle as pickle
from datetime import datetime
import time
import pytz

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
        a pandas data frame, None if the message list provided is empty
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
    
    # Return None if the message list is empty
    if not messages:
        return None
    
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

def aggregate_mail_counts(df, by='hour'):
    '''
    Aggregates mail counts hourly or daily.
    
    Arguments:
        df - Pandas dataframe
        by - How to aggregate the mail counts        
    
    Returns:
        a timeseries object containing the aggregated counts
    '''
    if by == 'hour':
        return aggregate_hourly(df)
    elif by == 'day':
        return aggregate_daily(df)
    else:
        return None    
    
def aggregate_hourly(df):
    '''
    Aggregates mail counts hourly.
    
    Arguments:
        df - Pandas dataframe     
    
    Returns:
        a timeseries object containing the aggregated counts
    '''
    hourly_agg = df[['year','month','day','hour','msg_id']].groupby(['year','month','day','hour']).count()
    
    hourly_index = pd.date_range(df['date'].min().floor('H'), df['date'].max().floor('H'), freq='H')
    hourly_counts = pd.Series(0, index=hourly_index)
    
    for dt in hourly_index:
        try:
            hourly_counts[dt] = hourly_agg.ix[dt.year,dt.month,dt.day,dt.hour]
        except:
            hourly_counts[dt] = 0  
    return hourly_counts

def aggregate_daily(df):
    '''
    Aggregates mail counts daily.
    
    Arguments:
        df - Pandas dataframe     
    
    Returns:
        a timeseries object containing the aggregated counts
    '''            
    daily_agg = df[['year','month','day','msg_id']].groupby(['year','month','day']).count()
    
    daily_index = pd.date_range(df['date'].min(), df['date'].max(), freq='D', normalize=True)
    daily_counts = pd.Series(0, index=daily_index)
    
    for dt in daily_index:
        try:
            daily_counts[dt] = daily_agg.ix[dt.year,dt.month,dt.day]
        except:
            daily_counts[dt] = 0
    return daily_counts
            
if __name__ == '__main__':

    with open('threads.pkl', 'r') as f:
        threads = pickle.load(f)
    df = threads_to_dataframe(threads)
    df.to_csv('gmail_threads.csv', encoding='utf-8')
    
    with open('emails.pkl', 'r') as f:
        messages = pickle.load(f)
    
    df = messages_to_dataframe(messages)
    df.to_csv('gmail_messages.csv', encoding='utf-8')