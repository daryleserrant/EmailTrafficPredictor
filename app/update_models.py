import pandas as pd
import numpy as np
import gmail_data_collection as gdc
import gmail_data_processing as gdp
import gmail_data_modeling as gdm
from datetime import datetime, timedelta
from statsmodels.tsa.statespace.sarimax import SARIMAX
from dateutil.relativedelta import relativedelta
import cPickle as pickle
from pytz import timezone
import sys

def create_timeseries_data(messages):
    '''
    Create daily and hourly time series data using the email messages returned from the GMAIL API
    
    Arguments:
        messages - a list of messages (implemented as dicts)
    Returns:
        A tuple containing the daily and hourly time series data
    '''
    df = gdp.messages_to_dataframe(messages)
    
    if df is None:
        hourly_index = pd.date_range(last_updated.floor('H'),datetime.now(timezone('US/Pacific')).replace(minute=0, second=0, microsecond=0), freq='H')
        hourly_counts = pd.Series(0, index=hourly_index)
        
        daily_index = pd.date_range(last_updated, datetime.now(timezone('US/Pacific')), freq='D')
        daily_counts = pd.Series(0, index=daily_index)
    else:
        # Remove all google hangout chat messages and messages that were sent by the user
        if 'is_sent' in df:
            df = df[~df['is_sent']]
        if 'is_chat' in df:
            df = df[~df['is_chat']]
        
        hourly_counts = gdp.aggregate_mail_counts(df, by='hour')
        daily_counts = gdp.aggregate_mail_counts(df, by='day')
    
    return (daily_counts, hourly_counts)
    
def merge_timeseries_data(ts_A, ts_B):
    '''
    Merge two time series objects together into one time series object.
    
    Arguments:
        ts_A, ts_B - Two time series objects
    Returns:
        Combination of both time series. Time series B merged into time series A.
        In the case of duplicate indicies, the data in time series A will be replaced
        with time series B data.
    '''
    merged = ts_A[:]
    for dt,cnt in ts_B.iteritems():
        if dt in merged:
            merged[dt] = cnt
        else:
            entry = pd.Series([cnt], index=[dt])
            merged = merged.append(entry, verify_integrity=True)
    return merged

def load_training_data():
    daily_ts = pd.read_pickle('../data/daily_ts.pkl')
    hourly_ts = pd.read_pickle('../data/hourly_ts.pkl')
    
    today = datetime.now(timezone('US/Pacific')).replace(hour=0,minute=0, second=0, microsecond=0)
    
    hourly_ts = hourly_ts[hourly_ts.index > (today - relativedelta(months=+6))]
    daily_ts = daily_ts[daily_ts.index > (today - relativedelta(years=+2))]
    
    return (daily_ts, hourly_ts)

def save_training_data(daily_ts, hourly_ts):
    # Save new data to files. 
    daily_ts.to_pickle('../data/daily_ts.pkl')
    hourly_ts.to_pickle('../data/hourly_ts.pkl')    

if __name__ == "__main__":
    weekly_model_file = '../models/weekly_model.pkl'
    hourly_model_file = '../models/hourly_model.pkl'
    
    daily_ts, hourly_ts = load_training_data()
    
    # Get the last date the models were updated
    last_updated = daily_ts.index.max().to_datetime()

    messages = gdc.collect_messages((datetime.now(timezone('US/Pacific')),last_updated))
    
    daily_counts, hourly_counts = create_timeseries_data(messages)
    
    # Merge the latest timeseries data into the training set.
    daily_ts = merge_timeseries_data(daily_ts, daily_counts)
    hourly_ts = merge_timeseries_data(hourly_ts, hourly_counts)
    
    # Update models
    weekly_model = gdm.build_weekly_arima_model(daily_ts)
    hourly_model = gdm.build_hourly_holt_winters_model(hourly_ts)
    
    weekly_model.save(weekly_model_file)
    
    with open(hourly_model_file,'w') as f:
        pickle.dump(hourly_model,f)
    
    save_training_data(daily_ts, hourly_ts)