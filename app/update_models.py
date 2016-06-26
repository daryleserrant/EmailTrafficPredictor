import pandas as pd
import numpy as np
import gmail_data_collection as gdc
import gmail_data_processing as gdp
import gmail_data_modeling as gdm
from datetime import datetime, timedelta
from statsmodels.tsa.statespace.sarimax import SARIMAX
import cPickle as pickle
import sys

def create_timeseries_data(messages):
    df = gdp.messages_to_dataframe(messages)
    
    if df == None:
        hourly_index = pd.date_range(last_updated.floor('H'),datetime.utcnow().replace(minute=0, second=0, microsecond=0), freq='H')
        hourly_counts = pd.Series(0, index=hourly_index)
        
        daily_index = pd.date_range(last_updated, datetime.utcnow(), freq='D')
        daily_counts = pd.Series(0, index=daily_index)
    else:
        # Remove all google hangout chat messages and messages that were sent by the user
        df = df[~df['is_sent'] & ~df['is_chat']]        
        
        hourly_counts = gdp.aggregate_mail_counts(df, by='hour')
        daily_counts = gdp.aggregate_mail_counts(df, by='day')
    
    return (daily_counts, hourly_counts)
    
def merge_timeseries_data(ts_A, ts_B):
    merged = ts_A[:]
    for dt,cnt in ts_B.iteritems():
        if dt in merged:
            merged[dt] = cnt
        else:
            entry = pd.Series([cnt], index=[dt])
            merged = merged.append(entry, verify_integrity=True)
    return merged

def load_training_data():
    daily_ts = pd.read_pickle('../data/daily_ts.pkl')[1:]
    hourly_ts = pd.read_pickle('../data/hourly_ts.pkl')[24:]
    
    return (daily_ts, hourly_ts)

def save_training_data(daily_ts, hourly_ts):
    # Save new data to files. 
    daily_ts.to_pickle('../data/daily_ts.pkl')
    hourly_ts.to_pickle('../data/hourly_ts.pkl')    

if __name__ == "__main__":
    weekly_model_file = 'weekly_model.pkl'
    hourly_model_file = 'hourly_model.pkl'
    
    daily_ts, hourly_ts = load_training_data()
    
    # Get the last date the models were updated
    last_updated = daily_ts.index.max().to_datetime()

    messages = gdc.collect_messages((datetime.utcnow(),last_updated))
    
    daily_counts, hourly_counts = create_timeseries_data(messages)
    
    # Merge the latest timeseries data into the training set.
    daily_ts = merge_timeseries_data(daily_ts, daily_counts)
    hourly_ts = merge_timeseries_data(hourly_ts, hourly_counts)
    
    # Update models
    weekly_model = gdm.build_weekly_arima_model(daily_ts)
    hourly_model = gdm.build_hourly_arima_model(hourly_ts)
    
    weekly_model.save(weekly_model_file)
    hourly_model.save(hourly_model_file)
    
    save_training_data(daily_ts, hourly_ts)