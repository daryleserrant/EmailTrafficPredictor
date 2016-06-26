import pandas as pd
import numpy as np
import gmail_data_collection as gdc
import gmail_processing as gdp
from datetime import datetime, timedelta
from statsmodels.tsa.statespace.sarimax import SARIMAX
import sys

def create_timeseries_data(messages):
    df = gdp.messages_to_dataframe(messages)
    
    if df == None:
        hourly_index = pd.date_range(last_updated.floor('H'),datetime.utcnow().floor('H'), freq='H')
        hourly_counts = pd.Series(0, index=hourly_index)
        
        daily_index = pd.date_range(last_updated, datetime.utcnow(), freq='D')
        daily_counts = pd.Series(0, index=daily_index)
    else:
        # Remove all google hangout chat messages and messages that were sent by the user
        df = df[~df['is_sent'] & ~df['is_chat']]        
        
        hourly_counts = gdp.aggregate_mail_counts(messages, by='hour')
        daily_counts = gdp.aggregate_mail_counts(messages, by='day')
    
    return (daily_counts, hourly_counts)

def load_training_data():
    with open('../data/daily_ts.pkl', 'r') as f:
        daily_ts = pickle.load(f)[1:]
        
    with open('../data/hourly_ts.pkl', 'r') as f:
        hourly_ts = pickle.load(f)[24:]
    
    return (daily_ts, hourly_ts)

def save_training_data(daily_ts, hourly_ts):
    # Save new data to files. 
    with open('../data/daily_ts.pkl', 'w') as f:
        pickle.dump(daily_ts,f)
    
    with open('../data/hourly_ts.pkl', 'w') as f:
        pickle.dump(hourly_ts,f)    

if __name__ == "__main__":
    weekly_model_file = 'weekly_model.pkl'
    hourly_model_file = 'hourly_model.pkl'
    
    daily_ts, hourly_ts = load_training_data()
    
    # Get the last date the models were updated
    last_updated = daily_ts.index.max().to_datetime()

    messages = gdc.collect_messages(last_updated)
    
    daily_counts, hourly_counts = create_timeseries_data(messages)
    
    # Append the recent data to the time series.
    daily_ts = pd.concat([daily_ts, daily_counts],axis=0)
    hourly_ts = pd.concat([hourly_ts, hourly_counts],axis=0)
    
    # Update models
    weekly_model = build_weekly_arima_model(daily_ts)
    hourly_model = build_hourly_arima_model(hourly_ts)
    
    weekly_model.save(weekly_model_file)
    hourly_model.save(hourly_model_file)
    
    save_training_data(daily_ts, hourly_ts)