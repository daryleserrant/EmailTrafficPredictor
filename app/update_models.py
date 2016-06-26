import pandas as pd
import numpy as np
import gmail_data_collection as gdc
import gmail_processing as gdp
from datetime import datetime, timedelta
from statsmodels.tsa.statespace.sarimax import SARIMAX
import sys

if __name__ == "__main__":
    weekly_model_file = 'weekly_model.pkl'
    hourly_model_file = 'hourly_model.pkl'
    
    with open('../data/daily_ts.pkl', 'r') as f:
        daily_ts = pickle.load(f)
    
    with open('../data/hourly_ts.pkl', 'r') as f:
        hourly_ts = pickle.load(f)

    messages = gdc.collect_messages(datetime.now() - timedelta(days=1))
    
    df = gdp.messages_to_dataframe(messages)
    
    # Remove all google hangout chat messages and messages that were sent by the user
    df = df[~df['is_sent'] & ~df['is_chat']]
    
    # Append the recent data to the time series.
    daily_ts = pd.concat([daily_ts, gdp.aggregate_mail_counts(messages, by='day')],axis=0)
    hourly_ts = pd.concat([hourly_ts, gdp.aggregate_mail_counts(messages, by='hour')],axis=0)
    
    # Update models
    weekly_model = build_weekly_arima_model(daily_ts)
    hourly_model = build_hourly_arima_model(hourly_ts)
    
    weekly_model.save(weekly_model_file)
    hourly_model.save(hourly_model_file)
    
    # Save new data to files. 
    with open('../data/daily_ts.pkl', 'w') as f:
        pickle.dump(daily_ts,f)
    
    with open('../data/hourly_ts.pkl', 'w') as f:
        pickle.dump(hourly_ts,f)