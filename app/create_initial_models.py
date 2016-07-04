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
    df = gdp.messages_to_dataframe(messages, today)
    daily_counts = None
    hourly_counts = None

    if df is None:
        daily_counts = None
        hourly_counts = None
    else:
        # Remove all google hangout chat messages and messages that were sent
        # by the user
        if 'is_sent' in df:
            df = df[~df['is_sent']]
        if 'is_chat' in df:
            df = df[~df['is_chat']]

        hourly_counts = gdp.aggregate_mail_counts(
            df[df.index > (today - relativedelta(months=+6))], by='hour')
        daily_counts = gdp.aggregate_mail_counts(df, by='day')

        hourly_counts = gdp.fill_dates_between(hourly_counts, today, by='hour')
        daily_counts = gdp.fill_dates_between(hourly_counts, today, by='day')

    return (daily_counts, hourly_counts)


def save_training_data(daily_ts, hourly_ts):
    # Save new data to files.
    daily_ts.to_pickle('../data/daily_ts.pkl')
    hourly_ts.to_pickle('../data/hourly_ts.pkl')


if __name__ == "__main__":
    weekly_model_file = '../models/weekly_model.pkl'
    hourly_model_file = '../models/hourly_model.pkl'

    before = datetime.now(timezone('US/Pacific')
                          ).replace(hour=0, minute=0, second=0, microsecond=0)
    after = before - relativedelta(years=+2)

    messages = gdc.collect_messages((before, after))

    daily_ts, hourly_ts = create_timeseries_data(messages, before)

    if daily_ts is None or hourly_ts is None:
        print "No data to train models!"
    else:
        weekly_model = gdm.build_weekly_arima_model(daily_ts)
        hourly_model = gdm.build_hourly_holt_winters_model(hourly_ts)

        weekly_model.save(weekly_model_file)

        with open(hourly_model_file, 'w') as f:
            pickle.dump(hourly_model, f)

        save_training_data(daily_ts, hourly_ts)
