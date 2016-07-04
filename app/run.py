from flask import Flask
from flask_apscheduler import APScheduler
from flask import render_template
from gmail_traffic_forecaster import DailyForecaster, HourlyForecaster
from threading import Lock
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sbn
import numpy as np
from StringIO import StringIO
from matplotlib.dates import date2num
import logging
import math

import os
import sys


class Config(object):
    JOBS = [
        {
            'id': 'job_modelupate',
            'func': '__main__:check_for_updates',
            'trigger': 'interval',
            'seconds': 30
        }
    ]

    SCHEDULER_VIEWS_ENABLED = True

last_hr_mtime = 0
last_wk_mtime = 0

hourly_model_file = None
weekly_model_file = None

weekly_model = DailyForecaster()
hourly_model = HourlyForecaster()

mutex = Lock()

app = Flask(__name__)
app.config.from_object(Config())
app.debug = True

HOURLY_FORECAST_STEPS = 24
WEEKLY_FORECAST_STEPS = 7


def check_for_updates():
    '''
    Periodically poll the model pickle files for updates by examining the modified
    date field. Reload the models if the files have been updated.
    '''
    global last_hr_mtime, last_wk_mtime

    hr_mtime = os.stat(hourly_model_file).st_mtime
    wk_mtime = os.stat(weekly_model_file).st_mtime

    if hr_mtime != last_hr_mtime and wk_mtime != last_wk_mtime:
        # Wait for the lock to be available
        mutex.acquire()
        print "Reloading forecast models..."
        weekly_model.load(weekly_model_file)
        hourly_model.load(hourly_model_file)
        mutex.release()

        last_hr_mtime = hr_mtime
        last_wk_mtime = wk_mtime

scheduler = APScheduler()


@app.route('/')
@app.route('/index')
def index():
    if mutex.acquire(False):
        return render_template('index.html',
                               today_date=datetime.now().strftime('%A %B %d, %Y'))
    else:
        return "We are currently updating the forecast models. Please check back after a few minutes..."


def adjust_forecast(fc, which):
    '''
    Combines the forecasts produced by the hourly model and
    the weekly model in an effort to produce a more
    accurate forecast for today.

    Arguments:
       daily_fc - The day forecast produced by the weekly model
       hourly_fc - The 24 hour forecast provided by the hourly model

    Returns:
       The adjusted day and 24 hour forecast
    '''
    ret_val = 2

    if which == 'hour':
        daily_fc = weekly_model.forecast(1)[0]
        hourly_fc = fc
        ret_val = 0
    elif which == 'day':
        hourly_fc = hourly_model.forecast(HOURLY_FORECAST_STEPS)
        daily_fc = fc
        ret_val = 1

    hourly_total = hourly_fc.sum()
    hourly_dist = hourly_fc / float(hourly_total)
    avg = (daily_fc + hourly_total) / 2.
    hourly_dist = hourly_dist * avg
    adj_houry_fc = hourly_dist.apply(math.ceil)
    return (adj_houry_fc, adj_houry_fc.sum(), None)[ret_val]


@app.route('/wkly_plt.png')
def forecast_weekly_traffic():
    '''
    Creates a plot of the daily forecast for the next seven days
    '''
    plt.figure()
    fc = weekly_model.forecast(WEEKLY_FORECAST_STEPS)
    adjusted_day = adjust_forecast(fc[0], 'day')
    fc[0] = adjusted_day
    x_pos = date2num(fc.index.tolist())
    y_pos = fc.tolist()
    labels = [dt.to_datetime().strftime('%a') for dt in fc.index]
    plt.bar(x_pos, y_pos, alpha=0.5, align='center', color="#ff3333")
    plt.xticks(x_pos, labels)
    plt.tick_params(axis='x', labelsize=10)
    image = StringIO()
    plt.savefig(image, transparent=True)
    return image.getvalue(), 200, {'Content-Type': 'image/png'}


@app.route('/hrly_plt.png')
def forecast_hourly_traffic():
    '''
    Creates a plot of the hourly forecast for today
    '''
    fc = hourly_model.forecast(HOURLY_FORECAST_STEPS)
    fc = adjust_forecast(fc, 'hour')
    mutex.release()
    plt.figure(figsize=(15, 6))
    x_pos = date2num(fc.index.tolist())
    y_pos = fc.tolist()
    labels = [dt.to_datetime().strftime('%I%p') for dt in fc.index]
    plt.plot(x_pos, y_pos, color='#ff3333')
    plt.fill_between(x_pos, y_pos, alpha=0.6, color='#ff3333')
    plt.xticks(x_pos, labels)
    image = StringIO()
    plt.savefig(image, transparent=True)
    return image.getvalue(), 200, {'Content-Type': 'image/png'}

if __name__ == "__main__":
    '''
    Application Entry Point. Invoked by the terminal command: "python run.py <hourly model> <weekly model>"
        Arguments:
            weekly model - The path to the weekly model pickle file
            hourly model - The path to the hourly model pickle file
    '''

    weekly_model_file = sys.argv[1]
    hourly_model_file = sys.argv[2]

    last_hr_mtime = os.stat(hourly_model_file).st_mtime
    last_wk_mtime = os.stat(weekly_model_file).st_mtime

    weekly_model.load(weekly_model_file)
    hourly_model.load(hourly_model_file)

    scheduler.init_app(app)
    scheduler.start()

    app.run(host='0.0.0.0', port=8000)
