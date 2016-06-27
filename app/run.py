from flask import Flask
from flask_apscheduler import APScheduler
from gmail_traffic_forecaster import Forecaster
from threading import Lock
import logging

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

weekly_model = Forecaster()
hourly_model = Forecaster()

mutex = Lock()

app = Flask(__name__)
app.config.from_object(Config())
app.debug = True

HOURLY_FORECAST_STEPS = 24
WEEKLY_FORECAST_STEPS = 28

def check_for_updates():
    '''
    Periodically poll the model pickle files for updates by examining the modified
    date field. Reload the models if the files have been updated.
    '''
    global last_hr_mtime, last_wk_mtime
    
    hr_mtime = os.stat(hourly_model_file).st_mtime
    wk_mtime = os.stat(weekly_model_file).st_mtime
    
    if hr_mtime != last_hr_mtime and wk_mtime != last_wk_mtime:
    
        try:
            # Wait for the lock to be available
            while not mutex.acquire():
                pass
            print "Reloading forecast models..."
            weekly_model.load(weekly_model_file)
            hourly_model.load(hourly_model_file)
            mutex.release()
          
            last_hr_mtime = hr_mtime
            last_wk_mtime = wk_mtime
        except:
            print "Error reloading models!"

scheduler = APScheduler()

@app.route('/')
@app.route('/index')
def index():
    return 'Hello,World!'
    
@app.route('/weekly_forecast')
def forecast_weekly_traffic():
    if mutex.acquire(False):
        fc = weekly_model.forecast(WEEKLY_FORECAST_STEPS)
        mutex.release()
        return 'Weekly Forecast'
    else:
        return 'We are updating the forecast models. Check back after a few minutes...'

@app.route('/hourly_forecast')
def forecast_hourly_traffic():
    if mutex.acquire(False):
        fc = hourly_model.forecast(HOURLY_FORECAST_STEPS)
        mutex.release()
        return 'Hourly Forecast'
    else:
        return 'We are updating the forecast models. Check back after a few minutes...'

if __name__ == "__main__":
    '''
    Application Entry Point. Invoked by the terminal command: "python run.py <hourly model> <weekly model>"
        Arguments:
            hourly model - The path to the hourly model pickle file
            weekly model - The path to the weekly model pickle file
    '''
    
    weekly_model_file = sys.argv[1]
    hourly_model_file = sys.argv[2]
    
    last_hr_mtime = os.stat(hourly_model_file).st_mtime
    last_wk_mtime = os.stat(weekly_model_file).st_mtime
    
    weekly_model.load(weekly_model_file)
    hourly_model.load(hourly_model_file)
    
    scheduler.init_app(app)
    scheduler.start()
    
    app.run()