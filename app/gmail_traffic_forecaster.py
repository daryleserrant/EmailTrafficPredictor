import pandas as pd
import numpy as np
import cPickle as pickle
from statsmodels.tsa.statespace.sarimax import SARIMAX, SARIMAXResults
import holtwinters as hw
from datetime import datetime, timedelta
import dateutil.relativedelta as relativedelta

class Forecaster(object):
'''
A base time series forecasting class
'''
    
    def __init__(self):
        pass
    
    def forecast(self, fc_steps):
        pass
    
    def load(self, filepath):
        pass

class DailyForecaster(Forecaster):
'''
A Forecaster subclass that forecasts the daily email traffic using the SARIMAXResults
class from statsmodels
'''
    
    def __init__(self, model=None):
        self.model = model
    
    def forecast(self, fc_steps):
        return self.model.forecast(steps=fc_steps)
    
    def load(self, filepath):
        self.model = SARIMAXResults.load(filepath)
        
class HourlyForecaster(Forecaster):
'''
A Forecaster subclass that forecasts the hourly email traffic using the holtwinters
additive exponential smoothing algorithm.
'''
    def __init__(self, alpha=None, beta=None, gamma=None, period=None, ts=None):
        self.alpha = alpha
        self.beta = beta
        self.gamma = gamma
        self.m = period
        self.ts = ts
    
    def update(self, alpha, beta, gamma, ts):
        self.alpha = alpha
        self.beta = beta
        self.gamma = gamma
        self.ts = ts
    
    def load(self, filepath):
        with open(filepath, 'r') as f:
            data = pickle.load(f)
            self.alpha = data[0]
            self.beta = data[1]
            self.gamma = data[2]
            self.m = data[3]
            self.ts = data[4]
            
    def forecast(self, fc_steps):
        results = hw.additive(self.ts.tolist(), self.m, fc_steps, self.alpha, self.beta, self.gamma)
        start = self.ts.index.max()
        end = start + relativedelta.relativedelta(hours=fc_steps)
        date_index = pd.date_range(start,end, freq='H')
        return pd.Series(results[0], index=date_index[1:])
    