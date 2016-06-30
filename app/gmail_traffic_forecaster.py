import pandas as pd
import numpy as np
import cPickle as pickle
from statsmodels.tsa.statespace.sarimax import SARIMAX, SARIMAXResults
import holtwinters as hw
from datetime import datetime, timedelta
import dateutil.relativedelta as relativedelta

class Forecaster(object):
    
    def __init__(self):
        self.model = None
    
    def update(self, model):
        self.model = model
    
    def forecast(self, fc_steps):
        return self.model.forecast(steps=fc_steps)
    
    def get_residuals(self):
        return self.model.resid
    
    def load(self, filepath):
        self.model = SARIMAXResults.load(filepath)
    
    def save(self, filepath):
        self.model.save(filepath)

class DailyForecaster(Forecaster):
    
    def __init__(self):
        self.model = None
    
    def update(self, model):
        self.model = model
    
    def forecast(self, fc_steps):
        return self.model.forecast(steps=fc_steps)
    
    def load(self, filepath):
        self.model = SARIMAXResults.load(filepath)
    
    def save(self, filepath):
        self.model.save(filepath)
        
class HourlyForecaster(Forecaster):

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
            self.gamma = data[3]
            self.m = data[4]
            self.ts = data[5]
            
    def forecast(self, fc_steps):
        results = hw.additive(self.ts.tolist(), m, fc_steps, self.alpha, self.beta, self.gamma)
        start = datetime.utcnow().replace(minute = 0, second=0, microsecond=0)
        end = start + relativedelta.relativedelta(hours=fc_steps)
        date_index = pd.date_range(start,end, freq='H')
        return pd.Series(results[0], index=date_index)
    