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
        '''
        Instantiate a new instance of Forecaster class
        '''
        pass

    def forecast(self, fc_steps):
        '''
        Returns a pandas series containing the forecast of the data
        fc_steps steps out

        Arguments:
          fc_steps: How many steps out the Forecaster should predict

        Returns:
          A pandas series containing the forecasts
        '''
        pass

    def load(self, filepath):
        '''
        Loads a model from a pickle file

        Arguments:
          filepath - Path to the pickle file containing the model
        '''
        pass


class DailyForecaster(Forecaster):
    '''
    A Forecaster subclass that forecasts the daily email traffic using the SARIMAXResults
    class from statsmodels
    '''

    def __init__(self, model=None):
        '''
        Instantiate a new instance of DailyForecaster class
        '''
        self.model = model

    def forecast(self, fc_steps):
        '''
        Returns a pandas series containing the forecast of the data
        fc_steps steps out

        Arguments:
          fc_steps: How many steps out the Forecaster should predict

        Returns:
          A pandas series containing the forecasts
        '''
        return self.model.forecast(steps=fc_steps)

    def load(self, filepath):
        '''
        Loads a model from a pickle file

        Arguments:
          filepath - Path to the pickle file containing the model
        '''
        self.model = SARIMAXResults.load(filepath)


class HourlyForecaster(Forecaster):
    '''
    A Forecaster subclass that forecasts the hourly email traffic using the holtwinters
    additive exponential smoothing algorithm.
    '''

    def __init__(self, alpha=None, beta=None, gamma=None, period=None, ts=None):
        '''
        Instantiate a new instance of the HourlyForecaster class

        Arguments:
          alpha - Holt winters alpha parameter
          beta - Holt winters beta parameter
          gamma- Holt winters gamma parameter
          period - The length of the seasonal period
          ts - Time series data to forecast
        '''
        self.alpha = alpha
        self.beta = beta
        self.gamma = gamma
        self.m = period
        self.ts = ts

    def update(self, alpha, beta, gamma, ts):
        '''
        Updates the DailyForecaster model

        Arguments:
          alpha - The new alpha parameter
          beta - The new beta parameter
          gamma- The new gamma parameter
          ts - Time series data to forecast
        '''
        self.alpha = alpha
        self.beta = beta
        self.gamma = gamma
        self.ts = ts

    def load(self, filepath):
        '''
        Loads a model from a pickle file

        Arguments:
          filepath - Path to the pickle file containing the model
        '''
        with open(filepath, 'r') as f:
            data = pickle.load(f)
            self.alpha = data[0]
            self.beta = data[1]
            self.gamma = data[2]
            self.m = data[3]
            self.ts = data[4]

    def forecast(self, fc_steps):
        '''
        Returns a pandas series containing the forecast of the data
        fc_steps steps out

        Arguments:
          fc_steps: How many steps out the Forecaster should predict

        Returns:
          A pandas series containing the forecasts
        '''
        results = hw.additive(self.ts.tolist(), self.m,
                              fc_steps, self.alpha, self.beta, self.gamma)
        start = self.ts.index.max()
        end = start + relativedelta.relativedelta(hours=fc_steps)
        date_index = pd.date_range(start, end, freq='H')
        return pd.Series(results[0], index=date_index[1:])
