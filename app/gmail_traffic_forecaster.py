import pandas as pd
import numpy as np
import cPickle as pickle
from statsmodels.tsa.statespace.sarimax import SARIMAX, SARIMAXResults

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