import pandas as pd
import numpy as np
import cPickle as pickle
from statsmodels.tsa.statespace.sarimax import SARIMAX, SARIMAXResults

def Forecaster(object):
    
    def __init__(self, model=None):
        self.model = model
    
    def update(self, model):
        self.model = model
    
    def forecast(self, fc_steps):
        model.forecast(steps=fc_steps)
    
    def get_residuals(self):
        return model.resid
    
    def load(self, filepath):
        self.model = SARIMAXResults.load(filepath)
    
    def save(self, filepath):
        self.model.save(filepath)