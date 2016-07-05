'''
Gmail Data Modeling Module

This module defines functions for building and validating
SARIMA and Holt Winters Time Series Models.

Author: Daryle J. Serrant
'''

import pandas as pd
import numpy as np
import gmail_data_collection as gdc
import gmail_data_processing as gdp
from operator import itemgetter
from multiprocessing import Pool
from functools import partial
from statsmodels.tsa.statespace.sarimax import SARIMAX
import holtwinters as hw

HOURLY_PERIOD = 24
WEEKLY_PERIOD = 7

DEFAULT_HOURLY_PARAMS = {'p': 0, 'd': 1, 'q': 10, 'P': 1, 'D': 1, 'Q': 2}
DEFAULT_WEEKLY_PARAMS = {'p': 3, 'd': 1, 'q': 6, 'P': 0, 'D': 1, 'Q': 0}


def test_stationarity(timeseries):
    '''
    Performs Dickey Fuller test for stationarity on the timeseries.
    Creates a plot of the data, along with the rolling mean and rolling standard deviation
    for visual verification.

    Arguments:
        data - time series data        
    '''

    # Determine rolling statistics
    rolmean = timeseries.rolling(window=7, center=False).mean()
    rolstd = timeseries.rolling(window=7, center=False).std()

    # Plot rolling statistics:
    fig = plt.figure(figsize=(12, 8))
    orig = plt.plot(timeseries, color='blue', label='Original')
    mean = plt.plot(rolmean, color='red', label='Rolling Mean')
    std = plt.plot(rolstd, color='black', label='Rolling Std')
    plt.legend(loc='best')
    plt.title('Rolling Mean & Standard Deviation')
    plt.show()

    dftest = adfuller(timeseries, autolag='AIC')
    dfoutput = pd.Series(dftest[0:4], index=[
                         'Test Statistic', 'p-value', '#Lags Used', 'Number of Observations Used'])
    for key, value in dftest[4].items():
        dfoutput['Critical Value (%s)' % key] = value
    print dfoutput


def run_grid_search(data, params):
    '''
    Perform grid search on seasonal arima model

    Arguments:
        data - the time series data
        params - a list of SARIMAX parameters. (A list of lists)

    Returns:
        A dictionary with the optimal aic and parameters
    '''
    best_aic = np.inf
    best_params = None

    for pm in params:
        p, d, q, P, D, Q = tuple(pm)
        model = SARIMAX(data, order=(p, d, q), seasonal_order=(P, D, Q, HOURLY_PERIOD), simple_differencing=True, enforce_stationarity=False,
                        enforce_invertibility=False).fit()
        print 'p:{},d:{},q:{},P:{},D:{},Q:{}, AIC:{}'.format(p, d, q, P, D, Q, model.aic)
        if model.aic < best_aic:
            best_aic = model.aic
            best_params = {'p': p, 'd': d, 'q': q, 'P': P, 'D': D, 'Q': Q}

    return {'params': best_params, 'aic': best_aic}


def parallel_grid_search_arima(ts, params):
    '''
    Perform parallel grid searches on arima model

    Arguments:
        ts - the time series
        params - a dictionary of SARIMAX parameters

    Returns:
        The parameters that produces most optimal aic 
    '''
    pool = Pool(processes=4)
    p_list = []
    best_aic = np.inf
    best_params = None

    partial_srch = partial(run_grid_search, ts)

    for lp in params['p']:
        for lq in params['q']:
            for ld in params['d']:
                for bp in params['P']:
                    for bd in params['D']:
                        for bq in params['Q']:
                            if bp == 0 and bq == 0:
                                continue
                            p_list.append([lp, ld, lq, bp, bd, bq])
    results = pool.map(partial_srch, [p_list])

    sorted_results = sorted(results, key=itemgetter('aic'))

    return sorted_results[0]


def build_hourly_arima_model(ts, params=None):
    '''
    Builds an arima model that forecast hourly email traffic.

    Arguments:
        ts - a time series
        params - parameters to pass to the SARIMAX training algorithms

    Returns:
        an arima model
    '''
    if params == None:
        params = DEFAULT_HOURLY_PARAMS

    p = params['p']
    d = params['d']
    q = params['q']
    P = params['Q']
    D = params['D']
    Q = params['Q']

    return SARIMAX(ts, order=(p, d, q), seasonal_order=(P, D, Q, HOURLY_PERIOD), simple_differencing=True).fit()


def build_hourly_holt_winters_model(ts):
    '''
    Builds an hourly additive holt winters model to forecast email traffic.

    Arguments:
        ts - a time series

    Returns:
        A tuple containing the alpha, beta, and gamma parameters returned from the algorithm along with the
        data used for tuning.
    '''
    additive_hw = hw.additive(ts.tolist(), HOURLY_PERIOD, 24)

    return (additive_hw[1], additive_hw[2], additive_hw[3], HOURLY_PERIOD, ts)


def build_weekly_arima_model(ts, params=None):
    '''
    Builds an arima model that forecast weekly email traffic.

    Arguments:
        ts - a time series
        params - parameters to pass to the SARIMAX training algorithms

    Returns:
        an arima model
    '''

    if params == None:
        params = DEFAULT_WEEKLY_PARAMS

    p = params['p']
    d = params['d']
    q = params['q']
    P = params['Q']
    D = params['D']
    Q = params['Q']

    return SARIMAX(ts, order=(p, d, q), seasonal_order=(P, D, Q, WEEKLY_PERIOD)).fit()
