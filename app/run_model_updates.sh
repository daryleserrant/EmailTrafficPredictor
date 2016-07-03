#!/bin/sh
# This script runs update_models.py, a python program that updates the hourly
# and daily time series models using the latest data from gmail. Create a cron
# job to run this file on a daily basis.
cd /home/ubuntu/EmailTrafficPredictor/app
python update_models.py