# Main driver file for the AlertCalifornia Wildfire probability Estimator (WilE).None
# Last edited: 7/27/2023
# Last editor: Ben Hoffman
# Contact: blhoff97@gmail.com
# This file and its ancillaries use the following formatting conventions:
# GLOBAL_CONSTANTS intended to be used throughout one or multiple files use caps snakecase
# local_variables use lowercase snakecase
# classMembers use camelcase

import os  # TODO: make the directory generation more robust: https://linuxize.com/post/python-get-change-current-working-directory/

# set constants for the main file
WDIR = os.getcwd()  # current working directory
DATA_DIR = os.path.join(WDIR, "data")  # where to store data
DATA_TMP_DIR = os.path.join(DATA_DIR, "tmp")  # where to store temporary and real-time data
DATA_HIST_DIR = os.path.join(DATA_DIR, "historical")  # where to store historical data sets
DATA_DERIVED_DIR = os.path.join(DATA_DIR, "derived")  # where to store derived data sets



# TODO: this section is WIP data accessing. Decompose into separate modules.

# TODO
# WIFIRE historical sets

# Synoptic realtime weather data
import requests  # for accessing HTML-formatted and other web data
from os.path import join as osJoin  # for concatenating strings to make a URL
from os import chdir as osChdir  # to change directories, such as when working with data
from datetime import datetime  # to mark files with the datetime their data was pulled
import pandas as pd


# https://api.synopticdata.com/v2/stations/metadata?&state=CA&sensorvars=1&complete=1&token=
SYNOPTIC_API_TOKEN = "eb977b5f24ed48b585ccb4e520906425"
SYNOPTIC_API_ROOT = "https://api.synopticdata.com/v2/"
SYNOPTIC_RT_FILTER = "stations/latest"  # filter for real-time data TODO: refactor so that this is function argument
# this const specifies which columns of the synoptic response to keep
# TODO: make this a default that can be changed
# TODO: consider finding a way to keep all observation data
# TODO: check what the difference is between sea level pressure measurement 1 and 1d is, what air temp 1 and 2 is
SYNOPTIC_RESPONSE_COLUMNS = ["ELEVATION", "LONGITUDE", "QC_FLAGGED", "LATITUDE", "PERIOD_OF_RECORD.start",
                             "PERIOD_OF_RECORD.end",
                             "OBSERVATIONS.air_temp_value_1.date_time", "OBSERVATIONS.air_temp_value_1.value",
                             "OBSERVATIONS.air_temp_value_2.date_time", "OBSERVATIONS.air_temp_value_2.value",
                             "OBSERVATIONS.sea_level_pressure_value_1d.date_time",
                             "OBSERVATIONS.sea_level_pressure_value_1d.value",
                             "OBSERVATIONS.sea_level_pressure_value_1.date_time",
                             "OBSERVATIONS.sea_level_pressure_value_1.value",
                             "OBSERVATIONS.dew_point_temperature_value_1d.date_time",
                             "OBSERVATIONS.dew_point_temperature_value_1d.value",
                             "OBSERVATIONS.dew_point_temperature_value_1.date_time",
                             "OBSERVATIONS.dew_point_temperature_value_1.value",
                             "OBSERVATIONS.relative_humidity_value_1.date_time",
                             "OBSERVATIONS.relative_humidity_value_1.value"]
syn_api_rt_req_url = osJoin(SYNOPTIC_API_ROOT, SYNOPTIC_RT_FILTER)  # URL to request synoptic data
# arguments to pass to the synoptic API
# TODO: either make two CSVs or two separate requests so that all vars needed to calc dewpoint dep are present together
# TODO: find out how to measure sustained wind speed. Var to get instantaneous is wind_speed
syn_api_args = {"state": "CA", "units": "metric,speed|kph,pres|mb", "varsoperator": "or",
                   "vars": "air_temp,sea_level_pressure,relative_humidity,dew_point_temperature,soil_temp,precip_accum",
                   "token": SYNOPTIC_API_TOKEN}
syn_resp = requests.get(syn_api_rt_req_url, params=syn_api_args)
syn_resp = syn_resp.json()  # despite it being called json(), this returns a dict object from the requests module
# syn_json = json.loads(syn_resp)  # convert the synoptic request to a JSON object from the json module

# clean data
# TODO: consider decomposing
# TODO: calculate dewpoint depression at each station using its measured data
#       -expect that any station with needed data transmits dewpoint: in this
#       case we need to extrapolate with what data we can lay hands on.
#       https://iridl.ldeo.columbia.edu/dochelp/QA/Basic/dewpoint.html
syn_df = pd.json_normalize(syn_resp['STATION'])
# syn_dc = syn_df[syn_df.QC_FLAGGED != "TRUE"]  # this removes any row that was flagged for quality control
# syn_df = syn_df[SYNOPTIC_RESPONSE_COLUMNS]  # this removes all columns except the ones in SYNOPTIC_RESPONSE_COLUMNS

# write the synoptic request to a CSV file
# TODO: decompose object to CSV file process
# TODO: decompose datetime retrieval and concatenation?
# TODO: consider making syn_csv_filename a global const
# now = datetime.now()  # get current datetime
# now_str = now.strftime("%m.%d.%Y_%H.%M.%S")  # convert the datetime to a string
# syn_csv_filename = "synoptic_request_csv_" + now_str + ".csv"  #
syn_csv_filename = "synoptic_request.csv"
osChdir(DATA_TMP_DIR)
syn_df.to_csv(syn_csv_filename)

print("saved most recent measurements to csv")


# Synoptic historical data acquisition. Parameters here are not meant to be used by other objects since this is
# intended to run only on rare occasions.
# TODO: decompose this so that it is only run on explicit request.

SYNOPTIC_HIST_FILTER = "stations/timeseries"  # filter for timeseries data TODO: refactor so that this is function argument
SYN_HIST_START = "199001010000"  # earliest time to seek to is 1990/01/01, 00:00. Most data will be nowhere near that.
# this const specifies which columns of the synoptic response to keep
# TODO: make this a default that can be changed
# TODO: consider finding a way to keep all observation data
# TODO: check what the difference is between sea level pressure measurement 1 and 1d is, what air temp 1 and 2 is
SYNOPTIC_RESPONSE_COLUMNS = ["ELEVATION", "LONGITUDE", "QC_FLAGGED", "LATITUDE", "PERIOD_OF_RECORD.start",
                             "PERIOD_OF_RECORD.end",
                             "OBSERVATIONS.air_temp_value_1.date_time", "OBSERVATIONS.air_temp_value_1.value",
                             "OBSERVATIONS.air_temp_value_2.date_time", "OBSERVATIONS.air_temp_value_2.value",
                             "OBSERVATIONS.sea_level_pressure_value_1d.date_time",
                             "OBSERVATIONS.sea_level_pressure_value_1d.value",
                             "OBSERVATIONS.sea_level_pressure_value_1.date_time",
                             "OBSERVATIONS.sea_level_pressure_value_1.value",
                             "OBSERVATIONS.dew_point_temperature_value_1d.date_time",
                             "OBSERVATIONS.dew_point_temperature_value_1d.value",
                             "OBSERVATIONS.dew_point_temperature_value_1.date_time",
                             "OBSERVATIONS.dew_point_temperature_value_1.value",
                             "OBSERVATIONS.relative_humidity_value_1.date_time",
                             "OBSERVATIONS.relative_humidity_value_1.value"]
syn_hist_end = datetime.now().strftime("%Y%m%d%H%M")  # seek up to the absolute most recent data
syn_api_hist_req_url = osJoin(SYNOPTIC_API_ROOT, SYNOPTIC_HIST_FILTER)  # URL to request synoptic data
syn_hist_args = syn_api_args
syn_hist_args["START"] = SYN_HIST_START
syn_hist_args["END"] = syn_hist_end
syn_resp = requests.get(syn_api_hist_req_url, params=syn_hist_args)
syn_resp = syn_resp.json()  # despite it being called json(), this returns a dict object from the requests module

osChdir(DATA_HIST_DIR)
#
now = datetime.now()  # get current datetime
now_str = now.strftime("%m.%d.%Y_%H.%M.%S")  # convert the datetime to a string
syn_hist_filename = "synoptic_historical_retrieved_" + now_str + ".csv"
syn_hist_df = pd.json_normalize(syn_resp)  # ["STATION", "OBSERVATIONS"]
syn_hist_df.to_csv(syn_hist_filename)

print("saved historical measurements to csv")



osChdir(WDIR)
