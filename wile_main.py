# Main driver file for the AlertCalifornia Wildfire probability Estimator (WilE).
# Last editor: Ben Hoffman
# Contact: blhoff97@gmail.com
# This file and its ancillaries use the following formatting conventions:
# GLOBAL_CONSTANTS intended to be used throughout one or multiple files use caps snakecase
# local_variables use lowercase snakecase
# classAttributes use camelcase

import wildfire_probability_estimator



wpe = wildfire_probability_estimator.wile()  # instantiate class object

# TODO
# WIFIRE historical sets

# Synoptic realtime weather data


# Synoptic historical data acquisition. Parameters here are not meant to be used by other objects since this is
# intended to run only on rare occasions.
# TODO: decompose this so that it is only run on explicit request.

# these import statements are temporary and should be removed when this section is decomposed
from datetime import datetime, timedelta  # to mark files with the datetime their data was pulled and to iterate across time ranges
from os.path import join as osJoin  # for concatenating strings to make a URL
from os import chdir as osChdir  # to change directories, such as when working with data
import pandas as pd
import requests

SYNOPTIC_API_TOKEN = "eb977b5f24ed48b585ccb4e520906425"
SYNOPTIC_API_ROOT = "https://api.synopticdata.com/v2/"
SYN_TIME_FORMAT = "%Y%m%d%H%M"  # format for time specifiers in synoptic API URLs
syn_api_args = {"state": "CA", "units": "metric,speed|kph,pres|mb", "varsoperator": "or",
                   "vars": "air_temp,sea_level_pressure,relative_humidity,dew_point_temperature,soil_temp,precip_accum",
                   "token": SYNOPTIC_API_TOKEN}

SYNOPTIC_HIST_FILTER = "stations/timeseries"  # filter for timeseries data TODO: refactor so that this is function argument
# SYN_HIST_START = "199001010000"  # earliest time to seek to is 1990/01/01, 00:00. Most data will be nowhere near that.
SYN_HIST_START = "202001010000"  # earliest time to seek to is 2020/01/01, 00:00. Most data will be nowhere near that.
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
syn_hist_end = datetime.now().strftime(SYN_TIME_FORMAT)  # string giving current datetime; pull up to the absolute most recent data
syn_api_hist_req_url = osJoin(SYNOPTIC_API_ROOT, SYNOPTIC_HIST_FILTER)  # URL to request synoptic data
syn_hist_args = syn_api_args

osChdir(DATA_HIST_DIR)

# Requesting all recorded timeseries for CA is too much at once. Instead, request timeseries in day-long chunks.
# This uses the datetime module to create 2 datetime objects that will outline each chunk.
chunk_range_end = syn_hist_end  # string; first end of range should be datetime.now()
chunk_end_dt = datetime.strptime(chunk_range_end, SYN_TIME_FORMAT)  # datetime object giving the end of the chunk timerange
chunk_end_dt -= timedelta(days=1)  # run back 1 day
chunk_range_start = chunk_end_dt.strftime(SYN_TIME_FORMAT)  # datetime object giving the start of the chunk timerange
chunk_start_dt = datetime.strptime(chunk_range_start, SYN_TIME_FORMAT)  # string; 1 day before chunk_range_end

start_dt = datetime.strptime(SYN_HIST_START, SYN_TIME_FORMAT)  # datetime giving the very earliest date to seek to
range_delta = chunk_end_dt - start_dt  # time difference in complete target range
min_delta = timedelta(days=1)  # set minimum distance between the current chunk and the earliest date

syn_resp = requests.get(syn_api_hist_req_url, params=syn_hist_args)
syn_resp = syn_resp.json()
syn_hist_df = pd.json_normalize(syn_resp)  # ["STATION", "OBSERVATIONS"]
chunk_df = syn_hist_df  # this will be used to store each chunk; initialize as a dataframe to save time

# https://stackoverflow.com/a/70639094
parser = pd.io.parsers.base_parser.ParserBase({'usecols': None})  # this will be used to make sure every response column name is unique

while range_delta > min_delta:

    # construct timeseries query
    syn_hist_args["START"] = chunk_range_start
    syn_hist_args["END"] = chunk_range_end
    syn_resp = requests.get(syn_api_hist_req_url, params=syn_hist_args)  # send query
    syn_resp = syn_resp.json()  # despite it being called json(), this returns a dict object from the requests module
    chunk_df = pd.json_normalize(syn_resp)  # convert query for this chunk into a dataframe

    # merge this chunk into the main historic dataframe
    # implemented method to do so is by pandas's .concat, but this breaks when a DF has duplicate column names
    # so we need to first make duplicate columns names chunk_df unique
    for df in [syn_hist_df, chunk_df]:
        df.columns = parser._maybe_dedup_names(df.columns)

    # then concatenate the two dataframes
    # http: // pandas.pydata.org / pandas - docs / stable / reference / api / pandas.concat.html
    # https://stackoverflow.com/a/28097336
    # TODO: this appears to sometimes drop data or throw errors. Make more robust.
    pd.concat([syn_hist_df, chunk_df], axis=0, ignore_index=True)

    # update chunk boundaries and distance from next chunk to the earliest date
    chunk_range_end = chunk_range_start
    chunk_end_dt = datetime.strptime(chunk_range_end, SYN_TIME_FORMAT)  # temporary datetime object of the chunk range end
    chunk_end_dt -= timedelta(days=1)  # run back one day
    chunk_range_start = chunk_end_dt.strftime(SYN_TIME_FORMAT)
    range_delta = chunk_end_dt - start_dt

    # print("start = {}, \nend = {}\n\n".format(chunk_range_start, chunk_range_end))


now = datetime.now()  # get current datetime
now_str = now.strftime("%m.%d.%Y_%H.%M.%S")  # convert the datetime to a string
syn_hist_filename = "synoptic_historical_retrieved_" + now_str + ".csv"
syn_hist_df.to_csv(syn_hist_filename)

print("saved historical measurements to csv")



osChdir(WDIR)
