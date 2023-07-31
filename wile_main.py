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
DATA_TMP_DIR = os.path.join(DATA_DIR, "tmp")  # where to store temporary data



# TODO: this section is WIP data accessing. Decompose into separate modules.

# TODO
# WIFIRE historical sets

# Synoptic realtime weather data
import requests  # for accessing HTML-formatted and other web data
from os.path import join as osJoin  # for concatenating strings to make a URL
from os import chdir as osChdir  # to change directories, such as when working with data
# import csv  # for handling API data
# import json  # for handling API data
# from datetime import datetime  # to mark files with the datetime their data was
import pandas as pd


# https://api.synopticdata.com/v2/stations/metadata?&state=CA&sensorvars=1&complete=1&token=
SYNOPTIC_API_TOKEN = "eb977b5f24ed48b585ccb4e520906425"
SYNOPTIC_API_ROOT = "https://api.synopticdata.com/v2/"
SYNOPTIC_FILTER = "stations/latest"  # TODO: refactor so that this is function argument
SYNOPTIC_RESPONSE_COLUMNS = ["ELEVATION", "LONGITUDE", "QC_FLAGGED", "LATITUDE"]  # TODO: make this a default that can be changed
syn_api_req_url = osJoin(SYNOPTIC_API_ROOT, SYNOPTIC_FILTER) # URL to request synoptic data
syn_api_args = {"state": "CA", "units": "metric,speed|kph,pres|mb", "varsoperator": "or",
                "vars": "air_temp,sea_level_pressure",
                "token": SYNOPTIC_API_TOKEN}  # arguments to pass to the synoptic API
syn_resp = requests.get(syn_api_req_url, params=syn_api_args)
syn_resp = syn_resp.json()  # despite it being called json(), this returns a dict object
# syn_json = json.loads(syn_resp)  # convert the synoptic request to a JSON object
  # retrieve synoptic response and cast to JSON

# TODO: decompose
# now = datetime.now()  # get current datetime
# now_str = now.strftime("%m.%d.%Y_%H.%M.%S")  # convert the datetime to a string
# syn_csv_filename = "synoptic_request_csv_" + now_str + ".csv"  #
syn_csv_filename = "synoptic_request.csv"

# write the synoptic request to a CSV file
osChdir(DATA_TMP_DIR)

# syn_df = pd.DataFrame(syn_resp['STATION'])
syn_df = pd.json_normalize(syn_resp['STATION'])
syn_dc = syn_df[syn_df.QC_FLAGGED != "TRUE"]  # this removes any row that was flagged for quality control
syn_df = syn_df[SYNOPTIC_RESPONSE_COLUMNS]  # this removes all columns except the ones in SYNOPTIC_RESPONSE_COLUMNS
# TODO: filter any QC_FLAGGED = TRUE,
#       filter unneeded columns

# TODO: decompose
# with pd.option_context('display.max_rows', None,
#                        'display.max_columns', None,
#                        'display.precision', 3):
#     print(syn_df)

syn_df.to_csv(syn_csv_filename)

# df2 = pd.read_json()
df = pd.json_normalize(syn_resp['STATION'])
df.to_csv("test.csv")

osChdir(WDIR)
