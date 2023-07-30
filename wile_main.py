# Main driver file for the AlertCalifornia Wildfire probability Estimator (WilE).None
# Last edited: 7/27/2023
# Last editor: Ben Hoffman
# Contact: blhoff97@gmail.com
# This file and its ancillaries use the following formatting conventions:
# GLOBAL_CONSTANTS intended to be used throughout one or multiple files use caps snakecase
# local_variables use lowercase snakecase
# classMembers use camelcase

# import pandas as pd # for data manipulation
import os  # TODO: make the directory generation more robust: https://linuxize.com/post/python-get-change-current-working-directory/
WDIR = os.getcwd()  # current working directory
DATA_DIR = os.path.join(WDIR, "data")  # where to store data
DATA_TMP_DIR = os.path.join(DATA_DIR, "tmp")  # where to store temporary data

# TODO: this section is WIP data accessing. Decompose.

# TODO
# WIFIRE historical sets

# Synoptic realtime weather data
import urllib.request as req # for accessing HTML-formatted and other web data
from os.path import join as osJoin  # for concatenating strings to make a URL
from os import chdir as osChdir  # to change directories, such as when working with data
import csv  # for handling API data
import json  # for handling API data
from datetime import datetime  # to mark files with the datetime their data was
import pandas as pd

SYNOPTIC_API_TOKEN = "eb977b5f24ed48b585ccb4e520906425"
SYNOPTIC_API_ROOT = "https://api.synopticdata.com/v2/"
SYNOPTIC_FILTER = "stations/latest"  # TODO: refactor so that this is function argument
syn_api_req_url = osJoin(SYNOPTIC_API_ROOT, SYNOPTIC_FILTER) # URL to request synoptic data
syn_api_args = {"token": SYNOPTIC_API_TOKEN, "stid": "KLAX"}  # arguments to pass to the synoptic API
syn_resp = req.urlopen(syn_api_req_url, syn_api_args)
syn_json = json.loads(syn_resp)  # convert the synoptic request to a JSON object
# syn_json = json.loads(req.urlopen(syn_api_req_url, syn_api_args))  # retrieve synoptic response and cast to JSON

# TODO: decompose
now = datetime.now()  # get current datetime
now_str = now.strftime("%m.%d.%Y_%H.%M.%S")  # convert the datetime to a string
syn_csv_filename = "synoptic_request_csv_" + now_str + ".csv"  #

# write the synoptic request to a CSV file
osChdir(DATA_TMP_DIR)

# TODO: refactor to use Pandas
# with open(syn_csv_filename, 'w', newline='') as syn_csv:
    # syn_fieldnames = syn_req_dict[0].keys()
    # syn_writer = csv.DictWriter(syn_csv, fieldnames=syn_fieldnames)  # create writer object
    # syn_writer.writeheader()  # write header to the csv file
    # syn_writer.writerows(syn_req_dict)
syn_df = pd.DataFrame(syn_json)
syn_df.to_csv(syn_csv_filename)

osChdir(WDIR)
