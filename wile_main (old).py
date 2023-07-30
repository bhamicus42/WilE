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
import requests  # for accessing HTML-formatted and other web data
from os.path import join as osJoin  # for concatenating strings to make a URL
from os import chdir as osChdir  # to change directories, such as when working with data
import csv
from datetime import datetime  # to get dates

SYNOPTIC_API_TOKEN = "eb977b5f24ed48b585ccb4e520906425"
SYNOPTIC_API_ROOT = "https://api.synopticdata.com/v2/"
SYNOPTIC_FILTER = "stations/latest"  # TODO: refactor so that this is function argument
syn_api_req_url = osJoin(SYNOPTIC_API_ROOT, SYNOPTIC_FILTER) # URL to request synoptic data
syn_api_args = {"token": SYNOPTIC_API_TOKEN, "stid": "KLAX"}  # arguments to pass to the synoptic API
syn_req = requests.get(syn_api_req_url, params=syn_api_args)
syn_req_dict = syn_req.json()  # convert the synoptic request to a dictionary (despite the function name)
# TODO: cast this as a json obj from the json module, then get the keys of that obj

# TODO: decompose
now = datetime.now()  # get current datetime
now_str = now.strftime("%m.%d.%Y_%H.%M.%S")  # convert the datetime to a string
syn_req_csv_filename = "synoptic_request_csv_" + now_str + ".csv"  #

# write the synoptic request to a CSV file
osChdir(DATA_TMP_DIR)

# TODO: refactor to use Pandas
with open(syn_req_csv_filename, 'w', newline='') as syn_csv:
    syn_fieldnames = syn_req_dict[0].keys()
    # syn_fieldnames = syn_req_dict.keys()
    syn_writer = csv.DictWriter(syn_csv, fieldnames=syn_fieldnames)  # create writer object
    syn_writer.writeheader()  # write header to the csv file
    syn_writer.writerows(syn_req_dict)

osChdir(WDIR)
