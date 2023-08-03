# Main class file for the AlertCalifornia Wildfire probability Estimator (WilE).
# Last editor: Ben Hoffman
# Contact: blhoff97@gmail.com
# This file and its ancillaries use the following formatting conventions:
# GLOBAL_CONSTANTS intended to be used throughout one or multiple files use caps snakecase
# local_variables use lowercase snakecase
# classMembers use camelcase

# some efforts are made in this section to only import what is needed so as to reduce overhead
import requests  # for accessing HTML-formatted and other web data
import os  # for operating on logs
           # for concatenating strings to make a URL
           # for changing directories, such as when working with data
           # for getting current working directory
           # TODO: make the directory generation more robust: https://linuxize.com/post/python-get-change-current-working-directory/
import logging
from sys import stdout
from datetime import datetime, timedelta  # to mark files with the datetime their data was pulled and to iterate across time ranges
import pandas as pd



def setup_new_dir(base_dir, new_dir):
    """
    Creates a new directory under a base directory.
    :param base_dir:  string giving the full path to a base directory under which to create the new directory
    :param new_dir:  string giving the name of a new directory to place under base directory
    :return new_dir_path:  string giving the full path of the new directory
    """
    # Check if new_dir exists.  If it doesn't, create it
    new_dir_path = base_dir + new_dir

    os.chdir(base_dir)
    if not os.path.isdir(new_dir_path):
        os.mkdir(new_dir_path)

    return new_dir_path



class wile:
    def __init__(self,

                 logger_level=20,
                 logname="output_log.txt",
                 logger_formatter_string="%(asctime)s:%(funcName)s:%(message)s",
                 delete_old_logs=True,
                 print_to_console=True):

        self.script_dir = sys.path[0]  # location of the file calling/running this code
        self.work_dir = setup_new_dir(self.script_dir, "\\fastHost working folder")  # location of the .glm file to use
                                                                                     # as a template for the new .glm
        self.output_dir = setup_new_dir(self.work_dir, "\\outputs")  # location of any output files from the script,
                                                                     # including recorder.csv, .txt, and image files
        self.temporary_file_dir = setup_new_dir(self.work_dir, "\\temp")  # if a file needs to be temporarily created
                                                                          # before being removed, it lives here while
                                                                          # it exists.

        # logging shenanigans
        self.logname = logname  # name of the .txt file to save log output.
        # TODO: replace this such that each log gets now() in its title, and if folder size is too big old logs are deleted
        if delete_old_logs:
            if os.path.exists(self.output_dir + "\\" + self.logname):
                os.chdir(self.output_dir)
                os.remove(self.logname)
                os.chdir(self.work_dir)

        # logging levels are stored in the logging library as integer constants.
        # DEBUG = 10, INFO = 20, WARNING = 30, ERROR = 40, CRITICAL = 50
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logger_level)
        formatter = logging.Formatter(logger_formatter_string)  # TODO:  find link to documentation page for formatter strings and put here
        file_handler = logging.FileHandler(self.output_dir + "\\" + self.logname)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        if print_to_console:
            console_handler = logging.StreamHandler(stdout)
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
        # references used for logging:
        # https://stackoverflow.com/questions/13733552/logger-configuration-to-log-to-file-and-self.logger.info-to-stdout
        # https://github.com/CoreyMSchafer/code_snippets/blob/master/Logging-Advanced/employee.py
        # https://www.youtube.com/watch?v=jxmzY9soFXg
        # https://docs.python.org/3/library/logging.html

        self.logger.info("beep beep settin up the tootle toot: \n" + "wile object instantiated")

        # set global constants for this class
        # TODO: define these more elegantly
        WDIR = os.getcwd()  # current working directory
        print(WDIR)
        DATA_DIR = os.path.join(WDIR, "data")  # where to store data
        DATA_TMP_DIR = os.path.join(DATA_DIR, "tmp")  # where to store temporary and real-time data
        DATA_HIST_DIR = os.path.join(DATA_DIR, "historical")  # where to store historical data sets
        DATA_DERIVED_DIR = os.path.join(DATA_DIR, "derived")  # where to store derived data sets
        SYNOPTIC_API_TOKEN = "eb977b5f24ed48b585ccb4e520906425"  # https://api.synopticdata.com/v2/stations/metadata?&state=CA&sensorvars=1&complete=1&token=
        SYNOPTIC_API_ROOT = "https://api.synopticdata.com/v2/"
        SYN_TIME_FORMAT = "%Y%m%d%H%M"  # format for time specifiers in synoptic API URLs
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
        syn_api_rt_req_url = os.path.join(SYNOPTIC_API_ROOT, SYNOPTIC_RT_FILTER)  # URL to request synoptic data
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
    os.chdir(DATA_TMP_DIR)
    syn_df.to_csv(syn_csv_filename)
