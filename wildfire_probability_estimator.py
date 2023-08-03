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
from sys import path as sys_path
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
    new_dir_path = os.path.join(base_dir, new_dir)
    os.chdir(base_dir)
    if not os.path.isdir(new_dir_path):
        os.mkdir(new_dir_path)

    return new_dir_path



class wile:
    def __init__(self,
                 token,
                 logger_level=20,
                 logname="output_log.txt",
                 logger_formatter_string="%(asctime)s:%(funcName)s:%(message)s",
                 delete_old_logs=True,
                 print_to_console=True):

        # set global constants for this class
        self.CALLER_DIR = sys_path[0]  # location of the file calling/running this code
        self.DATA_DIR = setup_new_dir(self.script_dir, "data")  # location of data for use
        self.DATA_RT_DIR = setup_new_dir(self.DATA_DIR, "rt")  # where to store "realtime" data, that is, the last
                                                              # available measurements for variables of interest
        self.DATA_HIST_DIR = setup_new_dir(self.DATA_DIR, "hist")  # where to store historical data sets
        self.DATA_DERIVED_DIR = setup_new_dir(self.DATA_DIR, "derived")  # where to store derived data sets
        self.DATA_TMP_DIR = setup_new_dir(self.DATA_DIR, "tmp")  # if a file needs to be temporarily created
                                                                          # before being removed, it lives here while
                                                                          # it exists.
        self.OUTPUT_DIR = setup_new_dir(self.DATA_DIR, "outputs")  # location of any output files

       
        # self.CALLER_DIR = os.getcwd()  # current working directory
        self.logger.debug(self.CALLER_DIR)
        # self.DATA_DIR = os.path.join(self.CALLER_DIR, "data")  # where to store data
        # self.DATA_TMP_DIR = os.path.join(self.DATA_DIR, "tmp")  # where to store temporary and real-time data
        # self.DATA_HIST_DIR = os.path.join(self.DATA_DIR, "historical")  # where to store historical data sets
        # self.DATA_DERIVED_DIR = os.path.join(self.DATA_DIR, "derived")  # where to store derived data sets
        # TODO: I'm not sure if this is the best place
        self.SYNOPTIC_API_TOKEN = token
        self.SYNOPTIC_API_ROOT = "https://api.synopticdata.com/v2/"
        self.SYN_TIME_FORMAT = "%Y%m%d%H%M"  # format for time specifiers in synoptic API URLs
        self.SYNOPTIC_RT_FILTER = "stations/latest"  # filter for real-time data

                                                     # TODO: consider refactoring so that this is function argument
        # this const specifies which columns of the synoptic response to keep
        # TODO: make this a default that can be changed
        # TODO: consider finding a way to keep all observation data
        # TODO: check what the difference is between sea level pressure measurement 1 and 1d is, what air temp 1 and 2 is
        self.SYNOPTIC_RESPONSE_COLUMNS = ["ELEVATION", "LONGITUDE", "QC_FLAGGED", "LATITUDE", "PERIOD_OF_RECORD.start",
                                          "PERIOD_OF_RECORD.end",
                                          "OBSERVATIONS.air_temp_value_1.date_time",
                                          "OBSERVATIONS.air_temp_value_1.value",
                                          "OBSERVATIONS.air_temp_value_2.date_time",
                                          "OBSERVATIONS.air_temp_value_2.value",
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

        # logging shenanigans. This section sets up a logger for the wile object to keep track of what goes on.
        self.logname = logname  # name of the .txt file to save log output.
        # TODO: consider replacing this such that each log gets now() in its title, and if folder size is too big old
        #  logs are deleted
        if delete_old_logs:
            if os.path.exists(self.OUTPUT_DIR + "\\" + self.logname):
                os.chdir(self.OUTPUT_DIR)
                os.remove(self.logname)
                os.chdir(self.DATA_DIR)

        # logging levels are stored in the logging library as integer constants.
        # DEBUG = 10, INFO = 20, WARNING = 30, ERROR = 40, CRITICAL = 50
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logger_level)
        formatter = logging.Formatter(logger_formatter_string)  # TODO:  find link to documentation page for formatter strings and put here
        file_handler = logging.FileHandler(self.OUTPUT_DIR + "\\" + self.logname)
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

        self.logger.info("beep beep settin up the tootle toot:\n" + "wile object instantiated")

        def pull():
            # pull data from API system
            self.logger.debug("under construction")

            def everything():
                # pull all data sources, including updating historical set
                self.logger.info("Pulling data from ALL built-in sources. Historical data being updated.\n",
                                 "This may take awhile!")

            def realtimet():
                # pull realtime data
                self.logger.debug("Pulling realtime data.")

                def pull_synoptic(auto_clean=True, write=True):
                    self.logger.info("Pulling synoptic weather data.")
                    self.logger.debug("Auto_clean = {} and write = {}".format(auto_clean, write))

                    syn_api_rt_req_url = os.path.join(self.SYNOPTIC_API_ROOT, self.SYNOPTIC_RT_FILTER)  # URL to request synoptic data
                    # arguments to pass to the synoptic API
                    # TODO: either make two CSVs or two separate requests so that all vars needed to calc dewpoint dep are present together
                    # TODO: find out how to measure sustained wind speed. Var to get instantaneous is wind_speed
                    syn_api_args = {"state": "CA", "units": "metric,speed|kph,pres|mb", "varsoperator": "or",
                                    "vars": "air_temp,sea_level_pressure,relative_humidity,dew_point_temperature,soil_temp,precip_accum",
                                    "token": self.SYNOPTIC_API_TOKEN}

                    syn_resp = requests.get(syn_api_rt_req_url, params=syn_api_args)
                    syn_resp = syn_resp.json()  # despite it being called json(), this returns a dict object from the requests module
                    # syn_json = json.loads(syn_resp)  # convert the synoptic request to a JSON object from the json module

                    # clean data
                    # TODO: consider decomposing
                    # TODO: calculate dewpoint depression at each station using its measured data
                    #       I expect that any station with needed data transmits dewpoint: in this
                    #       case we need to extrapolate with what data we can lay hands on.
                    #       https://iridl.ldeo.columbia.edu/dochelp/QA/Basic/dewpoint.html
                    syn_df = pd.json_normalize(syn_resp['STATION'])
                    if auto_clean:
                        syn_df = syn_df[syn_df.QC_FLAGGED != "TRUE"]  # this removes any row that was flagged for quality control
                        syn_df = syn_df[self.SYNOPTIC_RESPONSE_COLUMNS]  # this removes all columns except the ones in SYNOPTIC_RESPONSE_COLUMNS

                    # write the synoptic request to a CSV file
                    if write:
                        # TODO: decompose object to CSV file process
                        # TODO: decompose datetime retrieval and concatenation?
                        # TODO: consider making syn_csv_filename a global const
                        # now = datetime.now()  # get current datetime
                        # now_str = now.strftime("%m.%d.%Y_%H.%M.%S")  # convert the datetime to a string
                        # syn_csv_filename = "synoptic_request_csv_" + now_str + ".csv"  #
                        syn_csv_filename = "synoptic_rt_request.csv"
                        os.chdir(DATA_TMP_DIR)
                        syn_df.to_csv(syn_csv_filename)

            def historic():
                # pull historic data
                self.logger.debug("under construction")

                def synoptic():
                    # pull synoptic data as far back as arg
                    self.logger.debug("under construction")
