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
from sys import getsizeof as sys_getsizeof
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


# TODO: change constants and other relevant objects to private
class wile:
    def __init__(self,
                 token,
                 syn_api_root="https://api.synopticdata.com/v2/",  # root URL for synoptic API requests
                 syn_time_format="%Y%m%d%H%M",  # format for time specifiers in synoptic API URLs
                 syn_rt_filter="stations/latest",  # filter to get real-time data in synoptic API requests
                 syn_resp_cols=["ELEVATION", "LONGITUDE", "QC_FLAGGED", "LATITUDE", "PERIOD_OF_RECORD.start",  # which normalized columns of the synoptic response to keep
                                "PERIOD_OF_RECORD.end",                                                        # TODO: find a better way to store this than as mutable default arg
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
                                "OBSERVATIONS.relative_humidity_value_1.value"],
                 auto_clean=True,  # whether to automatically clean data according to preprogrammed parameters
                 logger_level=20,
                 logname="output_log.txt",
                 logger_formatter_string="%(asctime)s:%(funcName)s:%(message)s",
                 delete_old_logs=True,
                 print_to_console=True):

        # set global constants for this class
        self.CALLER_DIR = sys_path[0]  # location of the file calling/running this code
        self.DATA_DIR = setup_new_dir(self.CALLER_DIR, "data")  # location of data for use
        self.DATA_RT_DIR = setup_new_dir(self.DATA_DIR, "rt")  # where to store "realtime" data, that is, the last
                                                               # available measurements for variables of interest
        self.DATA_HIST_DIR = setup_new_dir(self.DATA_DIR, "hist")  # where to store historical data sets
        self.DATA_DERIVED_DIR = setup_new_dir(self.DATA_DIR, "derived")  # where to store derived data sets
        self.DATA_TMP_DIR = setup_new_dir(self.DATA_DIR, "tmp")  # if a file needs to be temporarily created
                                                                          # before being removed, it lives here while
                                                                          # it exists.
        self.OUTPUT_DIR = setup_new_dir(self.DATA_DIR, "outputs")  # location of any output files
        self.SYNOPTIC_API_TOKEN = token
        self.SYNOPTIC_API_ROOT = syn_api_root  # this is unlikely to change anytime soon but I figured I should make it
                                               # easily changable just in case
        self.SYN_TIME_FORMAT = syn_time_format  # ditto
        self.SYNOPTIC_RT_FILTER = syn_rt_filter  # ditto x2

        # this const specifies which columns of the synoptic response to keep
        # TODO: make this a default that can be changed
        # TODO: consider finding a way to keep all observation data
        # TODO: check what the difference is between sea level pressure measurement 1 and 1d is, what air temp 1 and 2 is
        self.SYNOPTIC_RESPONSE_COLUMNS = syn_resp_cols

        self.AUTO_CLEAN = auto_clean

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
        self.logger.debug(self.CALLER_DIR)
        # references used for logging:
        # https://stackoverflow.com/questions/13733552/logger-configuration-to-log-to-file-and-self.logger.info-to-stdout
        # https://github.com/CoreyMSchafer/code_snippets/blob/master/Logging-Advanced/employee.py
        # https://www.youtube.com/watch?v=jxmzY9soFXg
        # https://docs.python.org/3/library/logging.html

        self.logger.info("beep beep settin up the tootle toot:\n" + "wile object instantiated")



    # def syn_format(self, syn_resp#, keep
    #                 ):
    #     # clean data
    #     # TODO: generalize variable names
    #     # TODO: need to be able to pass variable number of keeps
    #     # TODO: calculate dewpoint depression at each station using its measured data
    #     #       I expect that any station with needed data transmits dewpoint: in this
    #     #       case we need to extrapolate with what data we can lay hands on.
    #     #       https://iridl.ldeo.columbia.edu/dochelp/QA/Basic/dewpoint.html
    #     syn_df = pd.json_normalize(syn_resp['STATION'])
    #     if self.AUTO_CLEAN:
    #         syn_df = syn_df[syn_df.QC_FLAGGED != "TRUE"]  # this removes any row that was flagged for
    #                                                       # quality control
    #         syn_df = syn_df[self.SYNOPTIC_RESPONSE_COLUMNS]  # this removes all columns except the ones in
    #                                                          # SYNOPTIC_RESPONSE_COLUMNS
    #     return syn_df

    # def syn_format(self, syn_resp, keep1, keep2):
    #     # clean data
    #     # TODO: generalize variable names
    #     # TODO: calculate dewpoint depression at each station using its measured data
    #     #       I expect that any station with needed data transmits dewpoint: in this
    #     #       case we need to extrapolate with what data we can lay hands on.
    #     #       https://iridl.ldeo.columbia.edu/dochelp/QA/Basic/dewpoint.html
    #     syn_df = pd.json_normalize(syn_resp[keep1, keep2])
    #     if self.AUTO_CLEAN:
    #         syn_df = syn_df[syn_df.QC_FLAGGED != "TRUE"]  # this removes any row that was flagged for
    #         # quality control
    #         syn_df = syn_df[self.SYNOPTIC_RESPONSE_COLUMNS]  # this removes all columns except the ones in
    #         # SYNOPTIC_RESPONSE_COLUMNS
    #     return syn_df

    def pull_everything(self):
        # pull all data sources, including updating historical set
        self.logger.info("Pulling data from ALL built-in sources. Historical data being updated.\n",
                         "This may take several hours!")

    def pull_realtimet(self):
        # pull realtime data
        self.logger.debug("Pulling realtime data.")

    def pull_synoptic_rt(self, auto_clean=True, write=True):
        self.logger.info("Pulling latest synoptic weather data.")
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

        # syn_df = self.syn_format(syn_resp, 'STATION')

        syn_df = pd.json_normalize(syn_resp['STATION'])
        if self.AUTO_CLEAN:
            syn_df = syn_df[syn_df.QC_FLAGGED != "TRUE"]  # this removes any row that was flagged for
                                                          # quality control
            syn_df = syn_df[self.SYNOPTIC_RESPONSE_COLUMNS]  # this removes all columns except the ones in
                                                             # SYNOPTIC_RESPONSE_COLUMNS

        # write the synoptic request to a CSV file
        if write:
            # TODO: decompose object to CSV file process
            # TODO: decompose datetime retrieval and concatenation?
            # TODO: consider making syn_csv_filename a global const
            # now = datetime.now()  # get current datetime
            # now_str = now.strftime("%m.%d.%Y_%H.%M.%S")  # convert the datetime to a string
            # syn_csv_filename = "synoptic_request_csv_" + now_str + ".csv"  #
            syn_csv_filename = "synoptic_rt_request.csv"
            os.chdir(self.DATA_RT_DIR)
            syn_df.to_csv(syn_csv_filename)
            self.logger.info("Wrote latest synoptic data response to CSV file")

            if self.logger.level == 10:  # if set to debug, open the historic csv to be sure it was retrieved properly
                os.startfile(os.path.join(os.getcwd(), syn_csv_filename))
            os.chdir(self.CALLER_DIR)


    def pull_historic(self):
        # pull historic data
        self.logger.debug("pull_historic() was called")

    def pull_synoptic_hist(self):
        # TODO
        # pull synoptic data as far back as arg
        self.logger.debug("pulling synoptic timeseries data \n" +
                          "NOTE: this function isn't complete and doesn't work yet! \n" +
                          "WARNING: it may take up to several hours to fulfill a timeseries request!")

        SYNOPTIC_HIST_FILTER = "stations/timeseries"  # filter for timeseries data TODO: refactor so that this is function argument
        # SYN_HIST_START = "199001010000"  # earliest time to seek to is 1990/01/01, 00:00. Most data will be nowhere near that.
        SYN_HIST_START = "202308050000"

        # TODO: these might need to be function args
        syn_api_args = {"state": "CA", "units": "metric,speed|kph,pres|mb", "varsoperator": "or",
                        "vars": "air_temp,sea_level_pressure,relative_humidity,dew_point_temperature,soil_temp,precip_accum",
                        "token": self.SYNOPTIC_API_TOKEN}  # TODO: this might need to go someplace else
        syn_hist_end = datetime.now().strftime(self.SYN_TIME_FORMAT)  # string giving current datetime; pull up to the absolute most recent data
        syn_api_hist_req_url = os.path.join(self.SYNOPTIC_API_ROOT, SYNOPTIC_HIST_FILTER)  # URL to request synoptic data
        syn_hist_args = syn_api_args

        # Requesting all recorded timeseries for CA is too much at once. Instead, request timeseries in day-long chunks.
        # This uses the datetime module to create 2 datetime objects that will outline each chunk.
        chunk_range_end = syn_hist_end  # string; first end of range should be datetime.now()
        chunk_end_dt = datetime.strptime(chunk_range_end, self.SYN_TIME_FORMAT)  # datetime object giving the end of the chunk timerange
        chunk_end_dt -= timedelta(hours=1)  # run the chunk end dt (but not the string!) back 1 day
        chunk_range_start = chunk_end_dt.strftime(self.SYN_TIME_FORMAT)  # datetime object giving the start of the chunk timerange
        chunk_start_dt = datetime.strptime(chunk_range_start, self.SYN_TIME_FORMAT)  # string; 1 day before chunk_range_end

        start_dt = datetime.strptime(SYN_HIST_START, self.SYN_TIME_FORMAT)  # datetime giving the very earliest date to seek to
        range_delta = chunk_end_dt - start_dt  # time difference in complete target range
        min_delta = timedelta(hours=1)  # set minimum distance between the current chunk and the earliest date

        syn_hist_args["START"] = chunk_range_start
        syn_hist_args["END"] = chunk_range_end
        syn_resp = requests.get(syn_api_hist_req_url, params=syn_hist_args)
        syn_resp = syn_resp.json()
        # syn_hist_df = pd.json_normalize(syn_resp)  # ["STATION", "OBSERVATIONS"]
        # syn_hist_df = self.syn_format(syn_resp, "STATION", "OBSERVATIONS")
        # syn_hist_df = self.syn_format(syn_resp, "STATION")
        syn_hist_df = self.syn_format(syn_resp)
        chunk_df = syn_hist_df  # this will be used to store each chunk; initialize as a dataframe to save time

        # https://stackoverflow.com/a/70639094
        parser = pd.io.parsers.base_parser.ParserBase(
            {'usecols': None})  # this will be used to make sure every response column name is unique

        self.logger.debug("start={}, end={} \n\nbeginning while loop:".format(SYN_HIST_START, syn_hist_end))

        while range_delta > min_delta:

            self.logger.debug("iteration start: chunk start={}, chunk end={}".format(chunk_range_start,
                                                                                     chunk_range_end))

            # construct timeseries query
            syn_hist_args["START"] = chunk_range_start
            syn_hist_args["END"] = chunk_range_end
            syn_resp = requests.get(syn_api_hist_req_url, params=syn_hist_args)  # send query
            syn_resp = syn_resp.json()  # despite it being called json(), this returns a dict object from the requests module
            # chunk_df = self.syn_format(syn_resp, "STATION", "OBSERVATIONS")  # convert query for this chunk into a dataframe
            # chunk_df = self.syn_format(syn_resp, "STATION")  # convert query for this chunk into a dataframe
            chunk_df = self.syn_format(syn_resp)  # convert query for this chunk into a dataframe

            # if self.logger.level == 10:  # if set to debug print synoptic args
            #     for line in syn_hist_args:
            #         self.logger.debug(line)

            # merge this chunk into the main synoptic historic dataframe
            # implemented method to do so is by pandas's .concat, but this breaks when a DF has duplicate column names
            # so we need to first make duplicate columns names chunk_df unique ("deduplicate")
            for df in [syn_hist_df, chunk_df]:
                df.columns = parser._maybe_dedup_names(df.columns)

            # then concatenate the two dataframes
            # http: // pandas.pydata.org / pandas - docs / stable / reference / api / pandas.concat.html
            # https://stackoverflow.com/a/28097336
            # TODO: this appears to sometimes drop data or throw errors. Make more robust.
            pd.concat([syn_hist_df, chunk_df], axis=0, ignore_index=True)

            # update chunk boundaries and distance from next chunk to the earliest date
            chunk_range_end = chunk_range_start
            chunk_end_dt = datetime.strptime(chunk_range_end, self.SYN_TIME_FORMAT)  # temporary datetime object of the chunk range end
            chunk_end_dt -= timedelta(hours=1)  # run back one day
            chunk_range_start = chunk_end_dt.strftime(self.SYN_TIME_FORMAT)
            range_delta = chunk_end_dt - start_dt

            # self.logger.debug("iteration end: chunk start={}, chunk end={}".format(chunk_range_start,
            #                                                                        chunk_range_end))
            self.logger.debug("syn_hist_df is {}kb".format(sys_getsizeof(syn_hist_df)/1000))

        os.chdir(self.DATA_HIST_DIR)

        now = datetime.now()  # get current datetime
        now_str = now.strftime("%m.%d.%Y_%H.%M.%S")  # convert the datetime to a string
        syn_hist_filename = "synoptic_historical_retrieved_" + now_str + ".csv"
        syn_hist_df.to_csv(syn_hist_filename)

        self.logger.info("saved historical measurements to csv\n" +
                         "filename = {}\n".format(syn_hist_filename) +
                         "path = {}\n".format(os.getcwd()))
        
        if self.logger.level == 10:  # if set to debug, open the historic csv to be sure it was retrieved properly
            os.startfile(os.path.join(os.getcwd(), syn_hist_filename))

        os.chdir(self.CALLER_DIR)
