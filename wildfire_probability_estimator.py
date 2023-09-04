# Main class file for the AlertCalifornia Wildfire probability Estimator (WilE).
# Last editor: Ben Hoffman
# Contact: blhoff97@gmail.com
# This file and its ancillaries use the following formatting conventions:
# GLOBAL_CONSTANTS intended to be used throughout one or multiple files use caps snakecase
# local_variables use lowercase snakecase
# classMembers use camelcase

import os  # for operating on logs
           # for concatenating strings to make a URL
           # for changing directories, such as when working with data
           # for getting current working directory
           # TODO: make the directory generation more robust: https://linuxize.com/post/python-get-change-current-working-directory/
import logging
import sys
from datetime import datetime, timedelta  # to mark files with the datetime their data was pulled and to iterate across time ranges
import pandas as pd

# additional modules needed for working with Earthdata LDAS
import json
import urllib3
import certifi
import requests
from time import sleep
from subprocess import Popen
import platform
import shutil
from requests.auth import HTTPBasicAuth # temporary attempt to resolve 401 error


def dir_return(d):
    """
    Checks if os.cwd is the desired dir. If so, return. If not, change to desired directory.
    :param d: string giving the complete path of the desired dir.
    :return: none
    """
    if os.getcwd() == d:
        return
    else:
        os.chdir(d)

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

def earthdata_setup_auth(auth,
                         dodsrc_dest,
                         urs='urs.Earthdata.nasa.gov',  # Earthdata URL to call for authentication
                         ):
    """
    Creates .netrc, .urs_cookies, and .dodsrc files in bash home directory; these files are prerequisite authenticators
    to access GES DISC data such as Earthdata's LDAS set
    :param dodsrc_dest: string giving path to destination of the .dodsrc file
    :param auth: dictionary giving authentication details; must have structure 'login':<login>, 'password':<password>
    :param urs: string giving URL to call for Earthdata authentication. Defaults to 'urs.Earthdata.nasa.gov'
    :return: none
    """
    auth_dir = os.path.expanduser("~") + os.sep

    with open(auth_dir + '.netrc', 'w') as file:
        file.write('machine {} login {} password {}'.format(urs,
                                                            auth['login'],
                                                            auth['password']))
        file.close()
    with open(auth_dir + '.urs_cookies', 'w') as file:
        file.write('')
        file.close()
    with open(auth_dir + '.dodsrc', 'w') as file:
        file.write('HTTP.COOKIEJAR={}.urs_cookies\n'.format(auth_dir))
        file.write('HTTP.NETRC={}.netrc'.format(auth_dir))
        file.close()

    # TODO:
    # print('Saved .netrc, .urs_cookies, and .dodsrc to:', auth_dir)

    # Set appropriate permissions for Linux/macOS
    if platform.system() != "Windows":
        Popen('chmod og-rw ~/.netrc', shell=True)
    else:
        # Copy dodsrc to working directory in Windows
        shutil.copy2(auth_dir + '.dodsrc', dodsrc_dest)
        # print('Copied .dodsrc to:', dodsrc_dest)

def get_dict_from_file(d, fname):
    """
    retrieves a dictionary object from a text file; assumes key and value pairs are separated by ',' and each pair is
    on its own line
    :param d: string giving the directory to find the file in
    :param fname: string giving the filename of the text file
    :return: data, a dictionary constructed from the data in the text file
    """
    # TODO: make dir optional (which defaults to working in cwd) and give fname a default
    # TODO: add error handling
    # https://www.quora.com/How-do-you-convert-a-text-file-into-a-dictionary-Python-syntax-file-dictionary-object-methods-and-development
    caller_dir = os.getcwd()    # get caller dir to return to after data is pulled
    os.chdir(d)   # go to target directory

    data = {}  # Create an empty dictionary to put the data in

    with open(fname, 'r') as f:
        lines = f.readlines()   # Read the contents of the file into a list

        for line in lines:  # Loop through the list of lines
            key, value = line.strip().split(',')    # Split the line into key-value pairs
            data[key] = value   # Store the key-value pairs in the dictionary

        # The dictionary 'data' now contains the contents of the text file

    os.chdir(caller_dir)    # return to caller dir

    return data



class wile:
    # TODO: change constants and other relevant objects to private
    # Current data sources:
    # Synoptic MesoNet: weather station data
    # GES DISC Earthdata: satellite data sets such as Earthdata's LDAS. NOTE: I never figured out what exactly the terms
    #                     Earthdata and GES DISC refer to; they're used in very similar contexts and almost used
    #                     interchangeably. I might misuses the terms in this documentation.
    def __init__(self,
                 syn_token,
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
                 gesdisc_auth_setup_flag=False,
                 gesdisc_auth_path='C:\\Users\\arche\\WilE certs\\Earthdata',
                 gesdisc_auth_fname='login.txt',
                 auto_clean=True,  # whether to automatically clean data according to preprogrammed parameters
                 logger_level=20,
                 logname="output_log.txt",
                 logger_formatter_string="%(asctime)s:%(funcName)s:%(message)s",
                 delete_old_logs=True,
                 print_to_console=True):

        # set global constants for this class
        self.CALLER_DIR = sys.path[0]  # location of the file calling/running this code
        self.DATA_DIR = setup_new_dir(self.CALLER_DIR, "data")  # location of data for use
        self.DATA_RT_DIR = setup_new_dir(self.DATA_DIR, "rt")  # where to store "realtime" data, that is, the last
                                                               # available measurements for variables of interest
        self.DATA_HIST_DIR = setup_new_dir(self.DATA_DIR, "hist")  # where to store historical data sets
        self.DATA_DERIVED_DIR = setup_new_dir(self.DATA_DIR, "derived")  # where to store derived data sets
        self.DATA_TMP_DIR = setup_new_dir(self.DATA_DIR, "tmp")  # if a file needs to be temporarily created
                                                                          # before being removed, it lives here while
                                                                          # it exists.
        self.DEBUG_DIR = setup_new_dir(self.CALLER_DIR, "debug")  # if any files are necessary for debugging purposes,
                                                                  # they'll be placed here
                                                                  # TODO: automatically clean this folder
        self.OUTPUT_DIR = setup_new_dir(self.CALLER_DIR, "outputs")  # location of any output files
        self.SYNOPTIC_API_TOKEN = syn_token
        self.SYNOPTIC_API_ROOT = syn_api_root  # this is unlikely to change anytime soon but I figured I should make it
                                               # easily changable just in case
        self.SYN_TIME_FORMAT = syn_time_format  # ditto
        self.SYNOPTIC_RT_FILTER = syn_rt_filter  # ditto x2

        # this const specifies which columns of the synoptic response to keep
        # TODO: make this a default that can be changed
        # TODO: consider finding a way to keep all observation data
        # TODO: check what the difference is between sea level pressure measurement 1 and 1d is, what air temp 1 and 2 is
        self.SYNOPTIC_RESPONSE_COLUMNS = syn_resp_cols

        self.GES_DISC_AUTH_SETUP_FLAG = gesdisc_auth_setup_flag  # whether or not the GES DISC authentication files
                                                                 # have been setup yet. These are needed to access any
                                                                 # GES DISC data like LDAS
        self.GES_DISC_AUTH_PATH = gesdisc_auth_path
        self.GES_DISC_AUTH_FNAME = gesdisc_auth_fname

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
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
        self.logger.debug(self.CALLER_DIR)
        # references used for logging:
        # https://stackoverflow.com/questions/13733552/logger-configuration-to-log-to-file-and-self.logger.info-to-stdout
        # https://github.com/CoreyMSchafer/code_snippets/blob/master/Logging-Advanced/employee.py
        # https://www.youtube.com/watch?v=jxmzY9soFXg
        # https://docs.python.org/3/library/logging.html

        self.logger.info("beep beep settin up the tootle toot:\n" + "wile object instantiated")

    def __del__(self):
        self.logger.info("beep boop, takin down the tootle toot")

        # delete GES DISC authentication files

        # delete dodsrc that was copied to caller dir
        os.chdir(self.CALLER_DIR)

        os.chdir(os.path.expanduser("~") + os.sep)  # go to bash root where auth files were stored
        # delete GES DISC authentication files if they exist
        os.chdir(self.CALLER_DIR)  # return to whence we came

    def gesdisc_get_http_data(self, request, http, svcurl):
        """
        POSTs formatted JSON WSP requests to the GES DISC endpoint URL and returns the response
        :param request: JSON object giving a WSP request for a subset of data
        :param http: URLLIB3 PoolManager object
        :param svcurl: string giving the URL for the GES DISC subset service endpoint
        :return: response JSON object giving API response to the request object
        """
        hdrs = urllib3.make_headers(basic_auth='Eshreth_of_Athshe:SONOlu4mi__._ne8scence')
        hdrs['Content-Type'] = 'application/json'
        hdrs['Accept'] = 'application/json'
        # hdrs = {'Content-Type': 'application/json',
        #         'Accept': 'application/json'}
        data = json.dumps(request)
        r = http.request('POST', svcurl, body=data, headers=hdrs)
        response = json.loads(r.data)

        # Check for errors
        # TODO: update with more robust error handling
        if response['type'] == 'jsonwsp/fault':
            if self.logger.level == 10:  # if logger set to DEBUG give full detail of error
                self.logger.error('API Error: faulty request', stack_info=True, exc_info=True)
            else:  # otherwise just note that an error happened
                self.logger.error('API Error: faulty request')
        return response

    def gesdisc_setup_auth(self, auth_path, auth_fname):
        """
        Sets up GES DISC authentication files for a WilE object
        :param auth_path: string giving path to text file that contains login and password for Earthdata account
        :param auth_fname: string giving the filename of the aforementioned text file
        :return: none
        """
        # how to make prereq files that you need in order to access GES DISC data like Earthdata's LDAS:
        # https://disc.gsfc.nasa.gov/information/howto?title=How%20to%20Generate%20Earthdata%20Prerequisite%20Files
        if not self.GES_DISC_AUTH_SETUP_FLAG:  # if GES DISC authentication hasn't already been set up, do so
            earthdata_auth = get_dict_from_file(auth_path, auth_fname)
            earthdata_setup_auth(earthdata_auth, dodsrc_dest=self.CALLER_DIR)  # TODO: ascertain whether caller dir is
                                                                               #       always appropriate place to put
                                                                               #       dodsrc document
            self.GES_DISC_AUTH_SETUP_FLAG = True

    def gesdisc_sort_results(self, results):
        """
        Sorts GES DISC API response into documents and download URLs
        :param results: JSON-formatted object derived from Requests response
        :return: docs and urls, arrays of strings giving links to documentation or to file URLs, respectively
        """
        docs = []
        urls = []

        for item in results:
            try:
                if item['start'] and item['end']: urls.append(item)
            except:
                docs.append(item)

        return docs, urls

    def pull_everything(self):
        # pull all data sources, including updating historical set
        self.logger.info("pull_everything() was called; this is still under construction!")
        self.logger.info("Pulling data from ALL built-in sources. Historical data being updated.\n",
                         "This may take several hours!")

    def pull_rt(self):
        # pull realtime data
        self.logger.debug("pull_rt was called. This is still under construction!")

    def pull_synoptic_rt(self, auto_clean=True, write=True):
        self.logger.info("Pulling latest synoptic weather data.")
        self.logger.debug("Auto_clean = {} and write = {}".format(auto_clean, write))

        syn_api_rt_req_url = os.path.join(self.SYNOPTIC_API_ROOT, self.SYNOPTIC_RT_FILTER)  # URL to request synoptic data
        # arguments to pass to the synoptic API
        # TODO: either make two CSVs or two separate requests so that all vars needed to calc dewpoint dep are present together
        # TODO: find out how to measure sustained wind speed. Var to get instantaneous is wind_speed
        syn_api_args = {"state": "CA", "units": "metric,speed|kph,pres|mb", "varsoperator": "or",
                        "vars": "air_temp,sea_level_pressure,relative_humidity,dew_point_temperature,soil_temp,precip_accum",
                        "syn_token": self.SYNOPTIC_API_TOKEN}

        syn_resp = requests.get(syn_api_rt_req_url, params=syn_api_args)
        syn_resp = syn_resp.json()  # despite it being called json(), this returns a dict object from the requests module

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
            syn_csv_filename = "synoptic_rt_request.csv"
            os.chdir(self.DATA_RT_DIR)
            syn_df.to_csv(syn_csv_filename)
            self.logger.info("Wrote latest synoptic data response to CSV file")

            if self.logger.level == 10:  # if set to debug, open the historic csv to be sure it was retrieved properly
                os.startfile(os.path.join(os.getcwd(), syn_csv_filename))
            os.chdir(self.CALLER_DIR)

    def pull_ldas_rt(self):

        earthdata_auth = get_dict_from_file(self.GES_DISC_AUTH_PATH, self.GES_DISC_AUTH_FNAME)

        urs = 'urs.earthdata.nasa.gov'  # Earthdata URL to call for authentication
        prompts = ['Enter NASA Earthdata Login Username \n(or create an account at urs.earthdata.nasa.gov): ',
                   'Enter NASA Earthdata Login Password: ']

        homeDir = os.path.expanduser("~") + os.sep
        print("Obtained homeDir: " + homeDir)

        with open(homeDir + '.netrc', 'w') as file:
            print("Attempting to create .netrc file...")
            # file.write('machine {} login {} password {}'.format(urs, getpass(prompt=prompts[0]), getpass(prompt=prompts[1])))
            file.write('machine {} login {} password {}'.format(urs, earthdata_auth['login'], earthdata_auth['password']))
            file.close()
        with open(homeDir + '.urs_cookies', 'w') as file:
            print("Attempting to create .urs_cookies file...")
            file.write('')
            file.close()
        with open(homeDir + '.dodsrc', 'w') as file:
            print("Attempting to create .dodsrc file...")
            file.write('HTTP.COOKIEJAR={}.urs_cookies\n'.format(homeDir))
            file.write('HTTP.NETRC={}.netrc'.format(homeDir))
            file.close()

        print('Saved .netrc, .urs_cookies, and .dodsrc to:', homeDir)

        # Set appropriate permissions for Linux/macOS
        if platform.system() != "Windows":
            Popen('chmod og-rw ~/.netrc', shell=True)
        else:
            # Copy dodsrc to working directory in Windows
            shutil.copy2(homeDir + '.dodsrc', os.getcwd())
            print('Copied .dodsrc to:', os.getcwd())

        # This method POSTs formatted JSON WSP requests to the GES DISC endpoint URL and returns the response
        def get_http_data(request):

            # hdrs = urllib3.make_headers(basic_auth='Eshreth_of_Athshe:SONOlu4mi__._ne8scence')
            hdrs = {}
            hdrs['Content-Type'] = 'application/json'
            hdrs['Accept'] = 'application/json'
            data = json.dumps(request)
            r = http.request('POST', svcurl, body=data, headers=hdrs)
            response = json.loads(r.data)

            # Check for errors
            if response['type'] == 'jsonwsp/fault':
                print('API Error: faulty request')
            return response

        # Create a urllib PoolManager instance to make requests.
        http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED',
                                   ca_certs=certifi.where())

        # Set the URL for the GES DISC subset service endpoint
        svcurl = 'https://disc.gsfc.nasa.gov/service/subset/jsonwsp'

        # Define the parameters for the data subset
        product = 'ML2T_004'
        begTime = '2015-08-01T00:00:00.000Z'
        endTime = '2015-08-03T23:59:59.999Z'
        minlon = -180.0
        maxlon = 180.0
        minlat = -30.0
        maxlat = 30.0
        varNames = ['/HDFEOS/SWATHS/Temperature/Data Fields/Temperature',
                    '/HDFEOS/SWATHS/Temperature/Data Fields/TemperaturePrecision',
                    '/HDFEOS/SWATHS/Temperature/Data Fields/Quality']

        # The dimension slice will be for pressure levels between 1000 and 100 hPa
        dimName = '/HDFEOS/SWATHS/Temperature/nLevels'
        dimVals = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
        dimSlice = []
        for i in range(len(dimVals)):
            dimSlice.append({'dimensionId': dimName, 'dimensionValue': dimVals[i]})

        # Construct JSON WSP request for API method: subset
        subset_request = {
            'methodname': 'subset',
            'type': 'jsonwsp/request',
            'version': '1.0',
            'args': {
                'role': 'subset',
                'start': begTime,
                'end': endTime,
                'box': [minlon, minlat, maxlon, maxlat],
                'crop': True,
                'data': [{'datasetId': product,
                          'variable': varNames[0],
                          'slice': dimSlice
                          },
                         {'datasetId': product,
                          'variable': varNames[1],
                          'slice': dimSlice
                          },
                         {'datasetId': product,
                          'variable': varNames[2]
                          }]
            }
        }

        # Submit the subset request to the GES DISC Server
        response = get_http_data(subset_request)

        # Report the JobID and initial status
        myJobId = response['result']['jobId']
        print('Job ID: ' + myJobId)
        print('Job status: ' + response['result']['Status'])

        # Construct JSON WSP request for API method: GetStatus
        status_request = {
            'methodname': 'GetStatus',
            'version': '1.0',
            'type': 'jsonwsp/request',
            'args': {'jobId': myJobId}
        }

        # Check on the job status after a brief nap
        while response['result']['Status'] in ['Accepted', 'Running']:
            sleep(5)
            response = get_http_data(status_request)
            status = response['result']['Status']
            percent = response['result']['PercentCompleted']
            print('Job status: %s (%d%c complete)' % (status, percent, '%'))

        if response['result']['Status'] == 'Succeeded':
            print('Job Finished:  %s' % response['result']['message'])
        else:
            print('Job Failed: %s' % response['fault']['code'])
            sys.exit(1)

        # Use the API method named GetResult. This method will return the URLs along with three additional attributes: a label,
        # plus the beginning and ending time stamps for that particular data granule. The label serves as the filename for the
        # downloaded subsets.

        # Construct JSON WSP request for API method: GetResult
        batchsize = 20
        results_request = {
            'methodname': 'GetResult',
            'version': '1.0',
            'type': 'jsonwsp/request',
            'args': {
                'jobId': myJobId,
                'count': batchsize,
                'startIndex': 0
            }
        }

        # Retrieve the results in JSON in multiple batches
        # Initialize variables, then submit the first GetResults request
        # Add the results from this batch to the list and increment the count
        results = []
        count = 0
        response = get_http_data(results_request)
        count = count + response['result']['itemsPerPage']
        results.extend(response['result']['items'])

        # Increment the startIndex and keep asking for more results until we have them all
        total = response['result']['totalResults']
        while count < total:
            results_request['args']['startIndex'] += batchsize
            response = get_http_data(results_request)
            count = count + response['result']['itemsPerPage']
            results.extend(response['result']['items'])

        # Check on the bookkeeping
        print('Retrieved %d out of %d expected items' % (len(results), total))

        # Sort the results into documents and URLs
        docs = []
        urls = []
        for item in results:
            try:
                if item['start'] and item['end']: urls.append(item)
            except:
                docs.append(item)

        # Print out the documentation links, but do not download them
        print('\nDocumentation:')
        for item in docs: print(item['label'] + ': ' + item['link'])

        print(os.getcwd())

        # Use the requests library to submit the HTTP_Services URLs and write out the results.
        print('\nHTTP_services output:')
        for item in urls:
            URL = item['link']
            result = requests.get(URL)
            try:
                result.raise_for_status()
                outfn = item['label']
                f = open(outfn, 'wb')
                f.write(result.content)
                f.close()
                print(outfn)
            except:
                print('Error! Status code is %d for this URL:\n%s' % (result.status.code, URL))
                print('Help for downloading data is at https://disc.gsfc.nasa.gov/information/documents?title=Data%20Access')



    def pull_ldas_rt2(self,
                     svcurl='https://disc.gsfc.nasa.gov/service/subset/jsonwsp',  # URL for the GES DISC subset service endpoint
                     product='ML2T_004',  # Define the parameters for the data subset
                     beg_time='2015-08-01T00:00:00.000Z',
                     end_time='2015-08-03T23:59:59.999Z',
                     min_lon=-180.0,
                     max_lon=180.0,
                     min_lat=-30.0,
                     max_lat=30.0,
                     var_names=['/HDFEOS/SWATHS/Temperature/Data Fields/Temperature',
                                  '/HDFEOS/SWATHS/Temperature/Data Fields/TemperaturePrecision',
                                  '/HDFEOS/SWATHS/Temperature/Data Fields/Quality'],  # The dimension slice defaults to pressure levels between 1000 and 100 hPa
                     dim_name='/HDFEOS/SWATHS/Temperature/nLevels',
                     dim_vals=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13],
                     dim_slice=[]):

        dir_return(self.CALLER_DIR)

        # TODO: decompose this monstrosity
        # https://disc.gsfc.nasa.gov/information/howto?title=How%20to%20Use%20the%20Web%20Services%20API%20for%20Subsetting
        self.logger.info("Pulling realtime LDAS data.")

        self.gesdisc_setup_auth(self.GES_DISC_AUTH_PATH,
                                self.GES_DISC_AUTH_FNAME)

        # Create a urllib PoolManager instance to make requests.
        http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED',
                                   ca_certs=certifi.where())

        # construct dimension slices for the subset request
        for i in range(len(dim_vals)):
            dim_slice.append({'dimensionId': dim_name, 'dimensionValue': dim_vals[i]})

        # Construct JSON WSP request for API method: subset
        subset_request = {
            'methodname': 'subset',
            'type': 'jsonwsp/request',
            'version': '1.0',
            'args': {
                'role': 'subset',
                'start': beg_time,
                'end': end_time,
                'box': [min_lon, min_lat, max_lon, max_lat],
                'crop': True,
                'data': [{'datasetId': product,
                          'variable': var_names[0],
                          'slice': dim_slice
                          },
                         {'datasetId': product,
                          'variable': var_names[1],
                          'slice': dim_slice
                          },
                         {'datasetId': product,
                          'variable': var_names[2]
                          }]
            }
        }

        # Submit the subset request to the GES DISC Server
        response = self.gesdisc_get_http_data(subset_request, http, svcurl)

        # Report the JobID and initial status
        my_job_id = response['result']['jobId']
        self.logger.info('GES DISC job ID: ' + my_job_id)
        self.logger.info('GES DISC job status: ' + response['result']['Status'])

        # Construct JSON WSP request for API method: GetStatus
        status_request = {
            'methodname': 'GetStatus',
            'version': '1.0',
            'type': 'jsonwsp/request',
            'args': {'jobId': my_job_id}
        }

        # Announce job status periodically until it finishes or fails
        while response['result']['Status'] in ['Accepted', 'Running']:
            sleep(5)
            response = self.gesdisc_get_http_data(status_request, http, svcurl)
            status = response['result']['Status']
            percent = response['result']['PercentCompleted']
            self.logger.info('GES DISC job status: %s (%d%c complete)' % (status, percent, '%'))

        if response['result']['Status'] == 'Succeeded':
            self.logger.info('GES DISC job Finished:  %s' % response['result']['message'])
        else:
            if self.logger.level == 10:  # if set to debugging, give verbose report
                self.logger.error('GES DISC job failed: %s' % response['fault']['code'], stack_info=True, exc_info=True)
            else:
                self.logger.error('GES DISC job failed: %s' % response['fault']['code'])
            # sys.exit(1)
            return

        # Use the API method named GetResult. This method will return the URLs along with three additional attributes:
        # a label, plus the beginning and ending time stamps for that particular data granule. The label serves as the
        # filename for the downloaded subsets.

        # Construct JSON WSP request for API method: GetResult
        batchsize = 20
        results_request = {
            'methodname': 'GetResult',
            'version': '1.0',
            'type': 'jsonwsp/request',
            'args': {
                'jobId': my_job_id,
                'count': batchsize,
                'startIndex': 0
            }
        }

        # Retrieve the results in JSON in multiple batches
        # Initialize variables, then submit the first GetResults request
        # Add the results from this batch to the list and increment the count
        results = []
        count = 0
        response = self.gesdisc_get_http_data(results_request, http, svcurl)
        count = count + response['result']['itemsPerPage']
        results.extend(response['result']['items'])

        # Increment the startIndex and keep asking for more results until we have them all
        total = response['result']['totalResults']
        while count < total:
            results_request['args']['startIndex'] += batchsize
            response = self.gesdisc_get_http_data(results_request, http, svcurl)
            count = count + response['result']['itemsPerPage']
            results.extend(response['result']['items'])

        # Check on the bookkeeping
        self.logger.info('Retrieved %d out of %d expected items' % (len(results), total))

        # Sort the results into documents and URLs
        docs, urls = self.gesdisc_sort_results(results)

        if self.logger.level == 10:  # if the logger is set to debug, provide additional information about the code that
                                     # ran

            # Print out the documentation links, but do not download them
            self.logger.debug('\nDocumentation:')
            for item in docs: self.logger.debug(item['label'] + ': ' + item['link'])

        dir_return(self.CALLER_DIR)

        # Use the requests library to submit the HTTP_Services URLs and write the results to file.
        self.logger.info('\nHTTP_services output:')
        for item in urls:
            URL = item['link']
            result = requests.get(URL)

            if result.status_code == 401:
                self.logger.debug('\t (First attempt to retrieve data 401\'d, attempting to pass auth info again)')

                earthdata_auth = get_dict_from_file(self.GES_DISC_AUTH_PATH,
                                                    self.GES_DISC_AUTH_FNAME)
                earthdata_setup_auth(earthdata_auth, dodsrc_dest=self.CALLER_DIR)
                result = requests.get(URL, auth=HTTPBasicAuth('Eshreth_of_Athshe', 'SONOlu4mi__._ne8scence'))

            try:
                result.raise_for_status()
                # os.chdir(self.DATA_RT_DIR)
                outfn = item['label']
                f = open(outfn, 'wb')
                f.write(result.content)
                f.close()
                self.logger.info("successfuly downloaded and wrote " + outfn)
                # os.chdir(self.CALLER_DIR)
            except:
                self.logger.error('Status code is %d, %s for this URL:\n%s' % (result.status_code, result.reason, URL),
                                  stack_info=True, exc_info=True)
                self.logger.debug('Help for downloading data is at https://disc.gsfc.nasa.gov/information/documents?title=Data%20Access')


    def pull_historic(self):
        # pull historic data
        self.logger.debug("pull_historic() was called")

    def pull_synoptic_hist(self):
        # TODO
        # pull synoptic data as far back as arg
        self.logger.debug("pulling synoptic timeseries data \n" +
                          "NOTE: this function isn't complete and doesn't work yet! \n" +
                          "WARNING: it may take up to several hours to fulfill a timeseries request longer than one day!")

        SYNOPTIC_HIST_FILTER = "stations/timeseries"  # filter for timeseries data TODO: refactor so that this is function argument
        # SYN_HIST_START not needed
        if self.logger.level == 10:  # if logging level set to debug, only retrieve a little historic data
            syn_hist_start_dt = datetime.now()
            syn_hist_start_dt -= timedelta(hours=3)  # only go back a bit from now
            SYN_HIST_START = syn_hist_start_dt.strftime(self.SYN_TIME_FORMAT)
        else:  # otherwise go waaaay back
            SYN_HIST_START = "199001010000"  # earliest time to seek to is 1990/01/01, 00:00.
                                             # Most data will be nowhere near that.
                                             # TODO: make this function arg.

        # TODO: these might need to be function args
        syn_api_args = {"state": "CA", "units": "metric,speed|kph,pres|mb", "varsoperator": "or",
                        "vars": "air_temp,sea_level_pressure,relative_humidity,dew_point_temperature,soil_temp,precip_accum",
                        "syn_token": self.SYNOPTIC_API_TOKEN}  # TODO: this might need to go someplace else
        syn_hist_end = datetime.now().strftime(self.SYN_TIME_FORMAT)  # string giving current datetime; pull up to the absolute most recent data
        syn_api_hist_req_url = os.path.join(self.SYNOPTIC_API_ROOT, SYNOPTIC_HIST_FILTER)  # URL to request synoptic data
        syn_hist_args = syn_api_args

        # Requesting all recorded timeseries for CA is too much at once. Instead, request timeseries in day-long chunks.
        # This uses the datetime module to create 2 datetime objects that will outline each chunk.
        chunk_range_end = syn_hist_end  # string; first end of range should be datetime.now()
        chunk_end_dt = datetime.strptime(chunk_range_end, self.SYN_TIME_FORMAT)  # datetime object giving the end of the chunk timerange
        chunk_end_dt -= timedelta(hours=1)  # run the chunk end dt (but not the string!) back 1 day
        chunk_range_start = chunk_end_dt.strftime(self.SYN_TIME_FORMAT)  # datetime object giving the start of the chunk timerange
        # chunk_start_dt not needed

        start_dt = datetime.strptime(SYN_HIST_START, self.SYN_TIME_FORMAT)  # datetime giving the very earliest date to seek to
        range_delta = chunk_end_dt - start_dt  # time difference in complete target range
        min_delta = timedelta(hours=1)  # set minimum distance between the current chunk and the earliest date

        syn_hist_args["START"] = chunk_range_start
        syn_hist_args["END"] = chunk_range_end
        syn_resp = requests.get(syn_api_hist_req_url, params=syn_hist_args, headers={'Accept': 'application/json'})
        syn_dict = syn_resp.json()  # convert to dict of dicts

        self.logger.debug("start={}, end={} \n\nbeginning while loop:".format(SYN_HIST_START, syn_hist_end))

        while range_delta > min_delta:
            self.logger.debug("iterating: chunk start={}, chunk end={}".format(chunk_range_start,
                                                                               chunk_range_end))

            # construct timeseries query
            syn_hist_args["START"] = chunk_range_start
            syn_hist_args["END"] = chunk_range_end
            chunk_resp = requests.get(syn_api_hist_req_url, params=syn_hist_args)  # send request and store response
                                                                                   # for this chunk
            chunk_dict = chunk_resp.json()
            syn_dict.update(chunk_dict)

            # update chunk boundaries and distance from next chunk to the earliest date
            chunk_range_end = chunk_range_start
            chunk_end_dt = datetime.strptime(chunk_range_end, self.SYN_TIME_FORMAT)  # temporary datetime object of the chunk range end
            chunk_end_dt -= timedelta(hours=1)  # run back one day
            chunk_range_start = chunk_end_dt.strftime(self.SYN_TIME_FORMAT)
            range_delta = chunk_end_dt - start_dt

            self.logger.debug("retrieved one chunk")

        # if set to debug, save raw response as text file
        if self.logger.level == 10:
            os.chdir(self.DEBUG_DIR)
            with open("synoptic historic raw response.txt", 'w') as f:
                f.write(json.dumps(syn_dict, indent=4))

        os.chdir(self.DATA_HIST_DIR)

        # convert JSON to pandas df
        syn_hist_df = pd.json_normalize(syn_dict['STATION'])


        now = datetime.now()  # get current datetime
        now_str = now.strftime("%m.%d.%Y_%H.%M.%S")  # convert the datetime to a string
        syn_hist_filename = "synoptic_historical_retrieved_" + now_str + ".csv"
        syn_hist_df.to_csv(syn_hist_filename)

        self.logger.info("saved historical measurements to csv\n" +
                         "filename = {}\n".format(syn_hist_filename) +
                         "path = {}\n".format(os.getcwd()))

        os.chdir(self.CALLER_DIR)
