# Main driver file for the AlertCalifornia Wildfire probability Estimator (WilE).
# Last editor: Ben Hoffman
# Contact: blhoff97@gmail.com
# This file and its ancillaries use the following formatting conventions:
# GLOBAL_CONSTANTS intended to be used throughout one or multiple files use caps snakecase
# local_variables use lowercase snakecase
# classAttributes use camelcase

import wildfire_probability_estimator

TOKEN = "eb977b5f24ed48b585ccb4e520906425"  # https://api.synopticdata.com/v2/stations/metadata?&state=CA&sensorvars=1&complete=1&token=

wpe = wildfire_probability_estimator.wile(TOKEN, logger_level=10)  # instantiate class object

wpe.pull_synoptic_hist()

# TODO
# WIFIRE historical sets

# Synoptic realtime weather data





