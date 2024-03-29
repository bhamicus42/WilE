# Main driver file for the Wildfire probability Estimator (WilE).
# Last editor: Ben Hoffman
# Contact: blhoff97@gmail.com



import wildfire_probability_estimator

SYN_TOKEN = "eb977b5f24ed48b585ccb4e520906425"  # https://api.synopticdata.com/v2/stations/metadata?&state=CA&sensorvars=1&complete=1&token=

wpe = wildfire_probability_estimator.wile(SYN_TOKEN, logger_level=10)  # instantiate class object; logger level 10 means debug

wpe.pull_ldas_rt()

