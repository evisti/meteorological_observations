import pandas as pd
from datetime import datetime, timedelta, time
from pathlib import Path

from sqlalchemy import MetaData

from db_connection import SQLRunner, get_engine

from extract.request import get_stations, get_metobs
from transform.transform import stations_data_cleaning, metobs_data_cleaning
from load.load import define_stations_table, define_metobs_table, create_tables, load_to_sql


# specify desired start and end time
# choose time interval: the latest 7 AM to 7 AM
now = datetime.now()
end_time = datetime.combine(now.date(), time(7)) - timedelta(days=now.time() < time(7)) # the most recent 7:00 AM
start_time = end_time - timedelta(days=1)

station_id = '06072'
parameter = 'wind_speed'


# Fetch meterological observation data
df1 = get_metobs(parameter, station_id, start_time, end_time)

# Fetch all stations
stations = get_stations()


# TODO: clean the data


# SQL connection

config_file = Path('./db_config.ini')
#sql_runner = SQLRunner(get_engine(config_file))


# create tables

metadata = MetaData()

metobs_table = define_metobs_table(metadata)
stations_table = define_stations_table(metadata)

#create_tables(sql_runner, metadata)


# load into database

#load_to_sql(sql_runner, metobs_table, df1)
#load_to_sql(sql_runner, stations_table, stations) # gives error because of ()

