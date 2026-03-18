from datetime import datetime, timedelta, time
from pathlib import Path
from sqlalchemy import MetaData

from db_connection import SQLRunner, get_engine
from etl.extract import get_stations, get_observations, get_spac
from etl.transform import clean_stations, clean_observations, clean_spac
from etl.load import stations_table, observations_table, spac_table, create_tables, load_to_sql


url_dmi = 'https://opendataapi.dmi.dk/v2/metObs/collections'
url_spac = 'https://climate.spac.dk/api/records'


# EXTRACT

# specify desired start and end time (e.g. the most recent 7:00 AM)
now = datetime.now()
end_time = datetime.combine(now.date(), time(7)) - timedelta(days=now.time() < time(7))
start_time = end_time - timedelta(days=30)

# specify station and parameters for dmi observations
station_id = '06072'
parameter = None
#parameter = 'wind_speed'

# fetch data
#df_stations = get_stations(url_dmi)
df_observations = get_observations(url_dmi, parameter, station_id, start_time, end_time)
#df_spac = get_spac(url_spac)

# TRANSFORM

#clean_stations(df_stations)
clean_observations(df_observations)
#clean_spac(df_spac)
 

# LOAD

# connect to database
config_file = Path('./db_config.ini')
sql_runner = SQLRunner(get_engine(config_file))

# create metadata
metadata = MetaData()

# define tables
#stations = stations_table(metadata)
observations = observations_table(metadata)
#spac = spac_table(metadata)

# create tables in database
create_tables(sql_runner, metadata)

# load data into database
#load_to_sql(sql_runner, table=stations, df=df_stations)
load_to_sql(sql_runner, table=observations, df=df_observations)
#load_to_sql(sql_runner, table=spac, df=df_spac)
