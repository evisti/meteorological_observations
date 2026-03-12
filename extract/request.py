import requests
import pandas as pd
from datetime import datetime


dmi_obs_url = 'https://opendataapi.dmi.dk/v2/metObs/collections/observation/items'
dmi_station_url = 'https://opendataapi.dmi.dk/v2/metObs/collections/station/items'


def _fetch_data(url: str, params: dict) -> pd.DataFrame:
    '''
    Submit GET request with url and parameters, and convert result to DataFrame
    '''
    timeout = 10 # seconds
    try:
        response = requests.get(url, params=params, timeout=timeout)
        print('\nFetching URL:', response.url)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print('Error: HTTP error')
        print(e)
    except requests.exceptions.ReadTimeout:
        print('Error: Request time out')
    except requests.exceptions.ConnectionError:
        print('Error: Connection error')
    except requests.exceptions.RequestException:
        print('Error: Exception request')

    # decode json response
    result = response.json()

    # convert to DataFrame
    df = pd.json_normalize(result['features'])
    if df.empty:
        print('No data')

    return df


def get_stations(station_id: str=None) -> pd.DataFrame:
    # define query parameters for the request
    query_params = {}
    if station_id: query_params['stationId'] = station_id

    return _fetch_data(dmi_station_url, query_params)


def get_metobs(parameter: str, station_id: str, start_time: datetime, end_time: datetime) -> pd.DataFrame:
    # define query parameters for the request
    datetime_str = start_time.isoformat() + 'Z/' + end_time.isoformat() + 'Z'
    query_params = {
        'datetime' : datetime_str,
        'limit' : '300000'  # maximum number of observations
        }
    if parameter: query_params['parameterId'] = parameter
    if station_id: query_params['stationId'] = station_id

    return _fetch_data(dmi_obs_url, query_params)
