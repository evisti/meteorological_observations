import pandas as pd
from sqlalchemy import MetaData, Table, Column, Integer, String, Float, DateTime, ARRAY
from db_connection import SQLRunner


def define_stations_table(metadata: MetaData) -> Table:
    table = Table(
        'stations', metadata,
        Column('id', Integer, primary_key=True),
        Column('owner', String(255)),
        Column('country', String(3)),
        Column('anemometerHeight', Float),
        Column('wmoCountryCode', String(4)),
        Column('operationFrom', DateTime),
        Column('parameter', ARRAY(String(50))),
        Column('created', DateTime),
        Column('barometerHeight', Float),
        Column('validFrom', DateTime),
        Column('type', String(50)),
        Column('stationHeight', Float),
        Column('regionId', Integer),
        Column('name', String(255)),
        Column('wmoStationId', String(5)),
        Column('operationTo', DateTime),
        Column('stationId', String(5)),
        Column('validTo', DateTime),
        Column('status', String(50)),
        Column('longitude', Float),
        Column('latitude', Float)
    )
    return table

def define_metobs_table(metadata: MetaData) -> Table:
    table = Table(
        'metobs', metadata,
        Column('id', Integer, primary_key=True),
        Column('observed', DateTime),
        Column('parameter', String(50)),
        Column('value', Float),
        Column('stationId', String(5)),
        Column('latitude', Float),
        Column('longitude', Float)
    )
    return table


def create_tables(sql_runner: SQLRunner, metadata: MetaData):
    metadata.create_all(sql_runner.engine)

    print('Tables created:', end=' ')
    print(*metadata.tables.keys(), sep=', ')


def drop_table(table_name):
    pass


def load_to_sql(sql_runner: SQLRunner, table: Table, df: pd.DataFrame):
    with sql_runner.engine.begin() as connection:
        df.to_sql(name=table.name, con=connection, if_exists='append', index=True, index_label='id')

