#!/usr/bin/env python3

import configparser
import getpass
import logging
import os
import sys
from dataclasses import InitVar, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pyodbc
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
from pytz import timezone

from app import keeper_of_the_state, now_adjusted, predictor
from app.constants import DAY_OF_WEEK, UNENFORCED_DAYS
from app.model import ParkingAvailabilityModel

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
if sys.stdout.isatty():
    LOGGER.addHandler(logging.StreamHandler(sys.stdout))

DIRNAME = Path(__file__).parent.absolute()


@dataclass
class SqlServerConfig:
    server: str
    database: str
    uid: InitVar[str] = None
    pwd: InitVar[str] = None

    def __post_init__(self, uid, pwd):
        if not (uid is None or pwd is None):
            self.driver = 'ODBC Driver 17 for SQL Server'
            self.uid = uid
            self.pwd = pwd
        else:
            self.driver = 'SQL Server Native Client 11.0'
            self.trusted_connection = self.mars_connection = 'yes'


def main():
    occupancy_dataframe = _get_occupancy_data_from_database(_get_database_config())

    model = ParkingAvailabilityModel()

    model.train(
        (occupancy_dataframe
            .astype({'semihour': 'datetime64[ns]'})
            .rename(columns={'zone_name': 'zone_id'})
            .pipe(_remove_unoccupied_timeslots)
            .pipe(_remove_times_outside_hours_of_operation))
    )

    keeper_of_the_state.archive_model(model)

    # _validate_variance()


def _get_database_config():
    config = configparser.RawConfigParser()
    config.read(DIRNAME / 'app/train.config')

    smrt_environment = os.getenv('SCOS_ENV', default='dev')
    sql_password = os.getenv('SQL_SERVER_PASSWORD') or getpass.getpass()
    return SqlServerConfig(
        server=os.getenv('SQL_SERVER_URL', config.get(smrt_environment, 'mssql_url')),
        database=os.getenv('SQL_SERVER_DATABASE', config.get(smrt_environment, 'mssql_db_name')),
        uid=os.getenv('SQL_SERVER_USERNAME', config.get(smrt_environment, 'mssql_db_user')),
        pwd=sql_password
    )


def _get_occupancy_data_from_database(database_config):
    sql_query = '''
        SELECT
            [zone_name], [semihour], [occu_min], [occu_mtr_cnt],
            [no_trxn_one_day_flg], [no_trxn_one_week_flg],
            [total_cnt],
            [occu_min_rate], [occu_cnt_rate],
            [city_holiday], [shortnorth_event],
            [no_data]
        FROM [dbo].[parking_zone_occupancy_aggr]
        WHERE CONVERT(date, semihour) >= CONVERT(date, DATEADD(month, -6, GETUTCDATE()))
        ORDER BY zone_name, semihour
    '''

    try:
        occupancy_dataframe = _sql_read(database_config, sql_query)
    except Exception as e:
        LOGGER.error(f'Unexpected error: {e}')
        raise e

    if not occupancy_dataframe.empty:
        LOGGER.info('Read data from DB into dataframe successfully.')
        LOGGER.info(f'Total (row, col) counts for dataframe: {occupancy_dataframe.shape}')
        LOGGER.info(f'Zones in dataframe: {len(occupancy_dataframe["zone_name"].unique())}')
    else:
        LOGGER.error(f'No data read from DB: {occupancy_dataframe}')
        raise Exception('No data read from DB')

    return occupancy_dataframe


def _sql_read(database_config, sql_query):
    LOGGER.info(f'Reading data from DB {database_config.server}')
    LOGGER.debug('Performing DB read with spec of %s', database_config.__dict__)

    with pyodbc.connect(**database_config.__dict__) as conn:
        return pd.concat(list(pd.read_sql_query(sql_query, conn, chunksize=10 ** 6)), ignore_index=True)


def _remove_unoccupied_timeslots(occupancy_dataframe: pd.DataFrame) -> pd.DataFrame:
    return occupancy_dataframe.loc[(
        (occupancy_dataframe.no_data != 1)
      & (occupancy_dataframe.no_trxn_one_week_flg != 1)
    )]


def _remove_times_outside_hours_of_operation(occupancy_dataframe: pd.DataFrame) -> pd.DataFrame:
    enforcement_days = [day.value for day in DAY_OF_WEEK
                        if day not in UNENFORCED_DAYS]
    return (
        occupancy_dataframe
            .set_index('semihour')
            .between_time('08:00', '22:00', include_end=False)
            .reset_index()
            .loc[lambda df: df.semihour.dt.dayofweek.isin(enforcement_days), :]
    )


def _validate_variance():
    yesterday_model = keeper_of_the_state.historical_model_name(date.today() - timedelta(1))
    today_model = keeper_of_the_state.historical_model_name(date.today())
    models = [yesterday_model, today_model]
    keeper_of_the_state.warm_caches_synchronously(models)

    now = now_adjusted.adjust(datetime.now(timezone('US/Eastern')))
    today_at_ten = now.replace(hour=10)
    predictions = predictor.predict_with(models, today_at_ten)

    registry = CollectorRegistry()
    gauge = Gauge(
        'parking_model_variance',
        'Variance in prediction after new model is trained',
        registry=registry,
        labelnames=['zone']
    )
    for prediction in predictions:
        prediction_yesterday = prediction[f'{yesterday_model}Prediction']
        prediction_today = prediction[f'{today_model}Prediction']
        variance = abs(round(prediction_today - prediction_yesterday, 10))
        zone = prediction['zoneId']
        gauge.labels(zone=zone).set(variance)

    environment = os.getenv('SCOS_ENV', default='dev')
    push_to_gateway(
        f'https://pushgateway.{environment}.internal.smartcolumbusos.com',
        job='variance',
        registry=registry
    )


if __name__ == '__main__':
    main()
