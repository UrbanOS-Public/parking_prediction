import functools
import itertools
import logging
import os
import pickle

import boto3
import numpy as np
import pandas as pd
import pytest
import responses
from moto import mock_s3

from app import keeper_of_the_state
from app.constants import DISCOVERY_API_QUERY_URL, MODEL_FILE_NAME
from app.keeper_of_the_state import MODELS_DIR_LATEST, MODELS_DIR_ROOT
from app.model import ParkingAvailabilityModel

for noisy_logger_name in ['botocore', 'fbprophet', 'app.model']:
    logging.getLogger(noisy_logger_name).setLevel(logging.CRITICAL)


ALL_VALID_ZONE_IDS = [
    'twilight', 'auto', 'splash', 'danger', 'red', 'habitable', 'school',
    '-d out', 'spin', 'no spin', 'cal-', 'defense', 'end', 'residential'
] + [
    parrot_count * '🦜'
    for parrot_count in range(1, 11)
]


@pytest.fixture(scope='function')
def mocked_scos_zone_ids_query():
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            DISCOVERY_API_QUERY_URL,
            'zoneIds\n' + '\n'.join(ALL_VALID_ZONE_IDS)
        )
        yield rsps


@pytest.fixture(scope='function')
async def with_warmup(mocked_scos_zone_ids_query, fake_model_files_in_s3):
    await keeper_of_the_state.warm_caches()


@pytest.fixture(scope='session')
def all_valid_zone_ids():
    return ALL_VALID_ZONE_IDS


@pytest.fixture(scope='module')
def fake_dataset(all_valid_zone_ids):
    weeks_of_semihours = functools.reduce(
        lambda current_dates, new_dates: current_dates + new_dates.tolist(),
        [
            pd.date_range(
                start=pd.Timestamp('2020-08-31 08:00') + pd.Timedelta(weeks=week),
                end=pd.Timestamp('2020-09-05 22:00') + pd.Timedelta(weeks=week),
                freq='30min',
                closed='left'
            )
            for week in range(4)
        ],
        []
    )

    rng = np.random.default_rng(seed=42)

    return pd.DataFrame.from_records([
        {
            'zone_id': zone_id,
            'semihour': semihour,
            'occu_cnt_rate': occupancy_rate,
            'total_cnt': total_cnt
        }
        for zone_id in all_valid_zone_ids
        for semihour, occupancy_rate, total_cnt in zip(
            weeks_of_semihours,
            itertools.accumulate(
                itertools.repeat(0),
                lambda _, __: rng.uniform()
            ),
            itertools.repeat(21)
        )
    ])


@pytest.fixture(scope='module')
def fake_model(fake_dataset):
    model = ParkingAvailabilityModel()
    model.train(fake_dataset)
    return model


@pytest.fixture(scope='module')
def aws_credentials():
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_REGION'] = 'us-west-2'


@pytest.fixture(scope='module')
def fake_model_files_in_s3(fake_model, aws_credentials):
    os.environ['COMPARED_MODELS'] = '12month,18month,24month'
    with mock_s3():
        s3 = boto3.resource('s3', region_name='us-west-2')

        bucket = s3.Bucket('dev-parking-prediction')
        bucket.create(CreateBucketConfiguration={'LocationConstraint': 'us-west-2'})

        pickled_model = pickle.dumps(fake_model)
        pickled_model_s3_keys = [
            f'{MODELS_DIR_LATEST}/{MODEL_FILE_NAME}',
            f'{MODELS_DIR_ROOT}/12month/{MODEL_FILE_NAME}',
            f'{MODELS_DIR_ROOT}/18month/{MODEL_FILE_NAME}',
            f'{MODELS_DIR_ROOT}/24month/{MODEL_FILE_NAME}'
        ]

        for pickled_model_key in pickled_model_s3_keys:
            bucket.put_object(Body=pickled_model, Key=pickled_model_key)

        yield
