import pickle

import boto3
import joblib
import pytest
from freezegun import freeze_time
from mockito import kwargs
from moto import mock_s3

from app import keeper_of_the_state
from app.constants import MODEL_FILE_NAME
from app.keeper_of_the_state import MODELS_DIR_LATEST, MODELS_DIR_ROOT
from app.model import ParkingAvailabilityModel


@pytest.fixture(scope='function')
def model_bucket(aws_credentials):
    with mock_s3():
        s3 = boto3.resource('s3', region_name='us-west-2')
        bucket = s3.Bucket('dev-parking-prediction')
        bucket.create(CreateBucketConfiguration={'LocationConstraint': 'us-west-2'})

        yield bucket


@pytest.mark.asyncio
async def test_warm_is_resilient(when, fake_model_files_in_s3):
    actual_boto3_session = boto3.Session()
    (when(boto3).Session(**kwargs)
                .thenRaise(Exception('this should not crash things'))
                .thenReturn(actual_boto3_session))
    await keeper_of_the_state.warm_caches()


def test_archive_model_writes_models_to_historical_and_latest_s3_paths(model_bucket, fake_model):
    year, month, day = 2020, 1, 14
    with freeze_time(f'{year}-{month:0>2}-{day:0>2} 14:00:00'):
        keeper_of_the_state.archive_model(fake_model)

    expected_archive_key_prefixes = [
        MODELS_DIR_LATEST,
        f'{MODELS_DIR_ROOT}/historical/{year}-{month:0>2}/{year}-{month:0>2}-{day:0>2}'
    ]

    for expected_key_prefix in expected_archive_key_prefixes:
        expected_model_key = f'{expected_key_prefix}/{MODEL_FILE_NAME}'
        unpickled_model = pickle.loads(model_bucket.Object(expected_model_key).get()['Body'].read())
        assert (
            joblib.hash(unpickled_model) == joblib.hash(fake_model)
            or unpickled_model == fake_model
        ), (
            f'Model archive at {expected_model_key} did not unpickle into '
            f'its original form.'
        )


def test_archive_model_overwrites_the_latest_model_archive(model_bucket, fake_dataset, fake_model):
    key_for_latest_model_archive = f'{MODELS_DIR_LATEST}/{MODEL_FILE_NAME}'

    with freeze_time('2020-01-14 14:00:00'):
        keeper_of_the_state.archive_model(fake_model)

    latest_model_in_archive = pickle.loads(
        model_bucket.Object(key_for_latest_model_archive).get()['Body'].read()
    )
    assert (
        joblib.hash(latest_model_in_archive) == joblib.hash(fake_model)
        or latest_model_in_archive == fake_model
    )

    new_model = ParkingAvailabilityModel()
    new_model.train(fake_dataset.sample(frac=0.5).reset_index(drop=True))

    with freeze_time('2020-01-15 14:00:00'):
        keeper_of_the_state.archive_model(new_model)

    latest_model_in_archive = pickle.loads(
        model_bucket.Object(key_for_latest_model_archive).get()['Body'].read()
    )
    assert (
        joblib.hash(latest_model_in_archive) == joblib.hash(new_model)
        or latest_model_in_archive == new_model
    )
    assert len(list(model_bucket.objects.filter(Prefix=MODELS_DIR_LATEST))) == 1