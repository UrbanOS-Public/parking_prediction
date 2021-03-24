import datetime as dt
import pickle
import time
from typing import Iterable

import hypothesis.strategies as st
import joblib
import pendulum
from hypothesis import given

from app.constants import DAY_OF_WEEK, HOURS_START, TIME_ZONE, UNENFORCED_DAYS
from app.data_formats import APIPredictionRequest
from app.model import ModelFeatures
from app.predictor import to_api_format
from tests.conftest import ALL_VALID_ZONE_IDS

DATETIME_DURING_HOURS_OF_OPERATION = st.builds(
    dt.datetime.combine,
    date=st.dates(
        min_value=dt.date(2020, 9, 7),
        max_value=dt.date(2020, 9, 19)
    ).filter(
        lambda dt: DAY_OF_WEEK(dt.weekday()) not in UNENFORCED_DAYS
    ),
    time=st.times(HOURS_START, pendulum.time(21, 59, 59, 999999)),
    tzinfo=st.sampled_from([TIME_ZONE, None])
)

VALID_ZONE_IDS = st.lists(
    st.sampled_from(ALL_VALID_ZONE_IDS),
    min_size=1,
    max_size=20
)


@given(
    timestamp=DATETIME_DURING_HOURS_OF_OPERATION,
    zone_ids=VALID_ZONE_IDS
)
def test_ModelFeatures_can_be_derived_from_prediction_APIPredictionRequest_during_hours_of_operation(timestamp, zone_ids):
    prediction_request = APIPredictionRequest(timestamp=timestamp, zone_ids=zone_ids)
    samples_batch = ModelFeatures.from_request(prediction_request)
    assert isinstance(samples_batch, Iterable)
    assert all(isinstance(sample, ModelFeatures) for sample in samples_batch)


@given(
    timestamp=DATETIME_DURING_HOURS_OF_OPERATION,
    zone_ids=VALID_ZONE_IDS
)
def test_ParkingAvailabilityModel_returns_one_prediction_per_valid_zone_id(timestamp, zone_ids, fake_model, with_warmup):
    prediction_request = APIPredictionRequest(timestamp=timestamp, zone_ids=zone_ids)
    samples_batch = ModelFeatures.from_request(prediction_request)

    predictions = fake_model.predict(samples_batch)
    assert set(predictions.keys()) == set(zone_ids)


def test_ParkingAvailabilityModel_is_picklable(fake_model):
    pickle.dumps(fake_model)


def test_ParkingAvailabilityModel_unpickles_into_the_same_model(fake_model):
    pickled_fake_model = pickle.dumps(fake_model)
    unpickled_pickled_fake_model = pickle.loads(pickled_fake_model)
    assert (
        joblib.hash(unpickled_pickled_fake_model) == joblib.hash(fake_model)
        or unpickled_pickled_fake_model == fake_model
    )


def test_ParkingAvailabilityModel_predict_runs_in_under_a_second(fake_model):
    """
    A poor man's profiler. It's best run after cranking the size of the
    fake_dataset fixture up to 11 (500+ unique zone IDs, a year of fake
    records, etc.).
    """
    timestamp = dt.datetime(
        year=2020,
        month=10,
        day=20,
        hour=15,
        minute=25,
        second=35,
        tzinfo=TIME_ZONE
    )
    start_time = time.time()
    to_api_format(
    fake_model.predict(
        ModelFeatures.from_request(
            APIPredictionRequest(
                timestamp=timestamp,
                zone_ids=ALL_VALID_ZONE_IDS
            )
        )
    ))
    end_time = time.time()
    assert end_time - start_time < 1
