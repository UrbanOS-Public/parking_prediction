import datetime as dt

import hypothesis.strategies as st
from hypothesis import given

from app import now_adjusted
from app.constants import TIME_ZONE


def local_datetimes(
    *,
    dates=st.dates(),
    times: st.SearchStrategy,
    timezones=st.one_of(st.just(TIME_ZONE), st.none())
) -> st.SearchStrategy:
    return st.builds(dt.datetime.combine,
                     date=dates, time=times, tzinfo=timezones)


@given(local_datetimes(times=st.times(dt.time(7, 30), dt.time(7, 59, 59, 999999))))
def test_times_between_0730_and_0759_are_adjusted_to_0800(datetime):
    adjusted = now_adjusted.adjust(datetime)
    expected = dt.datetime.combine(datetime.date(), dt.time(8, 0), datetime.tzinfo)

    assert adjusted == expected


@given(local_datetimes(times=st.times(dt.time(22, 0, 0, 1), dt.time(22, 30))))
def test_times_between_2200_and_2230_are_adjusted_to_2200(datetime):
    adjusted = now_adjusted.adjust(datetime)
    expected = dt.datetime.combine(datetime.date(), dt.time(22, 0), datetime.tzinfo)

    assert adjusted == expected


@given(local_datetimes(times=st.times(max_value=dt.time(7, 29, 59, 999999))))
def test_times_earlier_than_730_are_not_adjusted(datetime):
    adjusted = now_adjusted.adjust(datetime)

    assert adjusted == datetime


@given(local_datetimes(times=st.times(min_value=dt.time(8, 0), max_value=dt.time(22, 0))))
def test_times_during_hours_of_operation_are_not_adjusted(datetime):
    adjusted = now_adjusted.adjust(datetime)

    assert adjusted == datetime


@given(st.times(min_value=dt.time(22, 30, 0, 1)))
def test_times_after_2230_are_not_adjusted(time):
    given = dt.datetime.combine(dt.date(2020, 2, 7), time)
    adjusted = now_adjusted.adjust(given)

    assert adjusted == given