import logging
import sys
from datetime import datetime
from typing import ForwardRef, List, Mapping

import numpy as np
import pandas as pd
from pydantic import BaseModel, constr, validate_arguments

from app._models.abstract_model import Model
from app.data_formats import APIPredictionRequest

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
if sys.stdout.isatty():
    LOGGER.addHandler(logging.StreamHandler(sys.stdout))

AverageFeatures = ForwardRef('AverageFeatures')


class AverageFeatures(BaseModel):
    zone_id: constr(min_length=1)
    at: datetime

    @staticmethod
    @validate_arguments
    def from_request(request: APIPredictionRequest) -> List[AverageFeatures]:
        f"""
        Convert a prediction request to the input format expected by a parking
        availability model.

        Parameters
        ----------
        request : APIPredictionRequest
            The prediction request to transform into model features

        Returns
        -------
        list of AverageFeatures
            A set of features that can be passed into the `predict` method of a
            `AverageFeatures`.
        """
        return [AverageFeatures(zone_id=zone_id, at=request.timestamp)
                for zone_id in request.zone_ids]


AverageFeatures.update_forward_refs()


class AvailabilityAverager(Model):
    def __init__(self, weeks_to_average=1):
        super().__init__()
        self._supported_zones: List[str] = []
        self._weeks_to_average = weeks_to_average

    def __eq__(self, other):
        if not isinstance(other, AvailabilityAverager):
            return False
        rolling_average_ids = set(self._rolling_averages.keys())
        other_rolling_average_ids = set(other._rolling_averages.keys())
        return (
            (rolling_average_ids == other_rolling_average_ids) and
            all(
                (
                    self._rolling_averages[key] == other._rolling_averages[key]
                ).all().all()
                for key in rolling_average_ids
            ) and
            (self.supported_zones == other.supported_zones) and
            (self.weeks_to_average == other.weeks_to_average)
        )

    def __getstate__(self):
        return {
            'rolling_averages': self._rolling_averages,
            'supported_zones': self.supported_zones,
            'weeks_to_average': self.weeks_to_average
        }

    def __setstate__(self, state):
        self._rolling_averages = state['rolling_averages']
        self._supported_zones = state['supported_zones']
        self._weeks_to_average = state['weeks_to_average']

    @property
    def supported_zones(self) -> List[str]:
        return self._supported_zones

    @property
    def weeks_to_average(self) -> int:
        return self._weeks_to_average

    def train(self, training_data: pd.DataFrame) -> None:
        training_data = training_data.assign(
            available_rate=lambda df: 1 - df.occu_cnt_rate,
            dayofweek=lambda df: df.semihour.dt.dayofweek.astype('category'),
            semihour_tuples=lambda df: pd.Series(
                zip(df.semihour.dt.hour, df.semihour.dt.minute),
                dtype='category', index=df.index
            )
        )
        LOGGER.info(f"At start, {len(training_data)}")
        training_data = training_data.sort_values(
            ['zone_id', 'dayofweek', 'semihour_tuples']
        )
        LOGGER.info(f"Sorted, {len(training_data)}")
        training_data[f'available_rate_{self.weeks_to_average:0>2}w'] = (
            training_data.groupby(
                ['zone_id', 'dayofweek', 'semihour_tuples']
            ).available_rate.transform(
                lambda group: group.shift().rolling(self.weeks_to_average, 1).mean()
            ).dropna().clip(0, 1)
        )
        LOGGER.info(f"Grouped?, {len(training_data)}")
        training_data = training_data[
            [
                'zone_id', 'semihour', 'semihour_tuples', 'dayofweek',
                f'available_rate_{self.weeks_to_average:0>2}w'
            ]
        ].dropna()
        LOGGER.info(f"Dropped, {len(training_data)}")
        self._rolling_averages = {
            zone_day_time: data_in_zone_on_day_at_time
            for zone_day_time, data_in_zone_on_day_at_time
            in training_data.groupby(['zone_id', 'dayofweek',
                                      'semihour_tuples'])
        }

        def _get_zone_from_key(key):
            zone_id, _d, _s = key
            return zone_id

        zone_list = list(map(_get_zone_from_key, self._rolling_averages.keys()))
        unique_zone_list = np.unique(np.array(zone_list)).tolist()
        self._supported_zones = unique_zone_list


    # @validate_arguments
    def predict(self, samples_batch: List[AverageFeatures]) -> Mapping[str, float]:
        valid_requests = [sample for sample in samples_batch
                          if sample.zone_id in self.supported_zones]
        # LOGGER.info(f'Samples, {len(samples_batch)}')
        # LOGGER.info(f'Valid, {len(valid_requests)}')
        predictions = {}
        for sample in valid_requests:
            sample_timestamp = pd.Timestamp(sample.at)
            sample_semihour_tuple = (
                sample_timestamp.hour,
                30 * (sample_timestamp.minute // 30)
            )
            rolling_averages = self._rolling_averages.get((
                sample.zone_id,
                sample_timestamp.dayofweek,
                sample_semihour_tuple
            ), pd.DataFrame())

            if rolling_averages.empty:
                # LOGGER.warn(f'Sample {sample.zone_id} empty')
                # LOGGER.warn(f'Test {test}')
                continue

            rolling_averages = rolling_averages.assign(
                date_diff=lambda df: -(
                    df.semihour.dt.date - sample_timestamp.date()
                )
            ).loc[
                lambda df: df.date_diff == df.date_diff.min()
            ]

            try:
                predictions[sample.zone_id] = rolling_averages[f'available_rate_{self.weeks_to_average:0>2}w'].iloc[0]
            except IndexError:
                continue

        return predictions
