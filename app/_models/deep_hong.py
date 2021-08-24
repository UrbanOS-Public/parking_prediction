import logging
import sys
from typing import ForwardRef, List, Mapping, MutableMapping

import numpy as np
import pandas as pd
from pydantic import BaseModel, conlist, validate_arguments, validator
from sklearn.neural_network import MLPRegressor

from app._models.abstract_model import Model, ModelFeatures
from app.constants import DAY_OF_WEEK, HOURS_END, HOURS_START, UNENFORCED_DAYS
from app.data_formats import APIPredictionRequest

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
if sys.stdout.isatty():
    LOGGER.addHandler(logging.StreamHandler(sys.stdout))


TOTAL_SEMIHOURS = 2 * (HOURS_END - HOURS_START).hours + abs(HOURS_END.minute - HOURS_START.minute) // 30
TOTAL_ENFORCEMENT_DAYS = 7 - len(UNENFORCED_DAYS)


ModelFeaturesv0EarlyAccessPreRelease = ForwardRef('ModelFeaturesv0EarlyAccessPreRelease')


class ModelFeaturesv0EarlyAccessPreRelease(BaseModel):
    zone_id: str
    semihour_onehot: conlist(int,
                             min_items=TOTAL_SEMIHOURS - 1,
                             max_items=TOTAL_SEMIHOURS - 1)
    dayofweek_onehot: conlist(int,
                              min_items=TOTAL_ENFORCEMENT_DAYS - 1,
                              max_items=TOTAL_ENFORCEMENT_DAYS - 1)

    @validator('semihour_onehot', 'dayofweek_onehot')
    def one_hot_encoded(cls, feature):
        number_of_ones = (np.array(feature) == 1).sum()
        number_of_zeros = (np.array(feature) == 0).sum()
        assert number_of_ones + number_of_zeros == len(feature), \
            'Input should consist solely of 0s and 1s.'
        assert number_of_ones <= 1, \
            'Input must contain at most one 1.'
        return feature

    @staticmethod
    @validate_arguments
    def from_request(request: APIPredictionRequest) -> List[ModelFeatures]:
        """
        Convert a prediction request to the input format expected by a parking
        availability model.

        Parameters
        ----------
        request : APIPredictionRequest
            The prediction request to transform into model features

        Returns
        -------
        list of ModelFeaturesv0EarlyAccessPreRelease
            A set of features that can be passed into the `predict` method of a
            `ParkingAvailabilityModelv0EarlyAccessPreRelease`.
        """
        timestamp = request.timestamp

        hour_onehot = np.array([timestamp.hour == hour for hour in range(8, 22)])
        minute_bin_onehot = np.array([30 * minute_bin <= timestamp.minute < 30 * (minute_bin + 1) for minute_bin in range(2)])
        semihour_onehot = (hour_onehot[:, None] & np.array(minute_bin_onehot)).flatten()[1:]

        enforcement_days = [day.value for day in DAY_OF_WEEK if day not in UNENFORCED_DAYS]
        dayofweek_onehot = np.array([
            enforcement_days.index(timestamp.weekday()) == day_index
            for day_index in range(TOTAL_ENFORCEMENT_DAYS)
        ])[1:]

        return [ModelFeaturesv0EarlyAccessPreRelease(zone_id=zone_id,
                                                     semihour_onehot=semihour_onehot.astype(int).tolist(),
                                                     dayofweek_onehot=dayofweek_onehot.astype(int).tolist())
                for zone_id in request.zone_ids]


ModelFeaturesv0EarlyAccessPreRelease.update_forward_refs()


class ParkingAvailabilityModelv0EarlyAccessPreRelease(Model):
    def __init__(self):
        super().__init__()
        self._zone_models: MutableMapping[str, MLPRegressor] = {}
        self._supported_zones = []

    def __getstate__(self):
        return {
            'zone_models': self._zone_models,
            'supported_zones': self.supported_zones
        }

    def __setstate__(self, state):
        self._zone_models = state['zone_models']
        self._supported_zones = state['supported_zones']

    @property
    def supported_zones(self):
        return self._supported_zones

    def train(self, training_data: pd.DataFrame) -> None:
        zone_id, X, y = (
            training_data
                .assign(
                    available_rate=lambda df: 1 - df.occu_cnt_rate,
                    dayofweek=lambda df: df.semihour.dt.dayofweek.astype('category'),
                    semihour=lambda df: pd.Series(
                        zip(df.semihour.dt.hour, df.semihour.dt.minute),
                        dtype='category', index=df.index
                    )
                )
                .pipe(
                    lambda df: (
                        df.zone_id,
                        pd.get_dummies(df.loc[:, ['semihour', 'dayofweek']],
                                       drop_first=True),
                        df.available_rate
                    )
                )
        )

        self._supported_zones = zone_id.unique()
        for zone in self.supported_zones:
            LOGGER.info(f'Processing zone {zone}')
            X_cluster = X[zone_id == zone]
            y_cluster = y[zone_id == zone]

            LOGGER.info(f'Total (row, col) counts: {X_cluster.shape}')
            mlp = MLPRegressor(hidden_layer_sizes=(50, 50), activation='relu')
            mlp.fit(X_cluster, y_cluster)
            if np.issubdtype(type(zone), np.float64):
                zone = str(int(zone))
            self._zone_models[zone] = mlp

        LOGGER.info(f'Successfully trained {len(self._zone_models)} models')

    @validate_arguments
    def predict(self, samples_batch: List[ModelFeaturesv0EarlyAccessPreRelease]) -> Mapping[str, float]:
        requested_zone_ids = [sample.zone_id for sample in samples_batch
                              if sample.zone_id in self.supported_zones]

        regressor_feature_array = np.asarray([
            sample.semihour_onehot + sample.dayofweek_onehot
            for sample in samples_batch
        ])
        return {
            zone_id: self._zone_models[zone_id]
                         .predict(regressor_feature_array)
                         .clip(0, 1)[0]
            for zone_id in requested_zone_ids
        }