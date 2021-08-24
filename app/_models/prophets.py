import logging
import sys
from datetime import datetime
from itertools import starmap
from typing import ForwardRef, List, Mapping, MutableMapping

import pandas as pd
from fbprophet import Prophet
from fbprophet.serialize import model_from_json, model_to_json
from pydantic import BaseModel, constr, validate_arguments
from tqdm import tqdm

from app._models.abstract_model import Model
from app.data_formats import APIPredictionRequest

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
if sys.stdout.isatty():
    LOGGER.addHandler(logging.StreamHandler(sys.stdout))


ProphetableFeatures = ForwardRef('ProphetableFeatures')


class ProphetableFeatures(BaseModel):
    zone_id: constr(min_length=1)
    at: datetime

    @staticmethod
    @validate_arguments
    def from_request(request: APIPredictionRequest) -> List[ProphetableFeatures]:
        f"""
        Convert a prediction request to the input format expected by a parking
        availability model.

        Parameters
        ----------
        request : APIPredictionRequest
            The prediction request to transform into model features

        Returns
        -------
        list of ProphetableFeatures
            A set of features that can be passed into the `predict` method of a
            `ProphetableFeatures`.
        """
        return [ProphetableFeatures(zone_id=zone_id, at=request.timestamp)
                for zone_id in request.zone_ids]


ProphetableFeatures.update_forward_refs()


class ParkingProphet(Model):
    def __init__(self):
        super().__init__()
        self._zone_clusterer: Mapping[str, str] = lambda zone_id: zone_id
        self._cluster_models: MutableMapping[str, Prophet] = {}
        self._zone_models: MutableMapping[str, Prophet] = {}
        self._supported_zones: List[str] = []

    def __getstate__(self):
        def _to_json(name, model):
            return name, model_to_json(model)

        serializable_cluster_models = dict(starmap(_to_json, self._cluster_models.items()))
        serializable_zone_models = dict(starmap(_to_json, self._zone_models.items()))

        return {
            'zone_clusterer': self._zone_clusterer,
            'cluster_models': serializable_cluster_models,
            'zone_models': serializable_zone_models,
            'supported_zones': self.supported_zones
        }

    def __setstate__(self, state):
        def _from_json(name, jsonny):
            return name, model_from_json(jsonny)

        deserialized_cluster_models = dict(starmap(_from_json, state['cluster_models'].items()))
        deserialized_zone_models = dict(starmap(_from_json, state['zone_models'].items()))

        self._zone_clusterer = state['zone_clusterer']
        self._cluster_models = deserialized_cluster_models
        self._zone_models = deserialized_zone_models
        self._supported_zones = state['supported_zones']

    @property
    def supported_zones(self):
        return self._supported_zones

    def train(self, training_data: pd.DataFrame) -> None:
        self._supported_zones = list(training_data.zone_id.unique())
        self._zone_clusterer = self._derive_zone_clusters(training_data)
        prophet_features = self._derive_prophet_features(training_data, self._zone_clusterer)
        self._cluster_models = self._train_cluster_models(prophet_features)
        self._zone_models = self._train_zone_models(prophet_features)
        LOGGER.info(f'Successfully trained {len(self._zone_models)} models')

    def predict(self, samples_batch: List[ProphetableFeatures]) -> Mapping[str, float]:
        requested_zone_ids = [sample.zone_id for sample in samples_batch
                              if sample.zone_id in self.supported_zones]

        if not samples_batch:
            return {}
        requested_time = pd.Timestamp(samples_batch[0].at.replace(tzinfo=None))
        future = pd.DataFrame({'ds': [requested_time]})

        cluster_available_rates = {
            self._zone_clusterer[zone_id]: self._cluster_models[self._zone_clusterer[zone_id]].predict(future)
            for zone_id in requested_zone_ids
        }
        unwrap = lambda x: x[0] if x else x
        result = {
            zone_id: unwrap(self._zone_models[zone_id].predict(
                future.assign(
                    cluster_available_rate=cluster_available_rates[
                        self._zone_clusterer[zone_id]
                    ].yhat.clip(0, 1)
                )
            ).yhat.clip(0, 1).tolist())
            for zone_id in requested_zone_ids
        }
        return result

    def _derive_zone_clusters(self, training_data: pd.DataFrame) -> Mapping[str, str]:
        zone_and_cluster_id_pairs = pd.concat(
            [training_data.zone_id,
             training_data.zone_id.astype(str).str[:2].rename('cluster_id')],
            axis='columns'
        ).drop_duplicates()
        zone_to_cluster_map = dict(zip(zone_and_cluster_id_pairs.zone_id,
                                       zone_and_cluster_id_pairs.cluster_id))
        return zone_to_cluster_map

    def _derive_prophet_features(self, training_data: pd.DataFrame, zone_clusterer: Mapping[str, str]) -> pd.DataFrame:
        training_data = training_data.assign(
            available_rate=lambda df: (1 - df.occu_cnt_rate).clip(0, 1),
            available_count=lambda df: (df.available_rate * df.total_cnt).round().astype(int),
            cluster_id=lambda df: df.zone_id.map(lambda zone_id: zone_clusterer[zone_id])
        )
        training_data_by_cluster = training_data.groupby(['cluster_id', 'semihour'])
        training_data['cluster_available_rate'] = (
            training_data_by_cluster.available_count.transform(lambda s: s.sum())
            / training_data_by_cluster.total_cnt.transform(lambda s: s.sum())
        ).clip(0, 1)
        used_columns = ['cluster_id', 'zone_id', 'ds', 'available_rate',
                        'cluster_available_rate']
        return (training_data.rename(columns={'semihour': 'ds'})
                             .loc[:, used_columns])

    def _train_zone_models(self, refined_training_data: pd.DataFrame) -> Mapping[str, Prophet]:
        refined_training_data = refined_training_data.rename(
            columns={'available_rate': 'y'}
        )
        zone_models = {}
        for zone_id, zone_training_data in tqdm(
                refined_training_data.groupby('zone_id'),
                desc='Training Parking Zone Models',
                leave=False
        ):
            zone_model = Prophet(yearly_seasonality=False)
            zone_model.add_regressor('cluster_available_rate')
            zone_model.fit(zone_training_data)
            zone_models[zone_id] = zone_model
        return zone_models

    def _train_cluster_models(self, refined_training_data: pd.DataFrame) -> Mapping[str, Prophet]:
        refined_training_data = refined_training_data.rename(
            columns={'cluster_available_rate': 'y'}
        )
        cluster_models = {}
        for cluster_id, cluster_training_data in tqdm(
                refined_training_data.groupby('cluster_id'),
                desc='Training Clustered Parking Zone Models',
                leave=False
        ):
            cluster_model = Prophet(yearly_seasonality=False)
            cluster_model.fit(cluster_training_data)
            cluster_models[cluster_id] = cluster_model
        return cluster_models