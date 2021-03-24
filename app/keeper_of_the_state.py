import asyncio
import logging
import os
import pickle
import sys
from datetime import date
from io import BytesIO
from itertools import starmap
from typing import TYPE_CHECKING, List, Optional

import backoff
import botocore
import requests

from app import auth_provider
from app.constants import DISCOVERY_API_QUERY_URL, MODEL_FILE_NAME
from app.util import log_exception

if TYPE_CHECKING:
    from app.model import ParkingAvailabilityModel

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
if sys.stdout.isatty():
    LOGGER.addHandler(logging.StreamHandler(sys.stdout))

TTL_HOURS = 12
TTL_SECONDS = TTL_HOURS * 60 * 60

MODELS_DIR_ROOT = 'models'
MODELS_DIR_LATEST = f'{MODELS_DIR_ROOT}/latest'

MODELS = {}
ZONES = []


def provide_model(model_tag='latest') -> Optional['ParkingAvailabilityModel']:
    return MODELS.get(model_tag, None)


def provide_zones() -> List[str]:
    return ZONES


def warm_caches_synchronously(extra_model_tags=[]):
    LOGGER.info('Getting models for prediction')
    asyncio.get_event_loop().run_until_complete(warm_caches(extra_model_tags))
    LOGGER.info('Done getting models for prediction')


def historical_model_name(model_date):
    return f'historical/{model_date.strftime("%Y-%m")}/{model_date.isoformat()}'


@backoff.on_exception(backoff.expo, Exception, on_backoff=log_exception)
async def warm_caches(extra_model_tags=[]):
    model_tags = ['latest'] + get_comparative_models() + extra_model_tags
    model_fetches = [_fetch_all(model_tag) for model_tag in model_tags]

    fetched_models = await asyncio.gather(*model_fetches)

    for tag, model in fetched_models:
        MODELS[tag] = model or {}

    ZONES.clear()
    ZONES.extend(_fetch_zone_ids())


async def fetch_state_periodically():
    while True:
        await asyncio.sleep(TTL_SECONDS)
        await warm_caches()


async def _fetch_all(model_tag):
    bucket = await asyncio.get_event_loop().run_in_executor(None, _bucket_for_environment)

    model_key = f'{MODELS_DIR_ROOT}/{model_tag}/{MODEL_FILE_NAME}'

    async def _check_exists(cluster_id, path):
        return cluster_id, path, await asyncio.get_event_loop().run_in_executor(None, _model_exists_at_path, bucket, path)

    def _filter_exists(path_tuple):
        _, _, exists = path_tuple
        return exists

    async def _model_download(id, path, _exists):
        return await asyncio.get_event_loop().run_in_executor(None, _fetch_model, id, bucket, path)

    model_paths = [('irrelevant, unused id', model_key)]

    model_exists_futures = list(starmap(_check_exists, model_paths))
    model_exists = await asyncio.gather(*model_exists_futures)
    existing_model_paths = filter(_filter_exists, model_exists)

    model_futures = list(starmap(_model_download, existing_model_paths))
    models = await asyncio.gather(*model_futures)

    return model_tag, models[0]


def _model_exists_at_path(bucket, path):
    try:
        LOGGER.debug(f'checking if model exists at {path}')
        bucket.Object(path).load()
        LOGGER.debug(f'done checking model exists at {path}')
        return True
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise e


def _fetch_model(id, bucket, model_path):
    LOGGER.debug(f'Fetching model: {model_path}')
    with BytesIO() as in_memory_file:
        bucket.download_fileobj(model_path, in_memory_file)
        in_memory_file.seek(0)
        LOGGER.debug(f'Done fetching model {model_path}')
        return pickle.load(in_memory_file)


def archive_model(model):
    bucket = _bucket_for_environment()

    dated_path = f'{MODELS_DIR_ROOT}/{historical_model_name(date.today())}'

    _delete_models_in_path(bucket, dated_path)
    _delete_models_in_path(bucket, MODELS_DIR_LATEST)

    model_serialized = pickle.dumps(model)

    LOGGER.info(f'Loading {MODEL_FILE_NAME} into {bucket.name}')
    bucket.put_object(Body=model_serialized, Key=f'{MODELS_DIR_LATEST}/{MODEL_FILE_NAME}')
    bucket.put_object(Body=model_serialized, Key=f'{dated_path}/{MODEL_FILE_NAME}')


def _delete_models_in_path(bucket, path):
    def _convert_to_delete_syntax(object_summary):
        return {'Key': object_summary.key}

    existing_models = bucket.objects.filter(Prefix=f'{path}/')
    models_to_delete = list(map(_convert_to_delete_syntax, existing_models))

    if models_to_delete:
        bucket.delete_objects(Delete={'Objects': models_to_delete})


def _bucket_for_environment():
    s3 = auth_provider.authorized_s3_resource()
    environment = os.environ.get('SCOS_ENV', 'dev')
    return s3.Bucket(f'{environment}-parking-prediction')


def _fetch_zone_ids():
    data = ('SELECT DISTINCT "pm zone number" '
            'FROM city_of_columbus__columbus_parking_meters')

    with requests.post(DISCOVERY_API_QUERY_URL, data=data) as response:
        return sorted(response.text.strip().split('\n')[1:])


def get_comparative_models():
    from_env = os.getenv('COMPARED_MODELS', '')

    if from_env == '':
        return []
    else:
        return from_env.split(',')
