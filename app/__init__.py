import asyncio
import logging
from datetime import datetime
from typing import List, Union

from pytz import timezone
from quart import Quart, jsonify, request

from app import keeper_of_the_state, now_adjusted, predictor
from app.fybr import zone_info
from app.fybr.availability_provider import FybrAvailabilityProvider

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

app = Quart(__name__)

app.fybr_availability_provider = FybrAvailabilityProvider(
    'wss://streams.smartcolumbusos.com/socket/websocket',
    []
)


@app.before_serving
async def startup():
    LOGGER.info('starting API')
    app.fybr_availability_provider = FybrAvailabilityProvider(
        'wss://streams.smartcolumbusos.com/socket/websocket',
        zone_info.meter_and_zone_list()
    )
    loop = asyncio.get_event_loop()

    LOGGER.info('Scheduling availability cache to be filled from stream')
    app.fybr_availability_streamer = loop.create_task(app.fybr_availability_provider.handle_websocket_messages())
    LOGGER.info('Scheduling model and zone caches to be re-warmed periodically')
    app.model_fetcher = loop.create_task(keeper_of_the_state.fetch_state_periodically())
    LOGGER.info('Finished starting API')

    LOGGER.info('Warming model and zone caches')
    await keeper_of_the_state.warm_caches()
    LOGGER.info('Done warming model and zone caches')


@app.after_serving
async def shutdown():
    app.fybr_availability_streamer.cancel()
    app.model_fetcher.cancel()


@app.route('/healthcheck')
async def healthcheck():
    return 'OK'


@app.route('/api/v1/predictions')
async def predictions():
    now = now_adjusted.adjust(datetime.now(timezone('US/Eastern')))
    zone_ids = _parse_zone_ids(request.args.get('zone_ids'))

    availability = predictor.predict(now, zone_ids)

    prediction_transforms = [
        _override_availability_predictions_with_known_values,
        predictor.to_api_format,
        jsonify
    ]
    for transform in prediction_transforms:
        availability = transform(availability)

    return availability


def _parse_zone_ids(request_zone_ids_field) -> Union[List[str], str]:
    if (zone_param := request_zone_ids_field) is not None:
        zone_ids = zone_param.split(',')
    else:
        zone_ids = 'All'
    return zone_ids


def _override_availability_predictions_with_known_values(predictions):
    sensor_data = app.fybr_availability_provider.get_all_availability()

    for key in predictions.keys() & sensor_data.keys():
        predictions[key] = sensor_data[key]
    return predictions


@app.route('/api/v0/predictions')
async def predictions_comparative():
    comparative_models = keeper_of_the_state.get_comparative_models()

    if len(comparative_models) == 0:
        return jsonify([])

    now = now_adjusted.adjust(datetime.now(timezone('US/Eastern')))
    zone_ids = _parse_zone_ids(request.args.get('zone_ids'))

    results = predictor.predict_with(comparative_models, now, zone_ids)
    return jsonify(results)


@app.route('/api/v1/availability')
async def availability():
    return jsonify(app.fybr_availability_provider.get_all_availability())


if __name__ == '__main__':
    app.run()
