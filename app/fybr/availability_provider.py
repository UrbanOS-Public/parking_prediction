import asyncio
import json
from copy import deepcopy
from datetime import datetime

import backoff
import websockets
from dateutil import tz

import app.fybr.availability_tracker as fybr_availability_tracker
from app.util import log_exception

DATASET_STREAM_SYSTEM_NAME = 'fybr__short_north_parking_occupancy'
JOIN_MESSAGE = json.dumps({
    'topic': f'streaming:{DATASET_STREAM_SYSTEM_NAME}',
    'event': 'phx_join',
    'payload': {},
    'ref': '1'
})


class FybrAvailabilityProvider:
    def __init__(self, uri, meter_and_zone_list):
        zone_index, meter_index = fybr_availability_tracker.create_tracking_indices(meter_and_zone_list)
        self.uri = uri
        self.zone_index = zone_index
        self.meter_index = meter_index

    @backoff.on_exception(
        backoff.expo, Exception, on_backoff=log_exception, max_value=60)
    async def handle_websocket_messages(self):
        async with websockets.connect(self.uri) as websocket:
            await websocket.send(JOIN_MESSAGE)

            handler = fybr_availability_tracker.create_message_handler(self.meter_index)

            async for message_string in websocket:
                message = json.loads(message_string)
                self.zone_index = await asyncio.get_event_loop().run_in_executor(
                    None, handler, [message], self.zone_index)

    def get_all_availability(self):
        availabilities = {}
        now = datetime.now(tz.tzutc())
        zone_index = deepcopy(self.zone_index)

        for zone_id in zone_index:
            availability = fybr_availability_tracker.availability(zone_index, zone_id, now)
            if availability is not None:
                availabilities[zone_id] = availability

        return availabilities