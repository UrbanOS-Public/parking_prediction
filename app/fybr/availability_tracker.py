import datetime
from collections import defaultdict
from copy import deepcopy
from functools import reduce
from itertools import starmap
from operator import itemgetter

from dateutil import parser, tz

INVALID_AFTER_MINUTES = 5


def availability(zone_index, zone_id, timestamp):
    def _too_old(_id, details):
        last_seen = details['last_seen']
        if not last_seen:
            return True

        cutoff = timestamp - datetime.timedelta(minutes=INVALID_AFTER_MINUTES)

        return last_seen < cutoff

    all_meters = zone_index.get(zone_id, {}).get('meters', {})

    if not all_meters or any(starmap(_too_old, all_meters.items())):
        return None

    all_meters_count = len(all_meters)
    available_meters = [m for m, d in all_meters.items() if not d['occupied']]
    available_meters_count = len(available_meters)

    return round(available_meters_count / all_meters_count, 4)


def create_message_handler(meter_index):
    def _reducer(index, message):
        if not message['event'] == 'update':
            return index

        record = message['payload']
        meter_id = record['id']

        zone_id = meter_index.get(meter_id)
        if not zone_id:
            return index

        last_seen = parser.isoparse(record['time_of_ingest']).replace(tzinfo=tz.tzutc())
        occupied = record['occupancy'] == 'OCCUPIED'

        index[zone_id]['meters'][meter_id] = {
            'occupied': occupied,
            'last_seen': last_seen
        }

        return index

    def _handler(messages, zone_index):
        return reduce(_reducer, messages, deepcopy(zone_index))

    return _handler


def create_tracking_indices(meter_and_zone_list):
    meter_index = {meter['meter_id']: meter['zone_id']
                   for meter in meter_and_zone_list}

    zone_index = defaultdict(lambda: {'meters': {}})
    default_meter_status = {'occupied': None, 'last_seen': None}

    get_zone_and_meter_ids = itemgetter('zone_id', 'meter_id')
    for zone_id, meter_id in map(get_zone_and_meter_ids, meter_and_zone_list):
        zone_index[zone_id]['meters'][meter_id] = default_meter_status

    return zone_index, meter_index
