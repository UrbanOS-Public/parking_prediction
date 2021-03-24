import asyncio
from contextlib import asynccontextmanager

import pytest
import websockets
from freezegun import freeze_time
from mockito import mock, patch, unstub

from app.fybr.availability_provider import FybrAvailabilityProvider
from tests.fake_websocket_server import create_fake_server, update_event


@pytest.yield_fixture(scope='function')
def event_loop(request):
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.mark.asyncio
async def test_can_consume_stream_from_server(event_loop):
    uri='ws://localhost:5001/socket/websocket'
    meter_and_zone_list = [
        {'meter_id': '9861', 'zone_id': '0001'},
        {'meter_id': '9862', 'zone_id': '0001'},
        {'meter_id': '9863', 'zone_id': '0001'},
        {'meter_id': '9864', 'zone_id': '0001'},

        {'meter_id': '9871', 'zone_id': '0002'},
        {'meter_id': '9872', 'zone_id': '0002'},
        {'meter_id': '9873', 'zone_id': '0002'},

        {'meter_id': '9881', 'zone_id': '0003'},
        {'meter_id': '9882', 'zone_id': '0003'}
    ]
    availability_provider = FybrAvailabilityProvider(uri, meter_and_zone_list)

    first_chunk_of_messages = [
        update_event({'id': '9861', 'occupancy': 'UNOCCUPIED', 'time_of_ingest': '2020-05-21T18:00:00.000000'}),
        update_event({'id': '9862', 'occupancy': 'UNOCCUPIED', 'time_of_ingest': '2020-05-21T18:00:00.000000'}),
        update_event({'id': '9863', 'occupancy': 'UNOCCUPIED', 'time_of_ingest': '2020-05-21T18:00:00.000000'}),
        update_event({'id': '9864', 'occupancy': 'UNOCCUPIED', 'time_of_ingest': '2020-05-21T18:00:00.000000'}),

        update_event({'id': '9871', 'occupancy': 'UNOCCUPIED', 'time_of_ingest': '2020-05-21T18:00:00.000000'}),
        update_event({'id': '9872', 'occupancy': 'OCCUPIED', 'time_of_ingest': '2020-05-21T18:00:00.000000'}),
        update_event({'id': '9873', 'occupancy': 'UNOCCUPIED', 'time_of_ingest': '2020-05-21T18:00:00.000000'}),

        update_event({'id': '9881', 'occupancy': 'OCCUPIED', 'time_of_ingest': '2020-05-21T18:00:00.000000'}),
        update_event({'id': '9882', 'occupancy': 'UNOCCUPIED', 'time_of_ingest': '2020-05-21T18:00:00.000000'})
    ]

    second_chunk_of_messages = [
        update_event({'id': '9861', 'occupancy': 'UNOCCUPIED', 'time_of_ingest': '2020-05-21T18:00:15.000000'}),
        update_event({'id': '9862', 'occupancy': 'OCCUPIED', 'time_of_ingest': '2020-05-21T18:00:15.000000'}),
        update_event({'id': '9863', 'occupancy': 'OCCUPIED', 'time_of_ingest': '2020-05-21T18:00:15.000000'}),
        update_event({'id': '9864', 'occupancy': 'OCCUPIED', 'time_of_ingest': '2020-05-21T18:00:15.000000'}),

        update_event({'id': '9871', 'occupancy': 'OCCUPIED', 'time_of_ingest': '2020-05-21T18:00:15.000000'}),
        update_event({'id': '9872', 'occupancy': 'OCCUPIED', 'time_of_ingest': '2020-05-21T18:00:15.000000'}),
        update_event({'id': '9873', 'occupancy': 'UNOCCUPIED', 'time_of_ingest': '2020-05-21T18:00:15.000000'}),

        update_event({'id': '9881', 'occupancy': 'UNOCCUPIED', 'time_of_ingest': '2020-05-21T18:00:15.000000'}),
        update_event({'id': '9882', 'occupancy': 'UNOCCUPIED', 'time_of_ingest': '2020-05-21T18:00:15.000000'})
    ]
    third_chunk_of_messages = [
        update_event({'id': '9861', 'occupancy': 'OCCUPIED', 'time_of_ingest': '2020-05-21T18:30:15.000000'}),
        update_event({'id': '9862', 'occupancy': 'OCCUPIED', 'time_of_ingest': '2020-05-21T18:30:15.000000'}),
        update_event({'id': '9863', 'occupancy': 'OCCUPIED', 'time_of_ingest': '2020-05-21T18:30:15.000000'}),
        update_event({'id': '9864', 'occupancy': 'OCCUPIED', 'time_of_ingest': '2020-05-21T18:30:15.000000'}),

        update_event({'id': '9871', 'occupancy': 'UNOCCUPIED', 'time_of_ingest': '2020-05-21T18:30:15.000000'}),
        update_event({'id': '9872', 'occupancy': 'OCCUPIED', 'time_of_ingest': '2020-05-21T18:30:15.000000'}),
        update_event({'id': '9873', 'occupancy': 'UNOCCUPIED', 'time_of_ingest': '2020-05-21T18:30:15.000000'}),

        update_event({'id': '9881', 'occupancy': 'UNOCCUPIED', 'time_of_ingest': '2020-05-21T18:30:15.000000'}),
        update_event({'id': '9882', 'occupancy': 'OCCUPIED', 'time_of_ingest': '2020-05-21T18:30:15.000000'})
    ]

    fake_server = create_fake_server(messages=first_chunk_of_messages + second_chunk_of_messages)
    async with websockets.serve(fake_server, '127.0.0.1', 5001):
        await availability_provider.handle_websocket_messages()

        with freeze_time('2020-05-21T18:05:00.000000'):
            assert availability_provider.get_all_availability() == {
                '0001': 0.25,
                '0002': 0.3333,
                '0003': 1.0
            }

        with freeze_time('2020-05-21T18:30:00.000000'):
            assert availability_provider.get_all_availability() == {}

    fake_server = create_fake_server(messages=third_chunk_of_messages)
    async with websockets.serve(fake_server, '127.0.0.1', 5001):
        await availability_provider.handle_websocket_messages()

        with freeze_time('2020-05-21T18:35:00.000000'):
            availability_provider.get_all_availability() == {
                '0001': 0.0,
                '0002': 0.6667,
                '0003': 0.5
            }


@pytest.mark.asyncio
async def test_recovers_from_errors(event_loop):
    uri='ws://localhost:5001/socket/websocket'
    meter_and_zone_list = [
        {'meter_id': '9861', 'zone_id': '0001'},

        {'meter_id': '9871', 'zone_id': '0002'},

        {'meter_id': '9881', 'zone_id': '0003'}
    ]

    availability_provider = FybrAvailabilityProvider(uri, meter_and_zone_list)

    messages = [
        update_event({'id': '9861', 'occupancy': 'UNOCCUPIED', 'time_of_ingest': '2020-05-21T18:00:00.000000'}),

        update_event({'id': '9871', 'occupancy': 'UNOCCUPIED', 'time_of_ingest': '2020-05-21T18:00:00.000000'}),

        update_event({'id': '9881', 'occupancy': 'OCCUPIED', 'time_of_ingest': '2020-05-21T18:00:00.000000'}),
    ]

    fake_server = create_fake_server(messages=messages)

    async with websockets.serve(fake_server, '127.0.0.1', 5001):
        with patch(websockets.connect, fake_websocket_failure):
            await availability_provider.handle_websocket_messages()

            with freeze_time('2020-05-21T18:05:00.000000'):
                assert availability_provider.get_all_availability() == {
                    '0001': 1,
                    '0002': 1,
                    '0003': 0
                }


@asynccontextmanager
async def fake_websocket_failure(url):
    def _raise_on_send(message):
        raise websockets.exceptions.WebSocketException('bad stuff')

    unstub(websockets)
    yield mock({'send': _raise_on_send})