import json


def _standard_join_messages():
    connection_status = [{
        'event': 'phx_reply',
        'payload': {
            'status': 'ok'
        }
    }]

    cached_records = [
        update_event({
            'id': '9861',
            'occupancy': 'UNOCCUPIED',
            'time_of_ingest': '2020-05-21T17:59:45.000000'
        })
    ]

    start_of_stream = [{
        'event': 'presence_diff'
    }]

    return connection_status + cached_records + start_of_stream


def create_fake_server(messages=[]):
    all_messages = _standard_join_messages() + messages

    async def _fake_server(websocket, _path):
        _join_message = await websocket.recv()

        for message in all_messages:
            await websocket.send(json.dumps(message))

    return _fake_server


def update_event(payload):
    payload['price'] = 1.0
    payload['status'] = 'open'
    payload['limit'] = 'no-limit'

    return {'event':'update','payload':payload}
