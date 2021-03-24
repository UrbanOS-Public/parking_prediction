"""
This module handles the edge cases where parking can be paid for before parking
opens up to a half hour in advance.
"""
import datetime as dt


def adjust(timestamp: dt.datetime) -> dt.datetime:
    today8am = timestamp.replace(hour=8, minute=0, second=0, microsecond=0)
    today730am = today8am.replace(hour=7, minute=30)

    today10pm = timestamp.replace(hour=22, minute=0, second=0, microsecond=0)
    today1030pm = today10pm.replace(minute=30)

    if today730am <= timestamp < today8am:
        return today8am
    elif today10pm < timestamp <= today1030pm:
        return today10pm
    else:
        return timestamp
