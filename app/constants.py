import datetime as dt
from enum import Enum
from typing import List

import pendulum
import pytz

DISCOVERY_API_QUERY_URL = 'https://data.smartcolumbusos.com/api/v1/query'

MODEL_FILE_NAME = 'mlp_shortnorth_downtown_cluster.pkl'

DAY_OF_WEEK = Enum('DayOfWeek', ['MONDAY', 'TUESDAY', 'WEDNESDAY', 'THURSDAY',
                                 'FRIDAY', 'SATURDAY', 'SUNDAY'], start=0)

PARK_MOBILE_SUPPLIER_ID: str = '970010'

TIME_ZONE: dt.tzinfo = pytz.timezone('America/New_York')
HOURS_START: pendulum.Time = pendulum.time(8, 0)
HOURS_END: pendulum.Time = pendulum.time(22, 0)
HOURS_GRACE_PERIOD: pendulum.Duration = pendulum.duration(minutes=30)
UNENFORCED_DAYS: List[DAY_OF_WEEK] = [DAY_OF_WEEK.SUNDAY]