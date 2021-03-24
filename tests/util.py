from dateutil import parser, tz


def as_ts(iso_string):
  return parser.isoparse(iso_string).replace(tzinfo=tz.tzutc())