import sys
import logging
import traceback


def log_exception(details):
    log_to_both(("Backing off {wait:0.1f} seconds afters {tries} tries calling "
           "function {target} with args {args} and kwargs {kwargs}").format(**details))

    formatted_lines = traceback.format_exc().splitlines()
    log_to_both(f"Backing off due to exception {formatted_lines}")


def log_to_both(message):
    print(message)
    logging.warn(message)