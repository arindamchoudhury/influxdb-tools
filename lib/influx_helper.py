#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
from datetime import datetime
from numbers import Integral

from pytz import UTC
from dateutil.parser import parse
from six import binary_type, text_type, integer_types, PY2

EPOCH = UTC.localize(datetime.utcfromtimestamp(0))

LOGGER = logging.getLogger(__name__)

def to_nanos(timestamp):
    delta = timestamp - EPOCH
    nanos_in_days = delta.days * 86400 * 10 ** 9
    nanos_in_seconds = delta.seconds * 10 ** 9
    nanos_in_micros = delta.microseconds * 10 ** 3
    return nanos_in_days + nanos_in_seconds + nanos_in_micros


def convert_timestamp(timestamp, precision=None):
    if isinstance(timestamp, Integral):
        return timestamp  # assume precision is correct if timestamp is int

    if isinstance(get_unicode(timestamp), text_type):
        timestamp = parse(timestamp)

    if isinstance(timestamp, datetime):
        if not timestamp.tzinfo:
            timestamp = UTC.localize(timestamp)

        ns = to_nanos(timestamp)
        if precision is None or precision == 'n':
            return ns

        if precision == 'u':
            return ns / 10**3

        if precision == 'ms':
            return ns / 10**6

        if precision == 's':
            return ns / 10**9

        if precision == 'm':
            return ns / 10**9 / 60

        if precision == 'h':
            return ns / 10**9 / 3600

    raise ValueError(timestamp)


def escape_tag(tag):
    tag = get_unicode(tag, force=True)
    return tag.replace(
        "\\", "\\\\"
    ).replace(
        " ", "\\ "
    ).replace(
        ",", "\\,"
    ).replace(
        "=", "\\="
    ).replace(
        "\n", "\\n"
    )


def escape_tag_value(value):
    ret = escape_tag(value)
    if ret.endswith('\\'):
        ret += ' '
    return ret


def quote_ident(value):
    """Indent the quotes."""
    return "\"{}\"".format(value
                           .replace("\\", "\\\\")
                           .replace("\"", "\\\"")
                           .replace("\n", "\\n"))


def quote_literal(value):
    """Quote provided literal."""
    return "'{}'".format(value
                         .replace("\\", "\\\\")
                         .replace("'", "\\'"))


def is_float(value):
    try:
        float(value)
    except (TypeError, ValueError):
        return False

    return True


def escape_value(value):
    if value is None:
        return ''

    value = get_unicode(value)
    if isinstance(value, text_type):
        return quote_ident(value)

    if isinstance(value, integer_types) and not isinstance(value, bool):
        return str(value) + 'i'

    if isinstance(value, bool):
        return str(value)

    if is_float(value):
        return repr(float(value))

    return str(value)


def get_unicode(data, force=False):
    """Try to return a text aka unicode object from the given data."""
    if isinstance(data, binary_type):
        return data.decode('utf-8')

    if data is None:
        return ''

    if force:
        return str(data)

    return data

def convert_to_lp(measurement, tags, fields):
    line = escape_tag(get_unicode(measurement))

    tag_list = []
    for tag_key in sorted(tags.keys()):
        key = escape_tag(tag_key)
        value = escape_tag(tags[tag_key])

        if key != '' and value != '':
            tag_list.append(
                "{key}={value}".format(key=key, value=value)
            )

    if tag_list:
        line += ',' + ','.join(tag_list)

    field_list = []
    for field_key in sorted(fields.keys()):
        if field_key == 'time':
            continue
        key = escape_tag(field_key)
        value = escape_value(fields[field_key])

        if key != '' and value != '':
            field_list.append("{key}={value}".format(
                key=key,
                value=value
            ))

    if field_list:
        line += ' ' + ','.join(field_list)

    time = fields['time']

    timestamp = get_unicode(str(int(convert_timestamp(time)
        )))
    line += ' ' + timestamp

    return line

