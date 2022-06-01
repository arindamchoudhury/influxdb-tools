#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from influxdb import InfluxDBClient

from lib.config import (
    INFLUXDB_HOST,
    INFLUXDB_USER,
    INFLUXDB_PASS,
    INFLUXDB_PORT,
)

LOGGER = logging.getLogger(__name__)


class InfluxClient(object):
    def __init__(self, db_name):
        self.db_name = db_name
        self.influxdb = InfluxDBClient(INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USER, INFLUXDB_PASS, database=db_name, timeout=60, retries=3)

    def get_measurement_list(self):
        measurements = []
        query = "SHOW MEASUREMENTS"
        points = self.influxdb.query(query).get_points()

        for point in points:
            measurements.append(point['name'])

        return measurements

    def get_end_time(self, measurement):
        try:
            query = 'SELECT * from "{}" order by time desc limit 1'.format(measurement)
            points = list(self.influxdb.query(query).get_points())
            return points[0]["time"]
        except Exception:
            LOGGER.exception(query)

    def data_exists_duration(self, measurement, start_time, end_time):
        try:
            query = 'SELECT COUNT(*) FROM "{}" WHERE time > \'{}\' AND time < \'{}\''.format(measurement, start_time, end_time)
            points = list(self.influxdb.query(query).get_points())
            return points
        except Exception:
            LOGGER.exception(query)

    def get_data_from_measurement_duration(self, measurement, start_time, end_time):
        try:
            query = 'SELECT * FROM "{}" WHERE time > \'{}\' AND time < \'{}\' GROUP BY *'.format(measurement, start_time, end_time)
            return self.influxdb.query(query)
        except Exception:
            LOGGER.exception(query)

    def copy_data_from_measurement(self, to_db, measurement, start_time, end_time):
        try:
            query = 'SELECT * INTO "{}"."ecmanaged"."{}" FROM "{}"."ecmanaged"."{}" WHERE time > \'{}\' AND time < \'{}\' GROUP BY *'.format(to_db, measurement, self.db_name, measurement, start_time, end_time)
            return self.influxdb.query(query)
        except Exception:
            LOGGER.exception(query)





