#!/usr/bin/env python3

import coloredlogs
import logging
import argparse
import os
from lib.influx import InfluxClient
from datetime import datetime, timedelta
from time import sleep
import pickle

WHOAMI = 'influxdb-copy-database'

LOGGER = logging.getLogger(WHOAMI)

coloredlogs.install(level="INFO", fmt='%(asctime)s,%(msecs)03d %(name)s[%(process)d] line:%(lineno)d %(levelname)s %(message)s')
logging.getLogger('urllib3').setLevel(logging.WARNING)

def move_data_hours(from_db, to_db, hours):
    influxdb_cli = InfluxClient(from_db)
    measurements_list = influxdb_cli.get_measurement_list()
    progress_file = "progress_{}_{}.json".format(from_db, to_db)
    progress = []
    if not os.path.exists(progress_file):
        os.mknod(progress_file)
    else:
        with open(progress_file, 'rb') as filehandle:
            progress = pickle.load(filehandle)
    LOGGER.info("progress %s", progress)
    done = 1

    for measurement in measurements_list:
        done_hours = 0

        if measurement in progress:
            LOGGER.info("mesurement %s already processed", measurement)
            done = done + 1
            continue

        LOGGER.info("processing mesurement %s (%s/%s)", measurement, done, len(measurements_list))
        end_time = influxdb_cli.get_end_time(measurement)
        end_time = datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%SZ')

        while(True):
            start_time = end_time - timedelta(hours=6)
            done_hours = done_hours + 6
            start_time_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
            end_time_str = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')
            data_exists = influxdb_cli.data_exists_duration(measurement, start_time_str, end_time_str)

            if not data_exists or done_hours > hours:
                done = done + 1
                progress.append(measurement)
                with open(progress_file, 'wb') as filehandle:
                    pickle.dump(progress, filehandle)
                break

            LOGGER.info("moving data %s -> %s :: %s %s -> %s", from_db, to_db, measurement, start_time_str, end_time_str)
            influxdb_cli.copy_data_from_measurement(to_db, measurement, start_time_str, end_time_str)
            end_time = start_time
            sleep(1)

    return True


def move_data(from_db, to_db):
    influxdb_cli = InfluxClient(from_db)
    measurements_list = influxdb_cli.get_measurement_list()
    progress_file = "progress_{}_{}.json".format(from_db, to_db)
    progress = []
    if not os.path.exists(progress_file):
        os.mknod(progress_file)
    else:
        with open(progress_file, 'rb') as filehandle:
            progress = pickle.load(filehandle)
    LOGGER.info("progress %s", progress)
    done = 1
    for measurement in measurements_list:
        if measurement in progress:
            LOGGER.info("mesurement %s already processed", measurement)
            done = done + 1
            continue

        LOGGER.info("processing mesurement %s (%s/%s)", measurement, done, len(measurements_list))
        end_time = influxdb_cli.get_end_time(measurement)
        end_time = datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%SZ')


        while(True):
            start_time = end_time - timedelta(hours=6)
            start_time_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
            end_time_str = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')
            data_exists = influxdb_cli.data_exists_duration(measurement, start_time_str, end_time_str)

            if not data_exists:
                LOGGER.info("no data exists for %s between %s %s", measurement, start_time, end_time)
                done = done + 1
                progress.append(measurement)
                with open(progress_file, 'wb') as filehandle:
                    pickle.dump(progress, filehandle)
                break

            LOGGER.info("moving data %s -> %s :: %s %s -> %s", from_db, to_db, measurement, start_time_str, end_time_str)
            influxdb_cli.copy_data_from_measurement(to_db, measurement, start_time_str, end_time_str)
            end_time = start_time
            sleep(1)

    return True


def main(from_db, to_db, hours=None):
    if hours:
        move_data_hours(from_db, to_db, hours)
        return True
    else:
        move_data(from_db, to_db)
        return True


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='InfluxDB script for copying data to another database')
    parser.add_argument('--from_db', required=True, help='InfluxDB source database')
    parser.add_argument('--to_db', required=True, help='InfluxDB destination database')
    parser.add_argument('--hours', help='hours to copy')
    args = parser.parse_args()

    main(args.from_db, args.to_db, int(args.hours))



