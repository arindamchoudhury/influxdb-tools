#!/usr/bin/env python3

import coloredlogs
import logging
import argparse
import os
import pickle
from lib.influx import InfluxClient
from lib.influx_helper import convert_to_lp
from datetime import datetime, timedelta
from time import sleep

WHOAMI = 'influxdb-backup-database'

LOGGER = logging.getLogger(WHOAMI)

coloredlogs.install(level="INFO", fmt='%(asctime)s,%(msecs)03d %(name)s[%(process)d] line:%(lineno)d %(levelname)s %(message)s')
logging.getLogger('urllib3').setLevel(logging.WARNING)

def main(db):
    influxdb_cli = InfluxClient(db)
    measurements_list = influxdb_cli.get_measurement_list()
    progress_file = "progress_backup_{}.json".format(db)
    progress = []
    if not os.path.exists(progress_file):
        os.mknod(progress_file)
    else:
        with open(progress_file, 'rb') as filehandle:
            progress = pickle.load(filehandle)
    LOGGER.info("progress %s", progress)
    done = 1
    for measurement in measurements_list:
        measurement_data = []
        if measurement in progress:
            LOGGER.info("mesurement %s already processed", measurement)
            done = done + 1
            continue

        LOGGER.info("processing mesurement %s (%s/%s)", measurement, done, len(measurements_list))
        end_time = influxdb_cli.get_end_time(measurement)

        measurement_backup_file = "influxdb_{}_{}_{}.backup".format(db, measurement, end_time)
        if not os.path.exists(measurement_backup_file):
            os.mknod(measurement_backup_file)
        else:
            os.remove(measurement_backup_file)

        end_time = datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%SZ')

        while(True):
            start_time = end_time - timedelta(hours=6)
            start_time_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
            end_time_str = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')
            data_exists = influxdb_cli.data_exists_duration(measurement, start_time_str, end_time_str)

            if not data_exists:
                done = done + 1
                progress.append(measurement)
                with open(progress_file, 'wb') as filehandle:
                    pickle.dump(progress, filehandle)

                lines = []
                with open(measurement_backup_file) as file:
                    lines = [line.strip() for line in file]

                LOGGER.info("%s has %s lines", measurement_backup_file, len(lines))

                break

            # LOGGER.info("data_exists %s", data_exists)
            LOGGER.info("backing up data %s %s %s -> %s", db, measurement, start_time_str, end_time_str)
            res = influxdb_cli.get_data_from_measurement_duration(measurement, start_time_str, end_time_str)
            for _, series in res.keys():
                series_data = list(res.get_points(tags=series))
                for data in series_data:
                    measurement_data.append(convert_to_lp(measurement, series, data))

            try:
                with open(measurement_backup_file, mode='a', encoding='utf-8') as filehandle:
                    LOGGER.info("writing %s lines to %s", len(measurement_data), measurement_backup_file)
                    filehandle.write('\n'.join(measurement_data))
                    filehandle.write('\n')
                measurement_data = []
            except Exception:
                LOGGER.exception("failed to write to %s", measurement_backup_file)

            end_time = start_time
            sleep(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='script for backing up influxdb database')
    parser.add_argument('--db', required=True, help='InfluxDB database to backup')
    args = parser.parse_args()

    main(args.db)




