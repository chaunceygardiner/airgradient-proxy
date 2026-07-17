#!/usr/bin/python3

# Copyright (c) 2025-2026 John A Kline
# See the file LICENSE for your full rights.

"""Live-sensor self-tests (previously `airgradientproxyd --test`): collect
real readings from the AirGradient sensor named in the conf file, then
exercise the database, averaging, sanity checks and json conversion with
them.

    python3 tests/test-live.py [<airgradientproxy-conf-file>]

The conf file defaults to home/airgradientproxy/airgradientproxy.conf.
Requires a reachable sensor.  Exits non-zero if any test fails.
"""

import copy
import math
import os
import sys
import tempfile

import requests

from datetime import datetime, timedelta
from dateutil import tz
from dateutil.parser import parse
from json import loads
from time import sleep
from typing import List, Tuple

import fixtures

from fixtures import InsaneReading, create_test_reading, print_failed, print_passed
from monitor.database import Database
from monitor.model import Reading, convert_to_json
from monitor.service import Service

def collect_two_readings_one_second_apart(hostname: str, port: int, timeout_secs: int, long_read_secs: int) -> Tuple[Reading, Reading]:
    try:
        session: requests.Session = requests.Session()
        print('collect_two_readings_one_seconds_apart...', end='')
        reading1: Reading = Service.collect_data(session, hostname, port, timeout_secs, long_read_secs)
        sleep(1) # to get a different time (to the second) on reading2
        reading2: Reading = Service.collect_data(session, hostname, port, timeout_secs, long_read_secs)
        print_passed()
        return reading1, reading2
    except Exception as e:
        print_failed(e)
        raise e

def run_tests(service_name: str, hostname: str, port: int, timeout_secs: int, long_read_secs: int) -> None:
    reading, reading2 = collect_two_readings_one_second_apart(hostname, port, timeout_secs, long_read_secs)
    test_db_archive_records(service_name, reading)
    test_db_current_records(service_name, reading, reading2)
    sanity_check_reading(reading)
    test_compute_avg(reading)
    test_convert_to_json(reading, reading2)

def sanity_check_reading(reading: Reading) -> None:
    print('sanity_check_reading....', end='')
    sane, reason = Service.is_sane(reading)
    if sane:
        print_passed()
    else:
        print_failed(InsaneReading(reason))

def test_compute_avg(reading: Reading) -> None:
    try:
        print('test_compute_avg....', end='')
        reading1: Reading = copy.copy(reading)
        reading2: Reading = copy.copy(reading)

        reading1.measurementTime = datetime.now(tz=tz.gettz('UTC')) - timedelta(seconds=15)
        reading2.measurementTime = datetime.now(tz=tz.gettz('UTC'))

        reading1.wifi            = -71.0
        reading2.wifi            = -72.0

        reading1.pm01            = 325.0
        reading2.pm01            = 347.0

        reading1.pm02            = 221.0
        reading2.pm02            = 230.0

        reading1.pm10            = 410.0
        reading2.pm10            = 415.0

        reading1.pm02Compensated = 201.0
        reading2.pm02Compensated = 212.0

        reading1.pm01Standard   = 340.0
        reading2.pm01Standard   = 346.0

        reading1.pm02Standard   = 246.0
        reading2.pm02Standard   = 248.0

        reading1.pm10Standard    = 612.0
        reading2.pm10Standard    = 614.0

        reading1.rco2            = 746.0
        reading2.rco2            = 750.0

        reading1.pm003Count      = 112.0
        reading2.pm003Count      = 185.0

        reading1.pm005Count      = 356.0
        reading2.pm005Count      = 385.0

        reading1.pm01Count       = 643.0
        reading2.pm01Count       = 647.0

        reading1.pm02Count       = 331.0
        reading2.pm02Count       = 338.0

        reading1.pm50Count       = 123.0
        reading2.pm50Count       = 456.0

        reading1.pm10Count       = 432.0
        reading2.pm10Count       = 234.0

        reading1.atmp            = 20.5
        reading2.atmp            = 20.9

        reading1.atmpCompensated = 19.5
        reading2.atmpCompensated = 19.9

        reading1.rhum            = 64.2
        reading2.rhum            = 64.6

        reading1.rhumCompensated = 60.2
        reading2.rhumCompensated = 60.6

        reading1.tvocIndex      = 7.0
        reading2.tvocIndex      = 9.0

        reading1.tvocRaw         = 4.0
        reading2.tvocRaw         = 5.0

        reading1.noxIndex        = 12.0
        reading2.noxIndex        = 14.0

        reading1.noxRaw          = 10.0
        reading2.noxRaw          = 11.0

        readings: List[Reading] = []
        readings.append(reading1)
        readings.append(reading2)

        avg_reading: Reading = Service.compute_avg(readings)

        assert avg_reading.measurementTime == reading2.measurementTime, 'Expected measurementTime: %r, got %r.' % (reading2.measurementTime, avg_reading.measurementTime)

        assert avg_reading.serialno == reading2.serialno
        assert avg_reading.wifi is not None and math.isclose(avg_reading.wifi, -71.5)
        assert avg_reading.pm01 == 336
        assert avg_reading.pm02 is not None and math.isclose(avg_reading.pm02, 225.5)
        assert avg_reading.pm10 is not None and math.isclose(avg_reading.pm10, 412.5)
        assert avg_reading.pm02Compensated is not None and math.isclose(avg_reading.pm02Compensated, 206.5)
        assert avg_reading.pm01Standard == 343
        assert avg_reading.pm02Standard == 247
        assert avg_reading.pm10Standard == 613
        assert avg_reading.rco2 == 748
        assert avg_reading.pm003Count is not None and math.isclose(avg_reading.pm003Count, 148.5)
        assert avg_reading.pm005Count is not None and math.isclose(avg_reading.pm005Count, 370.5)
        assert avg_reading.pm01Count == 645
        assert avg_reading.pm02Count is not None and math.isclose(avg_reading.pm02Count, 334.5)
        assert avg_reading.pm50Count is not None and math.isclose(avg_reading.pm50Count, 289.5)
        assert avg_reading.pm10Count == 333
        assert avg_reading.atmp is not None and math.isclose(avg_reading.atmp, 20.7)
        assert avg_reading.atmpCompensated is not None and math.isclose(avg_reading.atmpCompensated, 19.7)
        assert avg_reading.rhum is not None and math.isclose(avg_reading.rhum, 64.4)
        assert avg_reading.rhumCompensated is not None and math.isclose(avg_reading.rhumCompensated, 60.4)
        assert avg_reading.tvocIndex == 8
        assert avg_reading.tvocRaw is not None and math.isclose(avg_reading.tvocRaw, 4.5)
        assert avg_reading.noxIndex == 13
        assert avg_reading.noxRaw is not None and math.isclose(avg_reading.noxRaw, 10.5)
        assert avg_reading.boot == reading2.boot
        assert avg_reading.bootCount == reading2.bootCount
        assert avg_reading.ledMode == reading2.ledMode
        assert avg_reading.firmware == reading2.firmware
        assert avg_reading.model == reading2.model

        print_passed()
    except Exception as e:
        print_failed(e)

def test_db_archive_records(service_name: str, reading_in: Reading) -> None:
    try:
        print('test_db_archive_records....', end='')
        tmp_db = tempfile.NamedTemporaryFile(
            prefix='tmp-test-db-archive-%s.sdb' % service_name, delete=False)
        tmp_db.close()
        os.unlink(tmp_db.name)
        db = Database.create(tmp_db.name)
        db.save_archive_reading(reading_in)
        cnt = 0
        for reading_out in db.fetch_archive_readings(0):
            if reading_in != reading_out:
                print('test_db_archive_records failed: in: %r, out: %r' % (reading_in, reading_out))
            cnt += 1
        if cnt != 1:
            print('test_db_archive_records failed with count: %d' % cnt)
        print_passed()
    except Exception as e:
        print('test_db_archive_records failed: %s' % e)
        raise e
    finally:
        os.unlink(tmp_db.name)

def test_db_current_records(service_name: str, reading_in_1: Reading, reading_in_2: Reading) -> None:
    try:
        print('test_db_current_records....', end='')
        tmp_db = tempfile.NamedTemporaryFile(
            prefix='tmp-test-db-current-%s.sdb' % service_name, delete=False)
        tmp_db.close()
        os.unlink(tmp_db.name)
        db = Database.create(tmp_db.name)
        db.save_current_reading(reading_in_1)
        db.save_current_reading(reading_in_2)
        cnt = 0
        for reading_out in db.fetch_current_readings():
            if reading_in_2 != reading_out:
                print('test_db_current_records failed: in: %r, out: %r' % (reading_in_2, reading_out))
            cnt += 1
        if cnt != 1:
            print('test_db_current_records failed with count: %d' % cnt)
        print_passed()
    except Exception as e:
        print('test_db_current_records failed: %s' % e)
        raise e
    finally:
        os.unlink(tmp_db.name)

def test_convert_to_json(reading1: Reading, reading2: Reading) -> None:
    try:
        print('test_convert_to_json....', end='')

        convert_to_json(reading1)
        convert_to_json(reading2)

        tzinfos = {'CST': tz.gettz("UTC")}
        reading = create_test_reading(parse('2019/12/15T03:43:05UTC', tzinfos=tzinfos))
        json_reading: str = convert_to_json(reading)

        expected = '{"pm01": 0.67, "pm02": 0.23, "pm10": 0.54, "pm01Standard": 0.61, "pm02Standard": 0.22, "pm10Standard": 10.0, "pm003Count": 94.33, "pm005Count": 75.33, "pm01Count": 22.33, "pm02Count": 2.33, "pm50Count": 0.0, "pm10Count": 0.0, "pm02Compensated": 0.9, "atmp": 21.66, "atmpCompensated": 21.36, "rhum": 60.74, "rhumCompensated": 60.44, "rco2": 547.0, "tvocIndex": 98.0, "tvocRaw": 32358.67, "noxIndex": 1.0, "noxRaw": 18311.08, "boot": 14, "bootCount": 14, "wifi": -70.0, "ledMode": "pm", "serialno": "000000000000", "firmware": "3.3.7", "model": "I-9PSL", "measurementTime": "2019-12-15T03:43:05.000000Z"}'

        assert loads(json_reading) == loads(expected), 'Expected json: %s, found: %s' % (expected, json_reading)
        print_passed()
    except Exception as e:
        print_failed(e)

def main() -> None:
    import configobj
    default_conf = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), '..', 'home', 'airgradientproxy', 'airgradientproxy.conf')
    conf_file = sys.argv[1] if len(sys.argv) > 1 else default_conf
    config_dict = configobj.ConfigObj(conf_file, file_error=True, encoding='utf-8')

    service_name  : str = config_dict.get('service-name', 'airgradient-proxy')
    hostname      : str = config_dict.get('hostname', '')
    port          : int = int(config_dict.get('port', 80))
    timeout_secs  : int = int(config_dict.get('timeout-secs', 25))
    long_read_secs: int = int(config_dict.get('long-read-secs', 10))

    if not hostname:
        print('hostname must be specified in the config file: %s' % conf_file)
        sys.exit(2)

    run_tests(service_name, hostname, port, timeout_secs, long_read_secs)

    if fixtures.failure_count:
        print('%d FAILURES' % fixtures.failure_count)
        sys.exit(1)

if __name__ == '__main__':
    main()
