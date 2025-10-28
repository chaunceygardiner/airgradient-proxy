#!/usr/bin/python3

# Copyright (c) 2025 John A Kline
# See the file LICENSE for your full rights.

"""Make a rolling average of AirGradient readings available.
Read from airgradient sensor every --poll-freq-secs seconds.
Offset readins by --poll-freq-offset.
Write an average readings every --archive-interval-secs to a file.
"""

import calendar
import copy
import math
import optparse
import os
import requests
import sqlite3
import sys
import tempfile
import time
import traceback

import server.server

import configobj

from monitor import Logger
from datetime import datetime
from datetime import timedelta
from dateutil import tz
from dateutil.parser import parse
from enum import Enum
from json import dumps
from json import loads
from time import sleep

from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional, Tuple

AIRGRADIENT_PROXY_VERSION = "1.0"

# Log to stdout until logger info is known.
log: Logger = Logger('monitor', log_to_stdout=True, debug_mode=False)

class Event(Enum):
    POLL = 1
    ARCHIVE = 2

# Sample AirGradient One reading
#{"pm01":0.67,
# "pm02":0.67,
# "pm10":0.67,
# "pm01Standard":0.67,
# "pm02Standard":0.67,
# "pm10Standard":0.67,
# "pm003Count":568.33,
# "pm005Count":383.33,
# "pm01Count":11,
# "pm02Count":0,
# "pm50Count":0,
# "pm10Count":0,
# "pm02Compensated":1.03,
# "atmp":21.91,
# "atmpCompensated":21.91,
# "rhum":58.86,
# "rhumCompensated":58.86,
# "rco2":514,
# "tvocIndex":75,
# "tvocRaw":32100.5,
# "noxIndex":1,
# "noxRaw":18138.67,
# "boot":0,
# "bootCount":0,
# "wifi":-72,
# "ledMode":"pm",
# "serialno":"000000000000",
# "firmware":"3.3.7",
# "model":"I-9PSL"}

# Sample AirGradient Open Air reading
#{"pm01":0,
# "pm02":0,
# "pm10":0,
# "pm01Standard":0,
# "pm02Standard":0,
# "pm10Standard":0,
# "pm003Count":57.17,
# "pm005Count":48.17,
# "pm01Count":7.83,
# "pm02Count":0.67,
# "atmp":23.2,
# "atmpCompensated":22.29,
# "rhum":46.05,
# "rhumCompensated":65.32,
# "pm02Compensated":0,
# "rco2":599.33,
# "tvocIndex":101,
# "tvocRaw":32378.83,
# "noxIndex":1,
# "noxRaw":17800.5,
# "boot":3,
# "bootCount":3,
# "wifi":-73,
# "serialno":"000000000000",
# "firmware":"3.3.9",
# "model":"O-1PST"}

# https://github.com/airgradienthq/arduino/blob/master/docs/local-server.md
@dataclass
class Reading:
    measurementTime : datetime        # time of reading
    serialno        : str             # Serial Number of the monitor
    wifi            : Optional[float] # WiFi signal strength
    pm01            : Optional[float] # PM1.0 in ug/m3 (atmospheric environment)
    pm02            : Optional[float] # PM2.5 in ug/m3 (atmospheric environment)
    pm10            : Optional[float] # PM10 in ug/m3 (atmospheric environment)
    pm02Compensated : Optional[float] # PM2.5 in ug/m3 with correction applied (from fw version 3.1.4 onwards)
    pm01Standard    : Optional[float] # PM1.0 in ug/m3 (standard particle)
    pm02Standard    : Optional[float] # PM2.5 in ug/m3 (standard particle)
    pm10Standard    : Optional[float] # PM10 in ug/m3 (standard particle)
    rco2            : Optional[float] # CO2 in ppm
    pm003Count      : Optional[float] # Particle count 0.3um per dL
    pm005Count      : Optional[float] # Particle count 0.5um per dL
    pm01Count       : Optional[float] # Particle count 1.0um per dL
    pm02Count       : Optional[float] # Particle count 2.5um per dL
    pm50Count       : Optional[float] # Particle count 5.0um per dL (only for indoor monitor)
    pm10Count       : Optional[float] # Particle count 10um per dL (only for indoor monitor)
    atmp            : Optional[float] # Temperature in Degrees Celsius
    atmpCompensated : Optional[float] # Temperature in Degrees Celsius with correction applied
    rhum            : Optional[float] # Relative Humidity
    rhumCompensated : Optional[float] # Relative Humidity with correction applied
    tvocIndex       : Optional[float] # Senisiron VOC Index
    tvocRaw         : Optional[float] # VOC raw value
    noxIndex        : Optional[float] # Senisirion NOx Index
    noxRaw          : Optional[float] # NOx raw value
    boot            : Optional[int  ] # Counts every measurement cycle. Low boot counts indicate restarts.
    bootCount       : Optional[int  ] # Same as boot property. Required for Home Assistant compatability. (deprecated soon!)
    ledMode         : Optional[str  ] # Current configuration of the LED mode
    firmware        : Optional[str  ] # Current firmware version
    model           : Optional[str  ] # Current model name

class DatabaseAlreadyExists(Exception):
    pass

class InsaneReading(Exception):
    pass

class RecordType:
    CURRENT   : int = 0
    ARCHIVE   : int = 1
    TWO_MINUTE: int = 2

class Database(object):
    def __init__(self, db_file: str):
        self.db_file = db_file

    @staticmethod
    def create(db_file): # -> Database:
        if db_file != ':memory:' and os.path.exists(db_file):
            raise DatabaseAlreadyExists("Database %s already exists" % db_file)
        if db_file != ':memory:':
            # Create parent directories
            dir = os.path.dirname(db_file)
            if not os.path.exists(dir):
                os.makedirs(dir)

        create_reading_table: str = ('CREATE TABLE Reading ('
            ' record_type            INTEGER NOT NULL,'
            ' timestamp              REAL NOT NULL,'
            ' serialno               TEXT,'
            ' wifi                   REAL,'
            ' pm01                   REAL,'
            ' pm02                   REAL,'
            ' pm10                   REAL,'
            ' pm02Compensated        REAL,'
            ' pm01Standard           REAL,'
            ' pm02Standard           REAL,'
            ' pm10Standard           REAL,'
            ' rco2                   REAL,'
            ' pm003Count              REAL,'
            ' pm005Count              REAL,'
            ' pm01Count              REAL,'
            ' pm02Count              REAL,'
            ' pm50Count              REAL,'
            ' pm10Count              REAL,'
            ' atmp                   REAL,'
            ' atmpCompensated        REAL,'
            ' rhum                   REAL,'
            ' rhumCompensated        REAL,'
            ' tvocIndex              REAL,'
            ' tvocRaw                REAL,'
            ' noxIndex               REAL,'
            ' noxRaw                 REAL,'
            ' boot                   INTEGER,'
            ' bootCount              INTEGER,'
            ' ledMode                TEXT,'
            ' firmware               TEXT,'
            ' model                  TEXT,'
            ' PRIMARY KEY (record_type, timestamp));')

        with sqlite3.connect(db_file, timeout=5) as conn:
            cursor = conn.cursor()
            cursor.execute(create_reading_table)
            cursor.close()

        return Database(db_file)

    def save_current_reading(self, r: Reading) -> None:
        self.save_reading(RecordType.CURRENT, r)

    def save_two_minute_reading(self, r: Reading) -> None:
        self.save_reading(RecordType.TWO_MINUTE, r)

    def save_archive_reading(self, r: Reading) -> None:
        self.save_reading(RecordType.ARCHIVE, r)

    def save_reading(self, record_type: int, r: Reading) -> None:
        stamp = r.measurementTime.timestamp()
        insert_reading_sql = ('INSERT INTO Reading ('
            ' record_type, timestamp, serialno, wifi, pm01, pm02, pm10, pm02Compensated, pm01Standard,'
            ' pm02Standard, pm10Standard, rco2, pm003Count, pm005Count, pm01Count, pm02Count, pm50Count,'
            ' pm10Count, atmp, atmpCompensated, rhum, rhumCompensated, tvocIndex, tvocRaw, noxIndex,'
            ' noxRaw, boot, bootCount, ledMode, firmware, model)'
            ' VALUES(%d, %f, %r, %r, %r, %r, %r, %r, %r, %r, %r, %r, %r, %r, %r, %r, %r, %r, %r, %r, %r, %r,'
                ' %r, %r, %r, %r, %r, %r, %r, %r, %r);' % (
                record_type, stamp,
                r.serialno if r.serialno is not None else 'NULL',
                r.wifi if r.wifi is not None else 'NULL',
                r.pm01 if r.pm01 is not None else 'NULL',
                r.pm02 if r.pm02 is not None else 'NULL',
                r.pm10 if r.pm10 is not None else 'NULL',
                r.pm02Compensated if r.pm02Compensated is not None else 'NULL',
                r.pm01Standard if r.pm01Standard is not None else 'NULL',
                r.pm02Standard if r.pm02Standard is not None else 'NULL',
                r.pm10Standard if r.pm10Standard is not None else 'NULL',
                r.rco2 if r.rco2 is not None else 'NULL',
                r.pm003Count if r.pm003Count is not None else 'NULL',
                r.pm005Count if r.pm005Count is not None else 'NULL',
                r.pm01Count if r.pm01Count is not None else 'NULL',
                r.pm02Count if r.pm02Count is not None else 'NULL',
                r.pm50Count if r.pm50Count is not None else 'NULL',
                r.pm10Count if r.pm10Count is not None else 'NULL',
                r.atmp if r.atmp is not None else 'NULL',
                r.atmpCompensated if r.atmpCompensated is not None else 'NULL',
                r.rhum if r.rhum is not None else 'NULL',
                r.rhumCompensated if r.rhumCompensated is not None else 'NULL',
                r.tvocIndex if r.tvocIndex is not None else 'NULL',
                r.tvocRaw if r.tvocRaw is not None else 'NULL',
                r.noxIndex if r.noxIndex is not None else 'NULL',
                r.noxRaw if r.noxRaw is not None else 'NULL',
                r.boot if r.boot is not None else 'NULL',
                r.bootCount if r.bootCount is not None else 'NULL',
                r.ledMode if r.ledMode is not None else 'NULL',
                r.firmware if r.firmware is not None else 'NULL',
                r.model if r.model is not None else 'NULL'))

        with sqlite3.connect(self.db_file, timeout=15) as conn:
            cursor = conn.cursor()
            # if a current record or two minute record, delete previous current.
            if record_type == RecordType.CURRENT:
                cursor.execute('DELETE FROM Reading where record_type = %d;' % RecordType.CURRENT)
            if record_type == RecordType.TWO_MINUTE:
                cursor.execute('DELETE FROM Reading where record_type = %d;' % RecordType.TWO_MINUTE)
            # Now insert.
            cursor.execute(insert_reading_sql)

    def fetch_current_readings(self) -> Iterator[Reading]:
        return self.fetch_readings(RecordType.CURRENT, 0)

    def fetch_current_reading_as_json(self) -> str:
        for reading in self.fetch_current_readings():
            log.info('fetch-current-record')
            return Service.convert_to_json(reading)
        return '{}'

    def fetch_two_minute_readings(self) -> Iterator[Reading]:
        return self.fetch_readings(RecordType.TWO_MINUTE, 0)

    def fetch_two_minute_reading_as_json(self) -> str:
        for reading in self.fetch_two_minute_readings():
            log.info('fetch-two-minute-record')
            return Service.convert_to_json(reading)
        return '{}'

    def get_earliest_timestamp_as_json(self) -> str:
        select: str = ('SELECT timestamp FROM Reading WHERE record_type = %d'
            ' ORDER BY timestamp LIMIT 1') % RecordType.ARCHIVE
        log.debug('get-earliest-timestamp: select: %s' % select)
        resp = {}
        with sqlite3.connect(self.db_file, timeout=5) as conn:
            cursor = conn.cursor()
            for row in cursor.execute(select):
                log.debug('get-earliest-timestamp: returned %s' % row[0])
                resp['timestamp'] = row[0]
                break
        log.info('get-earliest-timestamp: %s' % dumps(resp))
        return dumps(resp)

    def fetch_archive_readings(self, since_ts: int = 0, max_ts: Optional[int] = None, limit: Optional[int] = None) -> Iterator[Reading]:
        return self.fetch_readings(RecordType.ARCHIVE, since_ts, max_ts, limit)

    def fetch_archive_readings_as_json(self, since_ts: int = 0, max_ts: Optional[int] = None, limit: Optional[int] = None) -> str:
        contents = ''
        for reading in self.fetch_archive_readings(since_ts, max_ts, limit):
            if contents != '':
                contents += ','
            contents += Service.convert_to_json(reading)
        log.info('fetch-archive-records')
        return '[  %s ]' % contents

    def fetch_readings(self, record_type: int, since_ts: int = 0, max_ts: Optional[int] = None, limit: Optional[int] = None) -> Iterator[Reading]:
        select: str = ('SELECT timestamp, serialno, wifi, pm01, pm02, pm10, pm02Compensated, pm01Standard,'
            ' pm02Standard, pm10Standard, rco2, pm003Count, pm005Count, pm01Count, pm02Count, pm50Count,'
            ' pm10Count, atmp, atmpCompensated, rhum, rhumCompensated, tvocIndex, tvocRaw, noxIndex,'
            ' noxRaw, boot, bootCount, ledMode, firmware, model'
            ' FROM Reading WHERE record_type = %d AND timestamp > %d') % (record_type, since_ts)
        if max_ts is not None:
            select = '%s AND timestamp <= %d' % (select, max_ts)
        select += ' ORDER BY timestamp, record_type'
        if limit is not None:
            select = '%s LIMIT %d' % (select, limit)
        select += ';'
        log.debug('fetch_readings: select: %s' % select)
        with sqlite3.connect(self.db_file, timeout=5) as conn:
            cursor = conn.cursor()
            for row in cursor.execute(select):
                yield Database.create_reading_from_row(row)

    @staticmethod
    def create_reading_from_row(row) -> Reading:
        return Reading(
            measurementTime        = datetime.fromtimestamp(row[0], tz=tz.gettz('UTC')),
            serialno               = row[1],
            wifi                   = row[2],
            pm01                   = row[3],
            pm02                   = row[4],
            pm10                   = row[5],
            pm02Compensated        = row[6],
            pm01Standard           = row[7],
            pm02Standard           = row[8],
            pm10Standard           = row[9],
            rco2                   = row[10],
            pm003Count             = row[11],
            pm005Count             = row[12],
            pm01Count              = row[13],
            pm02Count              = row[14],
            pm50Count              = row[15],
            pm10Count              = row[16],
            atmp                   = row[17],
            atmpCompensated        = row[18],
            rhum                   = row[19],
            rhumCompensated        = row[20],
            tvocIndex              = row[21],
            tvocRaw                = row[22],
            noxIndex               = row[23],
            noxRaw                 = row[24],
            boot                   = row[25],
            bootCount              = row[26],
            ledMode                = row[27],
            firmware               = row[28],
            model                  = row[29])

class Service(object):
    def __init__(self, hostname: str, port: int, timeout_secs: int,
                 long_read_secs: int, pollfreq_secs: int,
                 pollfreq_offset: int, arcint_secs: int,
                 database: Database) -> None:
        self.hostname = hostname
        self.port = port
        self.timeout_secs    = timeout_secs
        self.long_read_secs  = long_read_secs
        self.pollfreq_secs   = pollfreq_secs
        self.pollfreq_offset = pollfreq_offset
        self.arcint_secs     = arcint_secs
        self.database        = database

        log.debug('Service created')

    @staticmethod
    def collect_data(session: requests.Session, hostname: str, port:int, timeout_secs: int, long_read_secs: int) -> Reading:
        # fetch data
        try:
            start_time = time.time()
            response: requests.Response = session.get(url="http://%s:%s/measures/current" % (hostname, port), timeout=timeout_secs)
            response.raise_for_status()
            elapsed_time = time.time() - start_time
            log.debug('collect_data: elapsed time: %f seconds.' % elapsed_time)
            if elapsed_time > long_read_secs:
                log.info('Event took longer than expected: %f seconds.' % elapsed_time)
        except Exception as e:
            raise e
        return Service.parse_response(response)

    @staticmethod
    def parse_response(response: requests.Response) -> Reading:
        try:
            # convert to json
            j: Dict[str, Any] = response.json()

            return Reading(
                measurementTime = Service.utc_now(),
                serialno        = j['serialno'],
                wifi            = float(j['wifi']) if j['wifi'] is not None else None,
                pm01            = float(j['pm01']) if j['pm01'] is not None else None,
                pm02            = float(j['pm02']) if j['pm02'] is not None else None,
                pm10            = float(j['pm10']) if j['pm10'] is not None else None,
                pm02Compensated = float(j['pm02Compensated']) if j['pm02Compensated'] is not None else None,
                pm01Standard    = float(j['pm01Standard']) if j['pm01Standard'] is not None else None,
                pm02Standard    = float(j['pm02Standard']) if j['pm02Standard'] is not None else None,
                pm10Standard    = float(j['pm10Standard']) if j['pm10Standard'] is not None else None,
                rco2            = float(j['rco2']) if j['rco2'] is not None else None,
                pm003Count      = float(j['pm003Count']) if j['pm003Count'] is not None else None,
                pm005Count      = float(j['pm005Count']) if j['pm005Count'] is not None else None,
                pm01Count       = float(j['pm01Count']) if j['pm01Count'] is not None else None,
                pm02Count       = float(j['pm02Count']) if j['pm02Count'] is not None else None,
                pm50Count       = float(j['pm50Count']) if j['pm50Count'] is not None else None,
                pm10Count       = float(j['pm10Count']) if j['pm10Count'] is not None else None,
                atmp            = float(j['atmp']) if j['atmp'] is not None else None,
                atmpCompensated = float(j['atmpCompensated']) if j['atmpCompensated'] is not None else None,
                rhum            = float(j['rhum']) if j['rhum'] is not None else None,
                rhumCompensated = float(j['rhumCompensated']) if j['rhumCompensated'] is not None else None,
                tvocIndex       = float(j['tvocIndex']) if j['tvocIndex'] is not None else None,
                tvocRaw         = float(j['tvocRaw']) if j['tvocRaw'] is not None else None,
                noxIndex        = float(j['noxIndex']) if j['noxIndex'] is not None else None,
                noxRaw          = float(j['noxRaw']) if j['noxRaw'] is not None else None,
                boot            = j['boot'],
                bootCount       = j['bootCount'],
                ledMode         = j['ledMode'],
                firmware        = j['firmware'],
                model           = j['model'])
        except Exception as e:
            log.info('parse_response: %r raised exception %r' % (response.text, e))
            raise e

    @staticmethod
    def datetime_display(dt: datetime) -> str:
        ts = dt.timestamp()
        return "%s (%d)" % (time.strftime("%Y-%m-%d %H:%M:%S %Z", time.localtime(ts)), ts)

    @staticmethod
    def compute_avg(readings: List[Reading]) -> Reading:
        # We are gauranteed at least one reading.
        summed_reading: Reading = copy.copy(readings[0])

        for reading in readings[1:]:
            # These will be overwritten on each iteration.
            summed_reading.measurementTime = reading.measurementTime
            summed_reading.serialno        = reading.serialno
            summed_reading.ledMode         = reading.ledMode
            summed_reading.firmware        = reading.firmware
            summed_reading.model           = reading.model
            summed_reading.boot            = reading.boot
            summed_reading.bootCount       = reading.bootCount

            # These will be averaged
            summed_reading.wifi            = summed_reading.wifi + reading.wifi if summed_reading.wifi is not None and reading.wifi is not None else None
            summed_reading.pm01            = summed_reading.pm01 + reading.pm01 if summed_reading.pm01 is not None and reading.pm01 is not None else None
            summed_reading.pm02            = summed_reading.pm02 + reading.pm02 if summed_reading.pm02 is not None and reading.pm02 is not None else None
            summed_reading.pm10            = summed_reading.pm10 + reading.pm10 if summed_reading.pm10 is not None and reading.pm10 is not None else None
            summed_reading.pm02Compensated = summed_reading.pm02Compensated + reading.pm02Compensated if summed_reading.pm02Compensated is not None and reading.pm02Compensated is not None else None
            summed_reading.pm01Standard    = summed_reading.pm01Standard + reading.pm01Standard if summed_reading.pm01Standard is not None and reading.pm01Standard is not None else None
            summed_reading.pm02Standard    = summed_reading.pm02Standard + reading.pm02Standard if summed_reading.pm02Standard is not None and reading.pm02Standard is not None else None
            summed_reading.pm10Standard    = summed_reading.pm10Standard + reading.pm10Standard if summed_reading.pm10Standard is not None and reading.pm10Standard is not None else None
            summed_reading.rco2            = summed_reading.rco2 + reading.rco2 if summed_reading.rco2 is not None and reading.rco2 is not None else None
            summed_reading.pm003Count      = summed_reading.pm003Count + reading.pm003Count if summed_reading.pm003Count is not None and reading.pm003Count is not None else None
            summed_reading.pm005Count      = summed_reading.pm005Count + reading.pm005Count if summed_reading.pm005Count is not None and reading.pm005Count is not None else None
            summed_reading.pm01Count       = summed_reading.pm01Count + reading.pm01Count if summed_reading.pm01Count is not None and reading.pm01Count is not None else None
            summed_reading.pm02Count       = summed_reading.pm02Count + reading.pm02Count if summed_reading.pm02Count is not None and reading.pm02Count is not None else None
            summed_reading.pm50Count       = summed_reading.pm50Count + reading.pm50Count if summed_reading.pm50Count is not None and reading.pm50Count is not None else None
            summed_reading.pm10Count       = summed_reading.pm10Count + reading.pm10Count if summed_reading.pm10Count is not None and reading.pm10Count is not None else None
            summed_reading.atmp            = summed_reading.atmp + reading.atmp if summed_reading.atmp is not None and reading.atmp is not None else None
            summed_reading.atmpCompensated = summed_reading.atmpCompensated + reading.atmpCompensated if summed_reading.atmpCompensated is not None and reading.atmpCompensated is not None else None
            summed_reading.rhum            = summed_reading.rhum + reading.rhum if summed_reading.rhum is not None and reading.rhum is not None else None
            summed_reading.rhumCompensated = summed_reading.rhumCompensated + reading.rhumCompensated if summed_reading.rhumCompensated is not None and reading.rhumCompensated is not None else None
            summed_reading.tvocIndex       = summed_reading.tvocIndex + reading.tvocIndex if summed_reading.tvocIndex is not None and reading.tvocIndex is not None else None
            summed_reading.tvocRaw         = summed_reading.tvocRaw + reading.tvocRaw if summed_reading.tvocRaw is not None and reading.tvocRaw is not None else None
            summed_reading.noxIndex        = summed_reading.noxIndex + reading.noxIndex if summed_reading.noxIndex is not None and reading.noxIndex is not None else None
            summed_reading.noxRaw          = summed_reading.noxRaw + reading.noxRaw if summed_reading.noxRaw is not None and reading.noxRaw is not None else None


        count: float = float(len(readings))
        avg_reading = copy.copy(summed_reading)

        avg_reading.wifi            = avg_reading.wifi            / count if avg_reading.wifi is not None else None
        avg_reading.pm01            = avg_reading.pm01            / count if avg_reading.pm01 is not None else None
        avg_reading.pm02            = avg_reading.pm02            / count if avg_reading.pm02 is not None else None
        avg_reading.pm10            = avg_reading.pm10            / count if avg_reading.pm10 is not None else None
        avg_reading.pm02Compensated = avg_reading.pm02Compensated / count if avg_reading.pm02Compensated is not None else None
        avg_reading.pm01Standard    = avg_reading.pm01Standard    / count if avg_reading.pm01Standard is not None else None
        avg_reading.pm02Standard    = avg_reading.pm02Standard    / count if avg_reading.pm02Standard is not None else None
        avg_reading.pm10Standard    = avg_reading.pm10Standard    / count if avg_reading.pm10Standard is not None else None
        avg_reading.rco2            = avg_reading.rco2            / count if avg_reading.rco2 is not None else None
        avg_reading.pm003Count      = avg_reading.pm003Count      / count if avg_reading.pm003Count is not None else None
        avg_reading.pm005Count      = avg_reading.pm005Count      / count if avg_reading.pm005Count is not None else None
        avg_reading.pm01Count       = avg_reading.pm01Count       / count if avg_reading.pm01Count is not None else None
        avg_reading.pm02Count       = avg_reading.pm02Count       / count if avg_reading.pm02Count is not None else None
        avg_reading.pm50Count       = avg_reading.pm50Count       / count if avg_reading.pm50Count is not None else None
        avg_reading.pm10Count       = avg_reading.pm10Count       / count if avg_reading.pm10Count is not None else None

        avg_reading.atmp            = avg_reading.atmp            / count if avg_reading.atmp is not None else None
        avg_reading.atmpCompensated = avg_reading.atmpCompensated / count if avg_reading.atmpCompensated is not None else None
        avg_reading.rhum            = avg_reading.rhum            / count if avg_reading.rhum is not None else None
        avg_reading.rhumCompensated = avg_reading.rhumCompensated / count if avg_reading.rhumCompensated is not None else None

        avg_reading.tvocIndex       = avg_reading.tvocIndex       / count if avg_reading.tvocIndex is not None else None
        avg_reading.tvocRaw         = avg_reading.tvocRaw         / count if avg_reading.tvocRaw is not None else None
        avg_reading.noxIndex        = avg_reading.noxIndex        / count if avg_reading.noxIndex is not None else None
        avg_reading.noxRaw          = avg_reading.noxRaw          / count if avg_reading.noxRaw is not None else None

        return avg_reading

    @staticmethod
    def convert_to_json(reading: Reading) -> str:
        reading_dict: Dict[str, Any] = {
            'pm01'            : reading.pm01,
            'pm02'            : reading.pm02,
            'pm10'            : reading.pm10,
            'pm01Standard'    : reading.pm01Standard,
            'pm02Standard'    : reading.pm02Standard,
            'pm10Standard'    : reading.pm10Standard,
            'pm003Count'      : reading.pm003Count,
            'pm005Count'      : reading.pm005Count,
            'pm01Count'       : reading.pm01Count,
            'pm02Count'       : reading.pm02Count,
            'pm50Count'       : reading.pm50Count,
            'pm10Count'       : reading.pm10Count,
            'pm02Compensated' : reading.pm02Compensated,
            'atmp'            : reading.atmp,
            'atmpCompensated' : reading.atmpCompensated,
            'rhum'            : reading.rhum,
            'rhumCompensated' : reading.rhumCompensated,
            'rco2'            : reading.rco2,
            'tvocIndex'       : reading.tvocIndex,
            'tvocRaw'         : reading.tvocRaw,
            'noxIndex'        : reading.noxIndex,
            'noxRaw'          : reading.noxRaw,
            'boot'            : reading.boot,
            'bootCount'       : reading.bootCount,
            'wifi'            : reading.wifi,
            'ledMode'         : reading.ledMode,
            'serialno'        : reading.serialno,
            'firmware'        : reading.firmware,
            'model'           : reading.model,
             # "2025-10-25T17:45:00.000Z"
            #'measurementTime' : reading.measurementTime.strftime('%Y-%m-%dT%H:%M:%S.000Z')}
            'measurementTime' : reading.measurementTime.strftime('%Y-%m-%dT%H:%M:%S.%fZ')}

        return dumps(reading_dict)

    @staticmethod
    def utc_now() -> datetime:
        return datetime.now(tz=tz.gettz('UTC'))

    @staticmethod
    def trim_two_minute_readings(two_minute_readings: List[Reading]) -> None:
        two_minutes_ago: datetime = datetime.now(tz=tz.gettz('UTC')) - timedelta(seconds=120)
        while len(two_minute_readings) > 0 and two_minute_readings[0].measurementTime < two_minutes_ago:
            two_minute_readings.pop(0)

    @staticmethod
    def is_sane(reading: Reading) -> Tuple[bool, str]:
        if not isinstance(reading.measurementTime, datetime):
            return False, 'measurementTime not instance of datetime'
        # Reject reading time that differs from now by more than 20s.
        delta_seconds = Service.utc_now().timestamp() - reading.measurementTime.timestamp()
        if abs(delta_seconds) > 20.0:
            return False, 'measurementTime more than 20s off: %f' % delta_seconds
        if reading.serialno is not None and not isinstance(reading.serialno, str):
            return False, 'serialno not instance of str'
        if reading.wifi is not None and not isinstance(reading.wifi, float):
            return False, 'wifi not instance of float'
        if reading.pm01 is not None and not isinstance(reading.pm01, float):
            return False, 'pm01 not instance of float'
        if reading.pm02 is not None and not isinstance(reading.pm02, float):
            return False, 'pm02 not instance of float'
        if reading.pm10 is not None and not isinstance(reading.pm10, float):
            return False, 'pm10 not instance of float'
        if reading.pm02Compensated is not None and not isinstance(reading.pm02Compensated, float):
            return False, 'pm02Compensated not instance of float'
        if reading.pm01Standard is not None and not isinstance(reading.pm01Standard, float):
            return False, 'pm01Standard not instance of float'
        if reading.pm02Standard is not None and not isinstance(reading.pm02Standard, float):
            return False, 'pm02Standard not instance of float'
        if reading.pm10Standard is not None and not isinstance(reading.pm10Standard, float):
            return False, 'pm10Standard not instance of float'
        if reading.rco2 is not None and not isinstance(reading.rco2, float):
            return False, 'rco2 not instance of float'
        if reading.pm003Count is not None and not isinstance(reading.pm003Count, float):
            return False, 'pm003Count not instance of float'
        if reading.pm005Count is not None and not isinstance(reading.pm005Count, float):
            return False, 'pm005Count not instance of float'
        if reading.pm01Count is not None and not isinstance(reading.pm01Count, float):
            return False, 'pm01Count not instance of float'
        if reading.pm02Count is not None and not isinstance(reading.pm02Count, float):
            return False, 'pm02Count not instance of float'
        if reading.pm50Count is not None and not isinstance(reading.pm50Count, float):
            return False, 'pm50Count not instance of float'
        if reading.pm10Count is not None and not isinstance(reading.pm10Count, float):
            return False, 'pm10Count not instance of float'
        if reading.atmp is not None and not isinstance(reading.atmp, float):
            return False, 'atmp not instance of float'
        if reading.atmpCompensated is not None and not isinstance(reading.atmpCompensated, float):
            return False, 'atmpCompensated not instance of float'
        if reading.rhum is not None and not isinstance(reading.rhum, float):
            return False, 'rhum not instance of float'
        if reading.rhumCompensated is not None and not isinstance(reading.rhumCompensated, float):
            return False, 'rhumCompensated not instance of float'
        if reading.tvocIndex is not None and not isinstance(reading.tvocIndex, float):
            return False, 'tvocIndex not instance of float'
        if reading.tvocRaw is not None and not isinstance(reading.tvocRaw, float):
            return False, 'tvocRaw not instance of float'
        if reading.noxIndex is not None and not isinstance(reading.noxIndex, float):
            return False, 'noxIndex not instance of float'
        if reading.noxRaw is not None and not isinstance(reading.noxRaw, float):
            return False, 'noxRaw not instance of float'
        if reading.boot is not None and not isinstance(reading.boot, int):
            return False, 'boot not instance of int'
        if reading.bootCount is not None and not isinstance(reading.bootCount, int):
            return False, 'bootCount not instance of int'
        if reading.ledMode is not None and not isinstance(reading.ledMode, str):
            return False, 'ledMode not instance of str'
        if reading.firmware is not None and not isinstance(reading.firmware, str):
            return False, 'firmware not instance of str'
        if reading.model is not None and not isinstance(reading.model, str):
            return False, 'model not instance of str'

        return True, ''

    def compute_next_event(self, first_time: bool) -> Tuple[Event, float]:
        now = time.time()
        next_poll_event = int(now / self.pollfreq_secs) * self.pollfreq_secs + self.pollfreq_secs
        next_arc_event = int(now / self.arcint_secs) * self.arcint_secs + self.arcint_secs
        event = Event.ARCHIVE if next_poll_event == next_arc_event else Event.POLL
        secs_to_event = next_poll_event - now
        # Add pollfreq_offset to computed next event.
        secs_to_event += self.pollfreq_offset
        log.debug('Next event: %r in %f seconds' % (event, secs_to_event))
        return event, secs_to_event

    def do_loop(self) -> None:
        archive_readings   : List[Reading] = []
        two_minute_readings: List[Reading] = []

        first_time: bool = True
        log.debug('Started main loop.')
        session: Optional[requests.Session] = None

        while True:
            if first_time:
                first_time = False
                event = Event.POLL
            else:
                # sleep until next event
                event, secs_to_event = self.compute_next_event(first_time)
                sleep(secs_to_event)

            # Always trim two_minute_readings
            Service.trim_two_minute_readings(two_minute_readings)

            # Write a reading and possibly write an archive record.
            try:
                # collect another reading and add it to archive_readings, two_minute_readings
                start = Service.utc_now()
                if session is None:
                    session= requests.Session()
                reading: Reading = Service.collect_data(session, self.hostname, self.port, self.timeout_secs, self.long_read_secs)
                log.debug('Read sensor in %d seconds.' % (Service.utc_now() - start).seconds)
                sane, reason = Service.is_sane(reading)
                if sane:
                    archive_readings.append(reading)
                    two_minute_readings.append(reading)
                    # Save this reading as the current reading
                    try:
                        start = Service.utc_now()
                        self.database.save_current_reading(reading)
                        log.info('Saved current reading %s in %d seconds.' %
                            (Service.datetime_display(reading.measurementTime), (Service.utc_now() - start).seconds))
                    except Exception as e:
                        log.critical('Could not save current reading to database: %s: %s' % (self.database, e))
                else:
                    log.error('Reading found insane due to:  %s: %s' % (reason, reading))
            except Exception as e:
                log.error('Skipping reading because of: %r' % e)
                # It's probably a good idea to reset the session
                try:
                    if session is not None:
                        session.close()
                except Exception as e:
                    log.info('Non-fatal: calling session.close(): %s' % e)
                finally:
                    session = None

            # Write two minute avg reading.
            if len(two_minute_readings) == 0:
                log.error('Skipping two_minute record because there have been zero readings this two minute period.')
            else:
                avg_reading: Reading = Service.compute_avg(two_minute_readings)
                avg_reading.measurementTime = two_minute_readings[-1].measurementTime
                try:
                    start = Service.utc_now()
                    self.database.save_two_minute_reading(avg_reading)
                    log.info('Saved two minute reading %s in %d seconds (%d samples).' %
                        (Service.datetime_display(avg_reading.measurementTime), (Service.utc_now() - start).seconds, len(two_minute_readings)))
                except Exception as e:
                    log.critical('Could not save two minute reading to database: %s: %s' % (self.database, e))

            # compute averages from records and write to database
            # if archive time, also write an archive record
            if event == event.ARCHIVE:
                if len(archive_readings) == 0:
                    log.error('Skipping archive record because there have been zero readings this archive period.')
                else: 
                    avg_reading = Service.compute_avg(archive_readings)
                    avg_reading.measurementTime = Service.utc_now()
                    # We care more about the timestamp for archive cycles as we
                    # are writing permanent archive records.  As such, we
                    # want these times to align exactly with the archive cycle.
                    # ARCHIVE cycles might be used for backfilling.
                    # The plus five seconds is to guard against this routine
                    # running a few seconds early.
                    reading_plus_5s_ts = calendar.timegm(
                        (avg_reading.measurementTime + timedelta(seconds=5)).utctimetuple())
                    archive_ts = int(reading_plus_5s_ts / self.arcint_secs) * self.arcint_secs
                    avg_reading.measurementTime = datetime.fromtimestamp(archive_ts, tz=tz.gettz('UTC'))
                    try:
                        start = Service.utc_now()
                        self.database.save_archive_reading(avg_reading)
                        log.debug('Saved archive reading in %d seconds.' % (Service.utc_now() - start).seconds)
                        log.info('Added record %s to archive (%d samples).'
                            % (Service.datetime_display(avg_reading.measurementTime), len(archive_readings)))
                        # Reset archive_readings for new archive cycle.
                        archive_readings.clear()
                    except Exception as e:
                        log.critical('Could not save archive reading to database: %s: %s' % (self.database, e))

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def print_passed() -> None:
    print(bcolors.OKGREEN + 'PASSED' + bcolors.ENDC)

def print_failed(e: Exception) -> None:
    print(bcolors.FAIL + 'FAILED' + bcolors.ENDC)
    print(traceback.format_exc())

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
    test_db_current_records(service_name, reading,reading2)
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

        reading1.wifi            = -71
        reading2.wifi            = -72

        reading1.pm01            = 325
        reading2.pm01            = 347

        reading1.pm02            = 221
        reading2.pm02            = 230

        reading1.pm10            = 410
        reading2.pm10            = 415

        reading1.pm02Compensated = 201
        reading2.pm02Compensated = 212

        reading1.pm01Standard   = 340
        reading2.pm01Standard   = 346

        reading1.pm02Standard   = 246
        reading2.pm02Standard   = 248

        reading1.pm10Standard    = 612
        reading2.pm10Standard    = 614

        reading1.rco2            = 746
        reading2.rco2            = 750

        reading1.pm003Count      = 112
        reading2.pm003Count      = 185

        reading1.pm005Count      = 356
        reading2.pm005Count      = 385

        reading1.pm01Count       = 643
        reading2.pm01Count       = 647

        reading1.pm02Count       = 331
        reading2.pm02Count       = 338

        reading1.pm50Count       = 123
        reading2.pm50Count       = 456

        reading1.pm10Count       = 432
        reading2.pm10Count       = 234

        reading1.atmp            = 20.5
        reading2.atmp            = 20.9

        reading1.atmpCompensated = 19.5
        reading2.atmpCompensated = 19.9

        reading1.rhum            = 64.2
        reading2.rhum            = 64.6

        reading1.rhumCompensated = 60.2
        reading2.rhumCompensated = 60.6

        reading1.tvocIndex      = 7
        reading2.tvocIndex      = 9

        reading1.tvocRaw         = 4
        reading2.tvocRaw         = 5

        reading1.noxIndex        = 12
        reading2.noxIndex        = 14

        reading1.noxRaw          = 10
        reading2.noxRaw          = 11

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

def create_test_reading(measurementTime: datetime) -> Reading:
    return Reading(
        measurementTime = measurementTime,
        serialno        = '000000000000',
        wifi            = -70,
        pm01            = 0.67,
        pm02            = 0.23,
        pm10            = 0.54,
        pm02Compensated = 0,
        pm01Standard    = 0,
        pm02Standard    = 0,
        pm10Standard    = 10,
        rco2            = 547,
        pm003Count      = 94.33,
        pm005Count      = 75.33,
        pm01Count       = 22.33,
        pm02Count       = 2.33,
        pm50Count       = 0,
        pm10Count       = 0,
        atmp            = 21.66,
        atmpCompensated = 21.66,
        rhum            = 60.74,
        rhumCompensated = 60.74,
        tvocIndex       = 98,
        tvocRaw         = 32358.67,
        noxIndex        = 1,
        noxRaw          = 18311.08,
        boot            = 14,
        bootCount       = 14,
        ledMode         = 'pm',
        firmware        = '3.3.7',
        model           = 'I-9PSL')

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
            #print(reading_out)
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
            #print(reading_out)
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

        Service.convert_to_json(reading1)
        Service.convert_to_json(reading2)

        tzinfos = {'CST': tz.gettz("UTC")}
        reading = create_test_reading(parse('2019/12/15T03:43:05UTC', tzinfos=tzinfos))
        json_reading: str = Service.convert_to_json(reading)

        expected = '{"pm01": 0.67, "pm02": 0.23, "pm10": 0.54, "pm01Standard": 0, "pm02Standard": 0, "pm10Standard": 10, "pm003Count": 94.33, "pm005Count": 75.33, "pm01Count": 22.33, "pm02Count": 2.33, "pm50Count": 0, "pm10Count": 0, "pm02Compensated": 0, "atmp": 21.66, "atmpCompensated": 21.66, "rhum": 60.74, "rhumCompensated": 60.74, "rco2": 547, "tvocIndex": 98, "tvocRaw": 32358.67, "noxIndex": 1, "noxRaw": 18311.08, "boot": 14, "bootCount": 14, "wifi": -70, "ledMode": "pm", "serialno": "000000000000", "firmware": "3.3.7", "model": "I-9PSL", "measurementTime": "2019-12-15T03:43:05.000000Z"}'

        assert loads(json_reading) == loads(expected), 'Expected json: %s, found: %s' % (expected, json_reading)
        print_passed()
    except Exception as e:
        print_failed(e)

def dump_database(db_file: str) -> None:
    start = Service.utc_now()
    database: Database = Database(db_file)
    print('----------------------------')
    print('* Dumping current reading  *')
    print('----------------------------')
    for reading in database.fetch_current_readings():
        print(reading)
        print('---')
    print('----------------------------')
    print('* Dumping archive readings *')
    print('----------------------------')
    for reading in database.fetch_archive_readings():
        print(reading)
        print('---')
    print('Dumped database in %d seconds.' % (Service.utc_now() - start).seconds)

class UnexpectedSensorRecord(Exception):
    pass

class CantOpenConfigFile(Exception):
    pass

class CantParseConfigFile(Exception):
    pass

def get_configuration(config_file):
    try:
        config_dict = configobj.ConfigObj(config_file, file_error=True, encoding='utf-8')
    except IOError:
        raise CantOpenConfigFile("Unable to open configuration file %s" % config_file)
    except configobj.ConfigObjError:
        raise CantParseConfigFile("Error parsing configuration file %s", config_file)

    return config_dict

def start(args):
    usage = """%prog [--help] [--test | --dump] [--pidfile <pidfile>] <airgradientproxy-conf-file>"""
    parser: optparse.OptionParser = optparse.OptionParser(usage=usage)

    parser.add_option('-p', '--pidfile', dest='pidfile', action='store',
                      type=str, default=None,
                      help='When running as a daemon, pidfile in which to write pid.  Default is None.')
    parser.add_option('-t', '--test', dest='test', action='store_true', default=False,
                      help='Run tests and then exit. Default is False')
    parser.add_option('-d', '--dump', dest='dump', action='store_true', default=False,
                      help='Dump database and then exit. Default is False')

    (options, args) = parser.parse_args()

    if len(args) != 1:
        parser.error('Usage: [--pidfile <pidfile>] [--test | --dump] <airgradientproxy-conf-file>')

    conf_file: str = os.path.abspath(args[0])
    config_dict    = get_configuration(conf_file)

    debug          : bool           = int(config_dict.get('debug', 0))
    log_to_stdout  : bool           = int(config_dict.get('log-to-stdout', 0))
    service_name   : str            = config_dict.get('service-name', 'airgradient-proxy')
    hostname       : Optional[str]  = config_dict.get('hostname', None)
    port           : int            = int(config_dict.get('port', 80))
    server_port    : int            = int(config_dict.get('server-port', 8000))
    timeout_secs   : int            = int(config_dict.get('timeout-secs', 25))
    long_read_secs : int            = int(config_dict.get('long-read-secs', 10))
    pollfreq_secs  : int            = int(config_dict.get('poll-freq-secs', 30))
    pollfreq_offset: int            = int(config_dict.get('poll-freq-offset', 0))
    arcint_secs    : int            = int(config_dict.get('archive-interval-secs', 300))
    db_file        : Optional[str]  = config_dict.get('database-file', None)

    global log
    log = Logger(service_name, log_to_stdout=log_to_stdout, debug_mode=debug)

    log.info('debug          : %r'    % debug)
    log.info('log_to_stdout  : %r'    % log_to_stdout)
    log.info('conf_file      : %s'    % conf_file)
    log.info('Version        : %s'    % AIRGRADIENT_PROXY_VERSION)
    log.info('host:port      : %s:%s' % (hostname, port))
    log.info('server_port    : %s'    % server_port)
    log.info('timeout_secs   : %d'    % timeout_secs)
    log.info('long_read_secs : %d'    % long_read_secs)
    log.info('pollfreq_secs  : %d'    % pollfreq_secs)
    log.info('pollfreq_offset: %d'    % pollfreq_offset)
    log.info('arcint_secs    : %d'    % arcint_secs)
    log.info('db_file        : %s'    % db_file)
    log.info('service_name   : %s'    % service_name)
    log.info('pidfile        : %s'    % options.pidfile)

    if options.test and options.dump:
        parser.error('At most one of --test and --dump can be specified.')

    if options.test is True:
        if not hostname:
            parser.error('hostname must be specified in the config file')
        assert(hostname)
        run_tests(service_name, hostname, port, timeout_secs, long_read_secs)
        sys.exit(0)

    if options.dump is True:
        if not db_file:
            parser.error('database-file must be specified in the config file')
        assert(db_file)
        dump_database(db_file)
        sys.exit(0)

    if not hostname:
        parser.error('hostname must be specified in the config file')

    if not db_file:
        parser.error('database-file must be specified in the config file')

    # arcint must be a multilpe of pollfreq
    if arcint_secs % pollfreq_secs != 0:
        parser.error('archive-interval-secs must be a multiple of poll-frequency-secs')

    if options.pidfile is not None:
        pid: str = str(os.getpid())
        with open(options.pidfile, 'w') as f:
            f.write(pid+'\n')
            os.fsync(f)

    # Create database if it does not yet exist.
    assert(db_file)
    if not os.path.exists(db_file):
        log.debug('Creating database: %s' % db_file)
        database: Database = Database.create(db_file)
    else:
        assert(db_file)
        database = Database(db_file)

    assert(hostname)
    airgradientproxy_service = Service(hostname, port, timeout_secs, long_read_secs, pollfreq_secs,
                                  pollfreq_offset, arcint_secs, database)

    log.debug('Staring server on port %d.' % server_port)
    assert(db_file)
    server.server.serve_requests(server_port, db_file, log)

    log.debug('Staring mainloop.')
    airgradientproxy_service.do_loop()
