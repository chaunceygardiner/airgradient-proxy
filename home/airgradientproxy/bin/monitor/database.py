#!/usr/bin/python3

# Copyright (c) 2025-2026 John A Kline
# See the file LICENSE for your full rights.

"""sqlite storage for readings.  Rows are keyed by record type; CURRENT and
TWO_MINUTE are single-row record types (delete+insert), ARCHIVE accumulates.
"""

import os
import sqlite3

from datetime import datetime
from dateutil import tz
from json import dumps
from typing import Any, Iterator, List, Optional, Tuple

from monitor import log
from monitor.model import Reading, RecordType, convert_to_json

class DatabaseAlreadyExists(Exception):
    pass

class Database(object):
    def __init__(self, db_file: str):
        self.db_file = db_file

    @staticmethod
    def create(db_file: str) -> 'Database':
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
        insert_reading_sql: str = ('INSERT INTO Reading ('
            ' record_type, timestamp, serialno, wifi, pm01, pm02, pm10, pm02Compensated,'
            ' pm01Standard, pm02Standard, pm10Standard, rco2, pm003Count, pm005Count,'
            ' pm01Count, pm02Count, pm50Count, pm10Count, atmp, atmpCompensated, rhum,'
            ' rhumCompensated, tvocIndex, tvocRaw, noxIndex, noxRaw, boot, bootCount,'
            ' ledMode, firmware, model)'
            ' VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);')
        insert_reading_values: Tuple[Any, ...] = (
            record_type, stamp, r.serialno, r.wifi, r.pm01, r.pm02, r.pm10, r.pm02Compensated,
            r.pm01Standard, r.pm02Standard, r.pm10Standard, r.rco2, r.pm003Count, r.pm005Count,
            r.pm01Count, r.pm02Count, r.pm50Count, r.pm10Count, r.atmp, r.atmpCompensated,
            r.rhum, r.rhumCompensated, r.tvocIndex, r.tvocRaw, r.noxIndex, r.noxRaw, r.boot,
            r.bootCount, r.ledMode, r.firmware, r.model)

        with sqlite3.connect(self.db_file, timeout=15) as conn:
            cursor = conn.cursor()
            # if a current record or two minute record, delete previous current.
            if record_type == RecordType.CURRENT or record_type == RecordType.TWO_MINUTE:
                cursor.execute('DELETE FROM Reading where record_type = ?;', (record_type,))
            # Now insert.
            cursor.execute(insert_reading_sql, insert_reading_values)

    def fetch_current_readings(self) -> Iterator[Reading]:
        return self.fetch_readings(RecordType.CURRENT, 0)

    def fetch_current_reading_as_json(self) -> str:
        for reading in self.fetch_current_readings():
            log.info('fetch-current-record')
            return convert_to_json(reading)
        return '{}'

    def fetch_two_minute_readings(self) -> Iterator[Reading]:
        return self.fetch_readings(RecordType.TWO_MINUTE, 0)

    def fetch_two_minute_reading_as_json(self) -> str:
        for reading in self.fetch_two_minute_readings():
            log.info('fetch-two-minute-record')
            return convert_to_json(reading)
        return '{}'

    def get_earliest_timestamp_as_json(self) -> str:
        select: str = ('SELECT timestamp FROM Reading WHERE record_type = ?'
            ' ORDER BY timestamp LIMIT 1')
        log.debug('get-earliest-timestamp: select: %s' % select)
        resp = {}
        with sqlite3.connect(self.db_file, timeout=5) as conn:
            cursor = conn.cursor()
            for row in cursor.execute(select, (RecordType.ARCHIVE,)):
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
            contents += convert_to_json(reading)
        log.info('fetch-archive-records')
        return '[  %s ]' % contents

    def fetch_readings(self, record_type: int, since_ts: int = 0, max_ts: Optional[int] = None, limit: Optional[int] = None) -> Iterator[Reading]:
        select: str = ('SELECT timestamp, serialno, wifi, pm01, pm02, pm10, pm02Compensated, pm01Standard,'
            ' pm02Standard, pm10Standard, rco2, pm003Count, pm005Count, pm01Count, pm02Count, pm50Count,'
            ' pm10Count, atmp, atmpCompensated, rhum, rhumCompensated, tvocIndex, tvocRaw, noxIndex,'
            ' noxRaw, boot, bootCount, ledMode, firmware, model'
            ' FROM Reading WHERE record_type = ? AND timestamp > ?')
        select_values: List[Any] = [record_type, since_ts]
        if max_ts is not None:
            select += ' AND timestamp <= ?'
            select_values.append(max_ts)
        select += ' ORDER BY timestamp'
        if limit is not None:
            # One row per reading, so a SQL LIMIT counts readings correctly.
            select += ' LIMIT ?'
            select_values.append(limit)
        select += ';'
        log.debug('fetch_readings: select: %s, values: %r' % (select, select_values))
        with sqlite3.connect(self.db_file, timeout=5) as conn:
            cursor = conn.cursor()
            for row in cursor.execute(select, select_values):
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
