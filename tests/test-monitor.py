#!/usr/bin/python3

# Copyright (c) 2026 John A Kline
# See the file LICENSE for your full rights.

"""Offline tests for the monitor and server code: database round trips,
fetch semantics (since_ts/max_ts/limit), sanity checks, REST request
parsing, the CO2 median, averaging, the schema migration, and the polling
loop itself, run on a fake clock.  Needs no live sensor (the live-sensor path is covered by
tests/test-live.py).

    python3 tests/test-monitor.py

Exits non-zero if any test fails.
"""

import dataclasses
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
import types

from datetime import datetime, timedelta
from dateutil import tz
from json import dumps, loads
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.abspath(__file__)), '..', 'home', 'airgradientproxy', 'bin'))

from fixtures import create_test_reading, create_open_air_test_reading
from monitor import log
from monitor.database import Database
from monitor.model import Reading, convert_to_json
from monitor.service import Service
import monitor.service as service_module
import server.server as srv

failures = []

def check(label: str, cond: bool, detail: str = '') -> None:
    if cond:
        print('PASS: %s' % label)
    else:
        failures.append(label)
        print('FAIL: %s %s' % (label, detail))

now = datetime.now(tz=tz.gettz('UTC')).replace(microsecond=0)

# --- build a database holding 5 archive readings 60s apart ---
tmp = tempfile.NamedTemporaryFile(prefix='airgradient-proxy-test', suffix='.sdb', delete=False)
tmp.close()
os.unlink(tmp.name)
db = Database.create(tmp.name)

readings_in = []
for i in range(5):
    r = create_test_reading(now + timedelta(seconds=60 * i))
    db.save_archive_reading(r)
    readings_in.append(r)

# 1. since_ts=0 returns everything, in order.
out = list(db.fetch_archive_readings(0))
check('fetch all (since_ts=0) returns 5 readings', len(out) == 5, 'got %d' % len(out))
check('readings in timestamp order',
      [r.measurementTime for r in out] == [r.measurementTime for r in readings_in])
check('round trip equality', all(a == b for a, b in zip(readings_in, out)))

# 2. limit counts readings (one row per reading, so a SQL LIMIT is correct).
out = list(db.fetch_archive_readings(0, limit=3))
check('limit=3 returns 3 readings', len(out) == 3, 'got %d' % len(out))
out = list(db.fetch_archive_readings(0, limit=1))
check('limit=1 returns 1 reading', len(out) == 1, 'got %d' % len(out))
check('limit=1 returns the earliest reading',
      len(out) == 1 and out[0].measurementTime == readings_in[0].measurementTime)

# 3. max_ts is honored, alone and alongside limit.
cutoff = int(readings_in[2].measurementTime.timestamp())
out = list(db.fetch_archive_readings(0, max_ts=cutoff))
check('max_ts returns 3 readings (max_ts inclusive)', len(out) == 3, 'got %d' % len(out))
out = list(db.fetch_archive_readings(0, max_ts=cutoff, limit=2))
check('max_ts+limit returns 2 readings', len(out) == 2, 'got %d' % len(out))

# 4. since_ts is exclusive.
first_ts = int(readings_in[0].measurementTime.timestamp())
out = list(db.fetch_archive_readings(first_ts))
check('since_ts exclusive', len(out) == 4, 'got %d' % len(out))

# 5. get_earliest_timestamp.
js = db.get_earliest_timestamp_as_json()
check('earliest timestamp', ('"timestamp": %d' % first_ts) in js, js)

# 6. save path edge case: an Open Air reading (no pm50Count/pm10Count/ledMode)
#    must round trip with its None fields preserved.
open_air = create_open_air_test_reading(now + timedelta(seconds=600))
db.save_archive_reading(open_air)
out = list(db.fetch_archive_readings(int((now + timedelta(seconds=500)).timestamp())))
check('open air reading round trips', len(out) == 1 and out[0] == open_air,
      'got %r' % (out,))
check('open air None fields preserved',
      len(out) == 1 and out[0].pm50Count is None and out[0].pm10Count is None
      and out[0].ledMode is None)

# 7. CURRENT and TWO_MINUTE stay single-row via delete+insert.
db.save_current_reading(readings_in[0])
db.save_current_reading(readings_in[1])
cur = list(db.fetch_current_readings())
check('current reading single row, latest wins',
      len(cur) == 1 and cur[0].measurementTime == readings_in[1].measurementTime)
db.save_two_minute_reading(readings_in[0])
db.save_two_minute_reading(readings_in[2])
two = list(db.fetch_two_minute_readings())
check('two minute reading single row, latest wins',
      len(two) == 1 and two[0].measurementTime == readings_in[2].measurementTime)

# 8. is_sane.
sane_r = create_test_reading(datetime.now(tz=tz.gettz('UTC')))
ok, _ = Service.is_sane(sane_r)
check('sane reading passes', ok)
open_air_r = create_open_air_test_reading(datetime.now(tz=tz.gettz('UTC')))
ok, _ = Service.is_sane(open_air_r)
check('open air reading (None fields) passes', ok)
sane_r.boot = True
ok, reason = Service.is_sane(sane_r)
check('bool boot rejected', not ok and reason == 'boot not instance of int', reason)
sane_r.boot = 14
sane_r.bootCount = False
ok, reason = Service.is_sane(sane_r)
check('bool bootCount rejected', not ok and reason == 'bootCount not instance of int', reason)
sane_r.bootCount = 14
sane_r.atmp = 21
ok, reason = Service.is_sane(sane_r)
check('int atmp rejected', not ok and reason == 'atmp not instance of float', reason)
sane_r.atmp = 21.66
stale = create_test_reading(datetime.now(tz=tz.gettz('UTC')) - timedelta(seconds=60))
ok, reason = Service.is_sane(stale)
check('stale measurementTime rejected',
      not ok and reason.startswith('measurementTime more than 20s off'), reason)

# 9. REST request parsing.
req = srv.Handler.parse_requestline('GET /fetch-archive-records?since_ts=0 HTTP/1.1')
check('since_ts=0 parses as FETCH_ARCHIVE_RECORDS',
      req.request_type == srv.RequestType.FETCH_ARCHIVE_RECORDS and req.since_ts == 0)
req = srv.Handler.parse_requestline('GET /fetch-archive-records?since_ts=5,max_ts=9,limit=2 HTTP/1.1')
check('full args parse', req.since_ts == 5 and req.max_ts == 9 and req.limit == 2)
req = srv.Handler.parse_requestline('GET /fetch-archive-records HTTP/1.1')
check('missing since_ts is an error', req.request_type == srv.RequestType.ERROR)
req = srv.Handler.parse_requestline('GET /fetch-archive-records?since_ts=abc HTTP/1.1')
check('non-integer since_ts is an error', req.request_type == srv.RequestType.ERROR)
req = srv.Handler.parse_requestline('GET /measures/current HTTP/1.1')
check('/measures/current is fetch-current', req.request_type == srv.RequestType.FETCH_CURRENT_RECORD)
req = srv.Handler.parse_requestline('GET /measures/current?junk=1 HTTP/1.1')
check('/measures/current with args is an error', req.request_type == srv.RequestType.ERROR)
req = srv.Handler.parse_requestline('GET /fetch-two-minute-record HTTP/1.1')
check('/fetch-two-minute-record parses', req.request_type == srv.RequestType.FETCH_TWO_MINUTE_RECORD)
req = srv.Handler.parse_requestline('GET /get-version HTTP/1.1')
check('/get-version parses', req.request_type == srv.RequestType.GET_VERSION)
req = srv.Handler.parse_requestline('GET /get-earliest-timestamp HTTP/1.1')
check('/get-earliest-timestamp parses', req.request_type == srv.RequestType.GET_EARLIEST_TIMESTAMP)
req = srv.Handler.parse_requestline('GET / HTTP/1.1')
check('bare / is an error', req.request_type == srv.RequestType.ERROR)
req = srv.Handler.parse_requestline('GET /nonsense HTTP/1.1')
check('unknown command is an error', req.request_type == srv.RequestType.ERROR)

# 10. rco2Unfiltered: through save, fetch and json; omitted when None.
spiked = create_test_reading(now + timedelta(seconds=900))
spiked.rco2 = 504.0
spiked.rco2Unfiltered = 900.0
db.save_current_reading(spiked)
js = loads(db.fetch_current_reading_as_json())
check('rco2Unfiltered round trips through save, fetch and json',
      js.get('rco2') == 504.0 and js.get('rco2Unfiltered') == 900.0, '%r' % js)
spiked.rco2 = None
spiked.rco2Unfiltered = None
js = loads(convert_to_json(spiked))
check('json omits a None rco2 and rco2Unfiltered',
      'rco2' not in js and 'rco2Unfiltered' not in js, '%r' % js)
# Every averaged field is type checked, with its own reason.
bad_fields = []
for field in Service.AVERAGED_FIELDS:
    sane_r = create_test_reading(datetime.now(tz=tz.gettz('UTC')))
    setattr(sane_r, field, 900)
    ok, reason = Service.is_sane(sane_r)
    if ok or reason != '%s not instance of float' % field:
        bad_fields.append((field, reason))
check('an int in any float field is rejected, naming the field', bad_fields == [], '%r' % bad_fields)

# 11. parse_response: rco2Unfiltered is set from the monitor's rco2, and a
#     non-finite value counts as absent for both.
class FakeResponse(object):
    def __init__(self, j: Dict[str, Any]) -> None:
        self.j = j
        self.text = dumps(j)
    def json(self) -> Dict[str, Any]:
        return self.j

def device_json(rco2: Any) -> Dict[str, Any]:
    j: Dict[str, Any] = {'serialno': '000000000000', 'pm02': 0.23, 'atmp': 21.66,
                         'rhum': 60.74, 'boot': 14, 'bootCount': 14, 'model': 'I-9PSL'}
    if rco2 is not None:
        j['rco2'] = rco2
    return j

parsed = Service.parse_response(FakeResponse(device_json(514)))      # type: ignore
check('parse: rco2Unfiltered set from rco2',
      parsed.rco2 == 514.0 and parsed.rco2Unfiltered == 514.0
      and isinstance(parsed.rco2Unfiltered, float), '%r' % parsed)
parsed = Service.parse_response(FakeResponse(device_json(None)))     # type: ignore
check('parse: rco2 absent leaves both None', parsed.rco2 is None and parsed.rco2Unfiltered is None)
for bad in (float('nan'), float('inf'), float('-inf')):
    # Through real json text: Python's json reads NaN and Infinity.
    parsed = Service.parse_response(FakeResponse(loads(dumps(device_json(bad)))))  # type: ignore
    check('parse: rco2 %r counts as absent' % bad,
          parsed.rco2 is None and parsed.rco2Unfiltered is None, '%r' % parsed)
# Every other number field the monitor sends gets the same rule.
device_float_fields = [f for f in Service.AVERAGED_FIELDS if f != 'rco2Unfiltered']
j = device_json(514)
for field in device_float_fields:
    j.setdefault(field, 1.0)
finite = Service.parse_response(FakeResponse(loads(dumps(j))))    # type: ignore
check('parse: every number field is read', all(getattr(finite, f) is not None for f in device_float_fields),
      '%r' % finite)
for field in device_float_fields:
    j[field] = float('nan')
parsed = Service.parse_response(FakeResponse(loads(dumps(j))))    # type: ignore
check('parse: a non-finite value counts as absent in every number field',
      [f for f in device_float_fields if getattr(parsed, f) is not None] == [], '%r' % parsed)

# 12. co2_median over spikes, steps, missed polls and clock steps (30 s
#     polling, 135 s age limit).
UTC = tz.gettz('UTC')
T0 = 1500000000   # a multiple of 300, so of 30 and 60 too

def at(secs: float) -> datetime:
    return datetime.fromtimestamp(T0 + secs, tz=UTC)

def medians(seq: List[Any], age_limit_secs: float = 135.0) -> List[Optional[float]]:
    """seq: (seconds, unfiltered CO2) pairs.  Returns each reading's rco2."""
    buf: List[Any] = []
    out: List[Optional[float]] = []
    for secs, value in seq:
        r = create_test_reading(at(secs))
        r.rco2 = value
        r.rco2Unfiltered = value
        out.append(Service.co2_median(buf, r, age_limit_secs).rco2)
    return out

def every30(values: List[Optional[float]]) -> List[Any]:
    return [(30 * i, v) for i, v in enumerate(values)]

got = medians(every30([500.0, 502.0, 900.0, 504.0, 506.0]))
check('median: a one-reading spike up is never served', got == [None, None, 502.0, 504.0, 506.0], '%r' % got)
got = medians(every30([500.0, 502.0, 100.0, 504.0, 506.0]))
check('median: a one-reading spike down is never served', got == [None, None, 500.0, 502.0, 504.0], '%r' % got)
got = medians(every30([520.0, 520.0, 520.0, 440.0, 440.0]))
check('median: a step change is served one poll late', got == [None, None, 520.0, 520.0, 440.0], '%r' % got)
got = medians(every30([500.0, 510.0, 520.0, 530.0, 540.0]))
check('median: a steady rise is served one poll late', got == [None, None, 510.0, 520.0, 530.0], '%r' % got)
got = medians(every30([500.0, 502.0, 300.0, 310.0, 504.0, 506.0]))
check('median: a fault spanning two readings LEAKS its milder value',
      got == [None, None, 500.0, 310.0, 310.0, 504.0], '%r' % got)
# Stamps land after each poll slot by the read's duration; the slower read
# coming last is the case that sat on the old limit.
got = medians([(0.1, 500.0), (30.1, 502.0), (60.1, 504.0), (150.9, 506.0)])
check('median: two missed polls keep three entries, a slow read included',
      got == [None, None, 502.0, 504.0], '%r' % got)
got = medians([(0.9, 500.0), (30.9, 502.0), (60.9, 504.0), (180.1, 506.0), (210.1, 508.0), (240.1, 510.0)])
check('median: three missed polls withhold CO2 for two readings, a fast read included',
      got == [None, None, 502.0, None, None, 508.0], '%r' % got)
got = medians([(0, 500.0), (30, 502.0), (60, 504.0), (1060, 506.0), (1090, 508.0), (1120, 510.0)])
check('median: a forward clock step empties the buffer', got == [None, None, 502.0, None, None, 508.0], '%r' % got)
got = medians([(0, 500.0), (30, 502.0), (60, 504.0), (-500, 900.0), (-470, 506.0)])
check('median: a spike across a backward clock step is still voted out',
      got == [None, None, 502.0, 504.0, 506.0], '%r' % got)

buf: List[Any] = [(T0 - 30.0, 500.0), (float(T0), 502.0)]
r = create_test_reading(at(30))
r.rco2 = None
r.rco2Unfiltered = None
m = Service.co2_median(buf, r, 120.0)
check('median: a reading without CO2 gets no rco2 and leaves the buffer alone',
      m.rco2 is None and buf == [(T0 - 30.0, 500.0), (float(T0), 502.0)], '%r' % buf)
r = create_test_reading(at(60))
r.rco2Unfiltered = 900.0
m = Service.co2_median(buf, r, 120.0)
check('median: rco2Unfiltered and the other fields are untouched, the input is not mutated',
      m.rco2 == 502.0 and m.rco2Unfiltered == 900.0 and r.rco2 == 547.0
      and dataclasses.replace(m, rco2=r.rco2) == r, '%r' % m)

def age_limit(pollfreq_secs: int) -> float:
    return Service('sensor.invalid', 80, 28, 10, pollfreq_secs, 0, 300, 0, db).co2_age_limit_secs
check('age limit is max(120, 3 x poll-freq-secs) plus half a poll',
      (age_limit(10), age_limit(30), age_limit(60), age_limit(120)) == (125, 135, 210, 420),
      '%r' % ((age_limit(10), age_limit(30), age_limit(60), age_limit(120)),))

# 12a. secs_to_next_poll finds the next offset slot, wherever now falls.
def secs_to_next_poll(pollfreq_secs: int, offset: int, now_secs: float) -> float:
    saved = service_module.time
    service_module.time = types.SimpleNamespace(time=lambda: T0 + now_secs)   # type: ignore
    try:
        return Service('sensor.invalid', 80, 28, 10, pollfreq_secs, offset, 300, 0, db).secs_to_next_poll()
    finally:
        service_module.time = saved   # type: ignore
got = [round(secs_to_next_poll(30, 15, now), 3) for now in (5.0, 15.0, 15.5, 31.0, 44.0)]
check('next poll with offset 15: a start before the offset slot waits for it, a slow read keeps its slot',
      got == [10.0, 30.0, 29.5, 14.0, 1.0], '%r' % got)
got = [round(secs_to_next_poll(30, 0, now), 3) for now in (0.0, 5.0, 29.5)]
check('next poll with offset 0', got == [30.0, 25.0, 0.5], '%r' % got)

# 13. compute_avg averages each field over the readings that carry it.
readings = [create_test_reading(at(0)), create_test_reading(at(30)), create_test_reading(at(60))]
readings[0].rco2 = None
readings[1].rco2 = 500.0
readings[2].rco2 = 510.0
for r in readings:
    r.pm50Count = None
readings[0].atmp = 20.0
readings[1].atmp = 21.0
readings[2].atmp = 23.0
readings[2].firmware = '3.3.9'
avg = Service.compute_avg(readings)
check('compute_avg: a field missing from some readings averages the rest', avg.rco2 == 505.0, '%r' % avg.rco2)
check('compute_avg: a field missing from all readings is None', avg.pm50Count is None)
check('compute_avg: a field in every reading averages them all', avg.atmp == 64.0 / 3, '%r' % avg.atmp)
check('compute_avg: last-reading fields come from the last reading',
      avg.measurementTime == at(60) and avg.firmware == '3.3.9')
last_fields = {'measurementTime', 'serialno', 'ledMode', 'firmware', 'model', 'boot', 'bootCount'}
float_fields = {f.name for f in dataclasses.fields(Reading) if f.type == Optional[float]}
check('compute_avg averages every float field of Reading, and only those',
      set(Service.AVERAGED_FIELDS) == float_fields
      and float_fields | last_fields == {f.name for f in dataclasses.fields(Reading)},
      '%r' % (float_fields ^ set(Service.AVERAGED_FIELDS),))

# 14. The migration, on a database built from the 2.1 schema.
OLD_CREATE_READING_TABLE: str = ('CREATE TABLE Reading ('
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

def temp_db_name() -> str:
    t = tempfile.NamedTemporaryFile(prefix='airgradient-proxy-test', suffix='.sdb', delete=False)
    t.close()
    os.unlink(t.name)
    return t.name

def column_names(db_file: str) -> List[str]:
    with sqlite3.connect(db_file) as conn:
        return [row[1] for row in conn.execute('PRAGMA table_info(Reading);')]

old_db_file = temp_db_name()
with sqlite3.connect(old_db_file) as conn:
    conn.execute(OLD_CREATE_READING_TABLE)
    for record_type in (0, 1, 2):   # CURRENT, ARCHIVE, TWO_MINUTE, as 2.1 wrote them
        conn.execute('INSERT INTO Reading (record_type, timestamp, serialno, rco2) VALUES (?, ?, ?, ?);',
                     (record_type, float(T0), '000000000000', 512.0))
logged: List[str] = []
log.logmsg = lambda level, msg: logged.append(msg)   # type: ignore
try:
    old_db = Database(old_db_file)
    old_db.ensure_schema()
    old_db.ensure_schema()
finally:
    del log.logmsg   # type: ignore
check('migration logs exactly one line, once', logged == ['Added column rco2Unfiltered to the Reading table.'], '%r' % logged)
fresh_db_file = temp_db_name()
Database.create(fresh_db_file)
check('migrated columns equal a fresh database\'s, in order',
      column_names(old_db_file) == column_names(fresh_db_file),
      '%r vs %r' % (column_names(old_db_file), column_names(fresh_db_file)))
logged.clear()
log.logmsg = lambda level, msg: logged.append(msg)   # type: ignore
try:
    Database(fresh_db_file).ensure_schema()
finally:
    del log.logmsg   # type: ignore
check('a fresh database needs no migration', logged == [], '%r' % logged)
old_rows = list(old_db.fetch_archive_readings(0))
check('an old archive row is kept, and reads back with rco2Unfiltered None',
      len(old_rows) == 1 and old_rows[0].rco2 == 512.0 and old_rows[0].rco2Unfiltered is None, '%r' % old_rows)
check('the migration clears the old CURRENT and TWO_MINUTE rows, so no unfiltered rco2 is served',
      old_db.fetch_current_reading_as_json() == '{}' and old_db.fetch_two_minute_reading_as_json() == '{}')
old_db.save_current_reading(create_test_reading(at(30)))
old_db.save_two_minute_reading(create_test_reading(at(30)))
old_db.ensure_schema()
check('a later start of a migrated database leaves CURRENT and TWO_MINUTE alone',
      len(list(old_db.fetch_current_readings())) == 1 and len(list(old_db.fetch_two_minute_readings())) == 1)
new_r = create_test_reading(at(300))
new_r.rco2Unfiltered = 515.0
old_db.save_archive_reading(new_r)
check('a migrated database stores rco2Unfiltered',
      [r.rco2Unfiltered for r in old_db.fetch_archive_readings(0)] == [None, 515.0])
classifier = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..',
                          'etc', 'logwatch', 'scripts', 'services', 'airgradient-proxy')
report = subprocess.run(['perl', classifier], input='Oct 27 12:00:00 host airgradient-proxy[1]: %s\n' % 'Added column rco2Unfiltered to the Reading table.',
                        capture_output=True, text=True, check=True).stdout
check('the logwatch classifier counts the migration line',
      'Database Columns Added' in report and 'unmatched' not in report, report)
os.unlink(old_db_file)
os.unlink(fresh_db_file)

# 15. The real do_loop on a fake clock.  collect_data is stubbed to hand the
#     next device json to the real parse_response; utc_now, the service
#     module's time.time and its sleep all read one fake clock.  sleep
#     snapshots the CURRENT and TWO_MINUTE records each time round, and stops
#     the loop when the polls run out.
class StopLoop(BaseException):
    pass

OUTAGE = object()

class LoopRun(object):
    def __init__(self, pollfreq_secs: int, values: List[Any], read_secs: Optional[List[float]] = None,
                 start_secs: float = 10.0) -> None:
        self.clock: float = float(T0) + start_secs
        self.pending: List[Any] = list(values)
        # How long each read takes: the reading is stamped when it ends.
        self.read_secs: List[float] = list(read_secs) if read_secs else [0.0] * len(values)
        self.events: List[Any] = []
        self.current: Dict[float, Reading] = {}
        self.two_minute: Dict[float, Reading] = {}
        self.db_file = temp_db_name()
        self.db = Database.create(self.db_file)
        saved = (Service.__dict__['utc_now'], Service.__dict__['collect_data'],
                 service_module.time, service_module.sleep)
        Service.utc_now = staticmethod(lambda: datetime.fromtimestamp(self.clock, tz=UTC))  # type: ignore
        Service.collect_data = staticmethod(self.collect)   # type: ignore
        service_module.time = types.SimpleNamespace(        # type: ignore
            time=lambda: self.clock, strftime=time.strftime, localtime=time.localtime)
        service_module.sleep = self.sleep                   # type: ignore
        log.logmsg = lambda level, msg: None                # type: ignore
        try:
            try:
                Service('sensor.invalid', 80, 28, 10, pollfreq_secs, 0, 300, 0, self.db).do_loop()
            except StopLoop:
                pass
            self.archive: List[Reading] = list(self.db.fetch_archive_readings(0))
        finally:
            Service.utc_now, Service.collect_data, service_module.time, service_module.sleep = saved  # type: ignore
            del log.logmsg   # type: ignore
            os.unlink(self.db_file)

    def collect(self, session: Any, hostname: str, port: int, timeout_secs: int, long_read_secs: int) -> Reading:
        self.events.append(('collect', self.clock - T0))
        value = self.pending.pop(0)
        self.clock += self.read_secs.pop(0)
        if value is OUTAGE:
            raise ConnectionError('outage')
        return Service.parse_response(FakeResponse(device_json(value)))  # type: ignore

    def sleep(self, secs: float) -> None:
        self.events.append(('sleep', self.clock - T0))
        # Keyed by poll slot: a read's duration is under a second.
        for r in self.db.fetch_current_readings():
            self.current[float(int(r.measurementTime.timestamp() - T0))] = r
        for r in self.db.fetch_two_minute_readings():
            self.two_minute[float(int(r.measurementTime.timestamp() - T0))] = r
        if not self.pending:
            raise StopLoop()
        self.clock += secs

def co2(r: Optional[Reading]) -> Any:
    return None if r is None else (r.rco2, r.rco2Unfiltered)

# A start, then a spike, polled across an archive boundary.
values = [500.0, 502.0, 504.0, 506.0, 900.0, 508.0, 510.0, 512.0, 514.0, 516.0]
run = LoopRun(30, values)
check('loop: the first poll waits for the schedule',
      run.events[0] == ('sleep', 10.0) and run.events[1] == ('collect', 30.0), '%r' % run.events[:2])
expected_medians = [None, None, 502.0, 504.0, 506.0, 508.0, 510.0, 510.0, 512.0, 514.0]
got_current = [co2(run.current.get(30.0 * (i + 1))) for i in range(len(values))]
check('loop: CURRENT carries the median and the unfiltered value',
      got_current == list(zip(expected_medians, values)), '%r' % got_current)
check('loop: CO2 is withheld for the first two readings after a start, rco2Unfiltered is not',
      got_current[0] == (None, 500.0) and got_current[1] == (None, 502.0), '%r' % got_current[:2])
check('loop: the spike is in CURRENT\'s rco2Unfiltered, never its rco2',
      got_current[4] == (506.0, 900.0), '%r' % (got_current[4],))
check('loop: the two-minute record averages the medians it has (spike poll)',
      co2(run.two_minute.get(150.0)) == (504.0, 582.4), '%r' % (co2(run.two_minute.get(150.0)),))
check('loop: the two-minute record spans two minutes (last poll)',
      co2(run.two_minute.get(300.0)) == (510.8, 512.0), '%r' % (co2(run.two_minute.get(300.0)),))
check('loop: one archive record at the boundary, from medians, still carrying CO2 after the start',
      len(run.archive) == 1 and run.archive[0].measurementTime == at(300)
      and co2(run.archive[0]) == (508.25, 547.2) and abs((run.archive[0].pm02 or 0.0) - 0.23) < 1e-9,
      '%r' % [(r.measurementTime, co2(r)) for r in run.archive])
served = [r.rco2 for r in list(run.current.values()) + list(run.two_minute.values()) + run.archive]
check('loop: the spike reaches no rco2 anywhere', all(v is None or v < 520.0 for v in served), '%r' % served)

# Outages just shorter and just longer than the age limit, at 30 s and 60 s.
# Read durations shift each stamp past its poll slot.  Where the median must
# survive, the read after the outage is the slow one, so the oldest entry is
# a little over 4 (or 3) polls old: the case the limit once sat exactly on.
# Where CO2 must be withheld, the reads before the outage are the slow ones,
# so the entry that must go is a little under 5 (or 4) polls old: the case a
# too-wide margin would wrongly keep.
run = LoopRun(30, [500.0, 502.0, 504.0, OUTAGE, OUTAGE, 506.0, 508.0],
              [0.1, 0.1, 0.1, 0.1, 0.1, 0.8, 0.1])
check('loop, 30 s: two missed polls keep the median, a slow read included',
      co2(run.current.get(180.0)) == (504.0, 506.0), '%r' % (co2(run.current.get(180.0)),))
run = LoopRun(30, [500.0, 502.0, 504.0, OUTAGE, OUTAGE, OUTAGE, 506.0, 508.0, 510.0],
              [0.8, 0.8, 0.8, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1])
got = [co2(run.current.get(t)) for t in (210.0, 240.0, 270.0)]
check('loop, 30 s: three missed polls withhold CO2 for two readings',
      got == [(None, 506.0), (None, 508.0), (508.0, 510.0)], '%r' % got)
run = LoopRun(60, [500.0, 502.0, 504.0, OUTAGE, 506.0], [0.1, 0.1, 0.1, 0.1, 0.8])
check('loop, 60 s: one missed poll keeps the median, a slow read included',
      co2(run.current.get(300.0)) == (504.0, 506.0), '%r' % (co2(run.current.get(300.0)),))
run = LoopRun(60, [500.0, 502.0, 504.0, OUTAGE, OUTAGE, 506.0, 508.0, 510.0],
              [0.8, 0.8, 0.8, 0.1, 0.1, 0.1, 0.1, 0.1])
got = [co2(run.current.get(t)) for t in (360.0, 420.0, 480.0)]
check('loop, 60 s: two missed polls withhold CO2 for two readings',
      got == [(None, 506.0), (None, 508.0), (508.0, 510.0)], '%r' % got)

os.unlink(tmp.name)
print()
if failures:
    print('%d FAILURES: %s' % (len(failures), failures))
    sys.exit(1)
print('ALL PASSED')
