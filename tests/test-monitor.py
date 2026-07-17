#!/usr/bin/python3

# Copyright (c) 2026 John A Kline
# See the file LICENSE for your full rights.

"""Offline tests for the monitor and server code: database round trips,
fetch semantics (since_ts/max_ts/limit), sanity checks and REST request
parsing.  Needs no live sensor (the live-sensor path is covered by
tests/test-live.py).

    python3 tests/test-monitor.py

Exits non-zero if any test fails.
"""

import os
import sys
import tempfile

from datetime import datetime, timedelta
from dateutil import tz

sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.abspath(__file__)), '..', 'home', 'airgradientproxy', 'bin'))

from fixtures import create_test_reading, create_open_air_test_reading
from monitor.database import Database
from monitor.service import Service
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

os.unlink(tmp.name)
print()
if failures:
    print('%d FAILURES: %s' % (len(failures), failures))
    sys.exit(1)
print('ALL PASSED')
