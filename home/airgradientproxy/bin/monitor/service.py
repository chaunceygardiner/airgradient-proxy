#!/usr/bin/python3

# Copyright (c) 2025-2026 John A Kline
# See the file LICENSE for your full rights.

"""The polling service: read the sensor every poll-freq-secs, sanity check
each reading, maintain rolling averages, and write current/two-minute/archive
records to the database.
"""

import calendar
import copy
import gc
import math
import time

import requests

from datetime import datetime
from datetime import timedelta
from dateutil import tz
from time import sleep
from typing import Any, Dict, List, Optional, Tuple

from monitor import log
from monitor.database import Database
from monitor.model import Reading

class Service(object):
    def __init__(self, hostname: str, port: int, timeout_secs: int,
                 long_read_secs: int, pollfreq_secs: int,
                 pollfreq_offset: int, arcint_secs: int,
                 gc_interval_secs: int, database: Database) -> None:
        self.hostname = hostname
        self.port = port
        self.timeout_secs     = timeout_secs
        self.long_read_secs   = long_read_secs
        self.pollfreq_secs    = pollfreq_secs
        self.pollfreq_offset  = pollfreq_offset
        self.arcint_secs      = arcint_secs
        self.gc_interval_secs = gc_interval_secs
        self.database         = database
        # A CO2 reading older than this no longer votes in the median, even if
        # it is one of the last 3 (see co2_median).  At 30 s polling it is
        # 135 s, which rides out two missed polls in a row.  The half poll is
        # margin: a reading is stamped when the sensor answers, so stamps
        # drift by each read's duration, and without it the oldest survivor
        # of two missed polls sat exactly on the limit and was dropped
        # whenever the later read was the slower one.
        self.co2_age_limit_secs: float = max(120, 3 * pollfreq_secs) + pollfreq_secs / 2

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
    def float_field(j: Dict[str, Any], key: str) -> Optional[float]:
        """j[key] as a float; None when the monitor omitted it, or sent a
        value that is not finite (Python's json accepts NaN and Infinity), so
        it is never stored, averaged or served.  A value that will not convert
        raises, rejecting the reading."""
        if key not in j:
            return None
        value: float = float(j[key])
        return value if math.isfinite(value) else None

    @staticmethod
    def parse_response(response: requests.Response) -> Reading:
        try:
            # convert to json
            j: Dict[str, Any] = response.json()

            # rco2 starts out as the monitor's value; do_loop replaces it with
            # the rolling median and rco2Unfiltered keeps the original.
            rco2: Optional[float] = Service.float_field(j, 'rco2')

            return Reading(
                measurementTime = Service.utc_now(),
                serialno        = j['serialno'],
                wifi            = Service.float_field(j, 'wifi'),
                pm01            = Service.float_field(j, 'pm01'),
                pm02            = Service.float_field(j, 'pm02'),
                pm10            = Service.float_field(j, 'pm10'),
                pm02Compensated = Service.float_field(j, 'pm02Compensated'),
                pm01Standard    = Service.float_field(j, 'pm01Standard'),
                pm02Standard    = Service.float_field(j, 'pm02Standard'),
                pm10Standard    = Service.float_field(j, 'pm10Standard'),
                rco2            = rco2,
                pm003Count      = Service.float_field(j, 'pm003Count'),
                pm005Count      = Service.float_field(j, 'pm005Count'),
                pm01Count       = Service.float_field(j, 'pm01Count'),
                pm02Count       = Service.float_field(j, 'pm02Count'),
                pm50Count       = Service.float_field(j, 'pm50Count'),
                pm10Count       = Service.float_field(j, 'pm10Count'),
                atmp            = Service.float_field(j, 'atmp'),
                atmpCompensated = Service.float_field(j, 'atmpCompensated'),
                rhum            = Service.float_field(j, 'rhum'),
                rhumCompensated = Service.float_field(j, 'rhumCompensated'),
                tvocIndex       = Service.float_field(j, 'tvocIndex'),
                tvocRaw         = Service.float_field(j, 'tvocRaw'),
                noxIndex        = Service.float_field(j, 'noxIndex'),
                noxRaw          = Service.float_field(j, 'noxRaw'),
                boot            = j['boot'] if 'boot' in j else None,
                bootCount       = j['bootCount'] if 'bootCount' in j else None,
                ledMode         = j['ledMode'] if 'ledMode' in j else None,
                firmware        = j['firmware'] if 'firmware' in j else None,
                model           = j['model'] if 'model' in j else None,
                rco2Unfiltered  = rco2)
        except Exception as e:
            log.info('parse_response: %r raised exception %r' % (response.text, e))
            raise e

    @staticmethod
    def datetime_display(dt: datetime) -> str:
        ts = dt.timestamp()
        return "%s (%d)" % (time.strftime("%Y-%m-%d %H:%M:%S %Z", time.localtime(ts)), ts)

    # The fields compute_avg averages.  Every other field (measurementTime,
    # serialno, ledMode, firmware, model, boot, bootCount) is taken from the
    # last reading.
    AVERAGED_FIELDS: Tuple[str, ...] = (
        'wifi', 'pm01', 'pm02', 'pm10', 'pm02Compensated', 'pm01Standard',
        'pm02Standard', 'pm10Standard', 'rco2', 'pm003Count', 'pm005Count',
        'pm01Count', 'pm02Count', 'pm50Count', 'pm10Count', 'atmp',
        'atmpCompensated', 'rhum', 'rhumCompensated', 'tvocIndex', 'tvocRaw',
        'noxIndex', 'noxRaw', 'rco2Unfiltered')

    @staticmethod
    def compute_avg(readings: List[Reading]) -> Reading:
        # We are guaranteed at least one reading.
        avg_reading: Reading = copy.copy(readings[-1])

        # Each field is averaged over the readings that carry it, and is None
        # only when none does: one reading without CO2 (the firmware omits
        # rco2 when the sensor returns an invalid value, and do_loop withholds
        # it for the first two readings after a start) must not empty a whole
        # record's CO2.
        for field in Service.AVERAGED_FIELDS:
            values: List[float] = [getattr(r, field) for r in readings if getattr(r, field) is not None]
            setattr(avg_reading, field, sum(values) / len(values) if values else None)

        return avg_reading

    @staticmethod
    def co2_median(buf: List[Tuple[float, float]], reading: Reading, age_limit_secs: float) -> Reading:
        """Return a copy of reading whose rco2 is the median of the last 3
        CO2 values, by count, from readings no older than age_limit_secs; or
        None until there are 3.  The monitor's CO2 sometimes jumps for exactly
        one reading, and a median of 3 never passes a lone outlier through, at
        the cost of serving CO2 about one poll late.

        buf is the caller's state: (timestamp, unfiltered CO2) of recent sane
        readings, oldest first, updated in place.  The value voted is the
        reading's rco2Unfiltered, set at parse time; a reading without one
        gets no rco2 and leaves buf alone.  Every field other than rco2 is
        returned untouched."""
        median_reading: Reading = copy.copy(reading)
        value: Optional[float] = reading.rco2Unfiltered
        if value is None:
            median_reading.rco2 = None
            return median_reading

        ts: float = reading.measurementTime.timestamp()
        buf[:] = [entry for entry in buf if ts - entry[0] <= age_limit_secs]
        buf.append((ts, value))
        # By position, oldest first: buf is never sorted.
        del buf[:-3]

        median_reading.rco2 = sorted(entry[1] for entry in buf)[1] if len(buf) == 3 else None
        return median_reading

    @staticmethod
    def utc_now() -> datetime:
        return datetime.now(tz=tz.gettz('UTC'))

    @staticmethod
    def is_int(value: Any) -> bool:
        # bool is a subclass of int; a JSON true/false must not pass as an int reading.
        return isinstance(value, int) and not isinstance(value, bool)

    @staticmethod
    def trim_two_minute_readings(two_minute_readings: List[Reading]) -> None:
        two_minutes_ago: datetime = Service.utc_now() - timedelta(seconds=120)
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
        # Every averaged field is a float (or absent).
        for field in Service.AVERAGED_FIELDS:
            value = getattr(reading, field)
            if value is not None and not isinstance(value, float):
                return False, '%s not instance of float' % field
        if reading.boot is not None and not Service.is_int(reading.boot):
            return False, 'boot not instance of int'
        if reading.bootCount is not None and not Service.is_int(reading.bootCount):
            return False, 'bootCount not instance of int'
        if reading.ledMode is not None and not isinstance(reading.ledMode, str):
            return False, 'ledMode not instance of str'
        if reading.firmware is not None and not isinstance(reading.firmware, str):
            return False, 'firmware not instance of str'
        if reading.model is not None and not isinstance(reading.model, str):
            return False, 'model not instance of str'

        return True, ''

    def secs_to_next_poll(self) -> float:
        now = time.time()
        # Poll slots are the multiples of pollfreq_secs plus pollfreq_offset.
        # The offset is taken out before rounding down, so the next slot is
        # found even when now falls between a multiple and its offset slot
        # (a start, or a read that ran past the multiple); adding it after
        # rounding skipped that slot and waited up to a poll longer.
        next_poll_event = (int((now - self.pollfreq_offset) / self.pollfreq_secs) + 1) * self.pollfreq_secs + self.pollfreq_offset
        secs_to_event = next_poll_event - now
        log.debug('Next poll in %f seconds' % secs_to_event)
        return secs_to_event

    def next_archive_boundary(self) -> float:
        return (int(time.time() / self.arcint_secs) + 1) * self.arcint_secs

    def do_loop(self) -> None:
        archive_readings   : List[Reading] = []
        two_minute_readings: List[Reading] = []
        # The CO2 median's state (see co2_median).
        co2_buf            : List[Tuple[float, float]] = []

        log.debug('Started main loop.')
        session: Optional[requests.Session] = None
        next_gc_ts: float = time.time() + self.gc_interval_secs
        # The archive record is written by the first poll at or past the
        # boundary (rather than scheduling ARCHIVE as its own event), so a
        # slow sensor read that straddles the boundary delays the record
        # instead of skipping it.
        next_arc_ts: float = self.next_archive_boundary()

        while True:
            # Sleep until the next poll, the first one included: polling off
            # the schedule at a start would put the first two readings ~13 s
            # apart, inside one CO2 fault, and the median would pass it.
            sleep(self.secs_to_next_poll())

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
                    reading = Service.co2_median(co2_buf, reading, self.co2_age_limit_secs)
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
            # if at or past an archive boundary, also write an archive record
            archive_due: bool = time.time() >= next_arc_ts
            if archive_due:
                next_arc_ts = self.next_archive_boundary()
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
                    except Exception as e:
                        log.critical('Could not save archive reading to database: %s: %s' % (self.database, e))
                    # Cleared whether or not the save succeeded: these samples
                    # belong to the period just closed, and carrying them into
                    # the next one would silently fold two periods into one
                    # average on top of the record already lost.
                    archive_readings.clear()

            # Periodically collect cyclic garbage, but only on a non-archive
            # poll: the loop is about to go idle, and the pause never stacks
            # on top of archive processing.
            if self.gc_interval_secs != 0 and not archive_due and time.time() >= next_gc_ts:
                gc_start = time.time()
                unreachable: int = gc.collect()
                log.info('Garbage collected %d objects in %.3f seconds.' % (unreachable, time.time() - gc_start))
                next_gc_ts = time.time() + self.gc_interval_secs
