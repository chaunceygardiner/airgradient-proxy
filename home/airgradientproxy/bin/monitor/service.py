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
                wifi            = float(j['wifi']) if 'wifi' in j else None,
                pm01            = float(j['pm01']) if 'pm01' in j else None,
                pm02            = float(j['pm02']) if 'pm02' in j else None,
                pm10            = float(j['pm10']) if 'pm10' in j else None,
                pm02Compensated = float(j['pm02Compensated']) if 'pm02Compensated' in j else None,
                pm01Standard    = float(j['pm01Standard']) if 'pm01Standard' in j else None,
                pm02Standard    = float(j['pm02Standard']) if 'pm02Standard' in j else None,
                pm10Standard    = float(j['pm10Standard']) if 'pm10Standard' in j else None,
                rco2            = float(j['rco2']) if 'rco2' in j else None,
                pm003Count      = float(j['pm003Count']) if 'pm003Count' in j else None,
                pm005Count      = float(j['pm005Count']) if 'pm005Count' in j else None,
                pm01Count       = float(j['pm01Count']) if 'pm01Count' in j else None,
                pm02Count       = float(j['pm02Count']) if 'pm02Count' in j else None,
                pm50Count       = float(j['pm50Count']) if 'pm50Count' in j else None,
                pm10Count       = float(j['pm10Count']) if 'pm10Count' in j else None,
                atmp            = float(j['atmp']) if 'atmp' in j else None,
                atmpCompensated = float(j['atmpCompensated']) if 'atmpCompensated' in j else None,
                rhum            = float(j['rhum']) if 'rhum' in j else None,
                rhumCompensated = float(j['rhumCompensated']) if 'rhumCompensated' in j else None,
                tvocIndex       = float(j['tvocIndex']) if 'tvocIndex' in j else None,
                tvocRaw         = float(j['tvocRaw']) if 'tvocRaw' in j else None,
                noxIndex        = float(j['noxIndex']) if 'noxIndex' in j else None,
                noxRaw          = float(j['noxRaw']) if 'noxRaw' in j else None,
                boot            = j['boot'] if 'boot' in j else None,
                bootCount       = j['bootCount'] if 'bootCount' in j else None,
                ledMode         = j['ledMode'] if 'ledMode' in j else None,
                firmware        = j['firmware'] if 'firmware' in j else None,
                model           = j['model'] if 'model' in j else None)
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
        avg_reading.noxIndex        = avg_reading.noxIndex       / count if avg_reading.noxIndex is not None else None
        avg_reading.noxRaw          = avg_reading.noxRaw          / count if avg_reading.noxRaw is not None else None

        return avg_reading

    @staticmethod
    def utc_now() -> datetime:
        return datetime.now(tz=tz.gettz('UTC'))

    @staticmethod
    def is_int(value: Any) -> bool:
        # bool is a subclass of int; a JSON true/false must not pass as an int reading.
        return isinstance(value, int) and not isinstance(value, bool)

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
        next_poll_event = int(now / self.pollfreq_secs) * self.pollfreq_secs + self.pollfreq_secs
        secs_to_event = next_poll_event - now
        # Add pollfreq_offset to computed next event.
        secs_to_event += self.pollfreq_offset
        log.debug('Next poll in %f seconds' % secs_to_event)
        return secs_to_event

    def next_archive_boundary(self) -> float:
        return (int(time.time() / self.arcint_secs) + 1) * self.arcint_secs

    def do_loop(self) -> None:
        archive_readings   : List[Reading] = []
        two_minute_readings: List[Reading] = []

        first_time: bool = True
        log.debug('Started main loop.')
        session: Optional[requests.Session] = None
        next_gc_ts: float = time.time() + self.gc_interval_secs
        # The archive record is written by the first poll at or past the
        # boundary (rather than scheduling ARCHIVE as its own event), so a
        # slow sensor read that straddles the boundary delays the record
        # instead of skipping it.
        next_arc_ts: float = self.next_archive_boundary()

        while True:
            if first_time:
                first_time = False
            else:
                # sleep until next poll
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
                        # Reset archive_readings for new archive cycle.
                        archive_readings.clear()
                    except Exception as e:
                        log.critical('Could not save archive reading to database: %s: %s' % (self.database, e))

            # Periodically collect cyclic garbage, but only on a non-archive
            # poll: the loop is about to go idle, and the pause never stacks
            # on top of archive processing.
            if self.gc_interval_secs != 0 and not archive_due and time.time() >= next_gc_ts:
                gc_start = time.time()
                unreachable: int = gc.collect()
                log.info('Garbage collected %d objects in %.3f seconds.' % (unreachable, time.time() - gc_start))
                next_gc_ts = time.time() + self.gc_interval_secs
