#!/usr/bin/python3

# Copyright (c) 2026 John A Kline
# See the file LICENSE for your full rights.

"""Shared test helpers.  The importing test script must first put
home/airgradientproxy/bin on sys.path (running a script from tests/ puts this
directory on the path, so `import fixtures` then works).
"""

import os
import sys
import traceback

from datetime import datetime
from typing import Optional

sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.abspath(__file__)), '..', 'home', 'airgradientproxy', 'bin'))

from monitor.model import Reading

class InsaneReading(Exception):
    pass

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

failure_count: int = 0

def print_passed() -> None:
    print(bcolors.OKGREEN + 'PASSED' + bcolors.ENDC)

def print_failed(e: Exception) -> None:
    global failure_count
    failure_count += 1
    print(bcolors.FAIL + 'FAILED' + bcolors.ENDC)
    print(traceback.format_exc())

def float_eq(v1: Optional[float], v2: Optional[float]) -> bool:
    if v1 is None and v2 is None:
        return True
    elif v1 is None or v2 is None:
        return False
    else:
        return abs(v1 - v2) < 0.0001

def create_test_reading(measurementTime: datetime) -> Reading:
    return Reading(
        measurementTime = measurementTime,
        serialno        = '000000000000',
        wifi            = -70.0,
        pm01            = 0.67,
        pm02            = 0.23,
        pm10            = 0.54,
        pm02Compensated = 0.9,
        pm01Standard    = 0.61,
        pm02Standard    = 0.22,
        pm10Standard    = 10.0,
        rco2            = 547.0,
        pm003Count      = 94.33,
        pm005Count      = 75.33,
        pm01Count       = 22.33,
        pm02Count       = 2.33,
        pm50Count       = 0.0,
        pm10Count       = 0.0,
        atmp            = 21.66,
        atmpCompensated = 21.36,
        rhum            = 60.74,
        rhumCompensated = 60.44,
        tvocIndex       = 98.0,
        tvocRaw         = 32358.67,
        noxIndex        = 1.0,
        noxRaw          = 18311.08,
        boot            = 14,
        bootCount       = 14,
        ledMode         = 'pm',
        firmware        = '3.3.7',
        model           = 'I-9PSL')

def create_open_air_test_reading(measurementTime: datetime) -> Reading:
    """An AirGradient Open Air style reading: no pm50Count/pm10Count (indoor
    only fields) and no ledMode."""
    r = create_test_reading(measurementTime)
    r.pm50Count = None
    r.pm10Count = None
    r.ledMode = None
    r.model = 'O-1PST'
    return r
