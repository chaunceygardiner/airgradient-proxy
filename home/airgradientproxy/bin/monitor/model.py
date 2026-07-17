#!/usr/bin/python3

# Copyright (c) 2025-2026 John A Kline
# See the file LICENSE for your full rights.

"""The data model: sensor readings, and their conversion to the json this
proxy serves (which mirrors the json the AirGradient device serves).
"""

from dataclasses import dataclass
from datetime import datetime
from json import dumps
from typing import Any, Dict, Optional

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

class RecordType:
    CURRENT   : int = 0
    ARCHIVE   : int = 1
    TWO_MINUTE: int = 2

def convert_to_json(reading: Reading) -> str:
    reading_dict: Dict[str, Any] = {}
    if reading.pm01 is not None:
        reading_dict['pm01'           ] = reading.pm01
    if reading.pm02 is not None:
        reading_dict['pm02'           ] = reading.pm02
    if reading.pm10 is not None:
        reading_dict['pm10'           ] = reading.pm10
    if reading.pm01Standard is not None:
        reading_dict['pm01Standard'   ] = reading.pm01Standard
    if reading.pm02Standard is not None:
        reading_dict['pm02Standard'   ] = reading.pm02Standard
    if reading.pm10Standard is not None:
        reading_dict['pm10Standard'   ] = reading.pm10Standard
    if reading.pm003Count is not None:
        reading_dict['pm003Count'     ] = reading.pm003Count
    if reading.pm005Count is not None:
        reading_dict['pm005Count'     ] = reading.pm005Count
    if reading.pm01Count is not None:
        reading_dict['pm01Count'      ] = reading.pm01Count
    if reading.pm02Count is not None:
        reading_dict['pm02Count'      ] = reading.pm02Count
    if reading.pm50Count is not None:
        reading_dict['pm50Count'      ] = reading.pm50Count
    if reading.pm10Count is not None:
        reading_dict['pm10Count'      ] = reading.pm10Count
    if reading.pm02Compensated is not None:
        reading_dict['pm02Compensated'] = reading.pm02Compensated
    if reading.atmp is not None:
        reading_dict['atmp'           ] = reading.atmp
    if reading.atmpCompensated is not None:
        reading_dict['atmpCompensated'] = reading.atmpCompensated
    if reading.rhum is not None:
        reading_dict['rhum'           ] = reading.rhum
    if reading.rhumCompensated is not None:
        reading_dict['rhumCompensated'] = reading.rhumCompensated
    if reading.rco2 is not None:
        reading_dict['rco2'           ] = reading.rco2
    if reading.tvocIndex is not None:
        reading_dict['tvocIndex'      ] = reading.tvocIndex
    if reading.tvocRaw is not None:
        reading_dict['tvocRaw'        ] = reading.tvocRaw
    if reading.noxIndex is not None:
        reading_dict['noxIndex'       ] = reading.noxIndex
    if reading.noxRaw is not None:
        reading_dict['noxRaw'         ] = reading.noxRaw
    if reading.boot is not None:
        reading_dict['boot'           ] = reading.boot
    if reading.bootCount is not None:
        reading_dict['bootCount'      ] = reading.bootCount
    if reading.wifi is not None:
        reading_dict['wifi'           ] = reading.wifi
    if reading.ledMode is not None:
        reading_dict['ledMode'        ] = reading.ledMode
    if reading.serialno is not None:
        reading_dict['serialno'       ] = reading.serialno
    if reading.firmware is not None:
        reading_dict['firmware'       ] = reading.firmware
    if reading.model is not None:
        reading_dict['model'          ] = reading.model

    reading_dict['measurementTime' ] = reading.measurementTime.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    return dumps(reading_dict)
