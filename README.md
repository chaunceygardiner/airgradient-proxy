# airgradient-proxy
Proxy server for AirGradient air quality sensor.  Serves current and archived readings which are averaged over a specified interval.

## What? Why?

airgradient-proxy works in the backround querying the AirGradient sensor and answering queries from clients about air quality.

### Why not query the sensor directly?
* The proxy can handle a higher load, even when running on a Raspberry Pi.
* The proxy will archive average readings every N seconds.  These archives are availble to be queried.
* Developed with WeeWX weather software in mind. Use with the [weewx-airgradient](https://github.com/chaunceygardiner/weewx-airgradient)
  plugin.

### Rest API
* `/measures/current` Identical to quering the device directly, returns the latest reading
* `/get-version' Returns the version of the proxy command set (currently, '1').
* `/get-earliest-timestamp' Returns the the timestamp of the oldest record in the database.
* `/fetch-two-minute-record` returns the two-minute average of the measurements.
* `/fetch-current-record` Same as `/measures/current (see above)
* `/fetch-archive-records?since_ts=<timestamp>` Fetches all archive records >= <timestamp> (i.e., seconds since the epoch).
* `/fetch-archive-records?since_ts=<since_ts>,max_ts=<max_ts>` Fetches all archive records > <since_ts> and <= <max_ts>.
* `/fetch-archive-records?since_ts=<since_ts>,limit=<count>` Fetches up to <count> records  > <since_ts>.
* `/fetch-archive-records?since_ts=<since_ts>,max_ts=<max_ts>,limit=<count>` Fetches up to <count> archive records > <since_ts> and <= <max_ts>.

### Json Specification
See the AirGradient local spec for the json.  In addition to that spec, the proxy adds a 'measurementTime' field.

## Installation Instructions

Note: Tested under Debian (Raspberry OS).  For other platorms,
these instructions and the install script serve as a specification
for what steps are needed to install.

If running debian bookwor and above, you'll needs to use syslog (this install requires it):
```
sudo apt install rsyslog
sudo systemctl enable rsyslog
sudo systemctl start rsyslog
```

In all cases:

```
sudo apt install python3-configobj
sudo apt install python3-dateutil
sudo <airgradient-proxy-src-dir>/install <airgradient-proxy-src-dir> <target-dir> <archive-interval-seconds> <airgradientair-dns-name-or-ip-address>"
```

### Example installation commands:
```
cd ~/software/airgradient-proxy
sudo ./install . /home/airgradientproxy 300 airgradient
```
