# airgradient-proxy

A proxy and archiver for [AirGradient](https://www.airgradient.com/) air quality
sensors.  airgradient-proxy runs as a daemon that polls an AirGradient sensor on the
local network, sanity checks each reading, maintains rolling averages, stores archive
records in a sqlite database, and serves everything through a small REST API.

## Why not query the sensor directly?

* The sensor's processor is easily overwhelmed.  The proxy absorbs client load and
  queries the sensor at a steady, configurable rate.
* The proxy archives an averaged reading every archive interval.  These archive
  records can be queried later (for example, to backfill a weather database).
* Every reading is sanity checked before it is accepted: field types are verified and
  readings with a clock more than 20 seconds off are rejected.
* Developed with [WeeWX](https://weewx.com) weather software in mind.  Use with the
  [weewx-airgradient](https://github.com/chaunceygardiner/weewx-airgradient)
  extension, which queries the proxy instead of the sensor.

For redundancy, two proxies (on different machines) can poll the same sensor.  Set
`poll-freq-offset` on the second proxy so the two never query the sensor at the same
moment.

## REST API

* `/measures/current` — identical to querying the device directly; returns the latest
  reading.
* `/fetch-current-record` — same as `/measures/current`.
* `/fetch-two-minute-record` — returns an average of the readings from the last two
  minutes.
* `/get-version` — returns the version of the proxy command set (currently `1.0`).
* `/get-earliest-timestamp` — returns the timestamp of the oldest archive record in
  the database.
* `/fetch-archive-records?since_ts=<since_ts>` — returns all archive records with
  timestamp > `<since_ts>` (seconds since the epoch; `since_ts=0` fetches everything).
* `/fetch-archive-records?since_ts=<since_ts>,max_ts=<max_ts>` — limits the records
  returned to timestamps <= `<max_ts>`.
* `/fetch-archive-records?since_ts=<since_ts>,limit=<count>` — returns at most
  `<count>` records.
* `max_ts` and `limit` may be combined:
  `/fetch-archive-records?since_ts=<since_ts>,max_ts=<max_ts>,limit=<count>`.

The JSON returned matches what the AirGradient device itself serves (see the
[AirGradient local server spec](https://github.com/airgradienthq/arduino/blob/master/docs/local-server.md)
for the fields); the proxy adds a `measurementTime` field.

## Requirements

* Debian or Raspberry Pi OS (tested there; on other platforms these instructions and
  the install script serve as a specification of the steps needed).
* systemd (the service is installed as a systemd unit).
* Python 3 with the `python3-configobj`, `python3-dateutil` and `python3-requests`
  packages.
* rsyslog (recommended: it routes the daemon's log to
  `/var/log/airgradient-proxy.log`; without it the log is only in the systemd
  journal).
* logwatch (optional; a log classifier is installed if logwatch is present).

## Installation

```sh
sudo apt install rsyslog python3-configobj python3-dateutil python3-requests
cd <airgradient-proxy-src-dir>
sudo ./install
```

Every setting can be given as a command line option; on a fresh install, the script asks
for anything not specified (press Enter to accept the shown default), and `-y` accepts
the default for everything not specified.  `./install -h` lists all options.

```sh
# Fully interactive:
sudo ./install

# Scripted; defaults for everything not given:
sudo ./install --sensor airgradient --poll-freq-offset 15 -y

# Upgrade an existing installation (settings come from the installed conf):
sudo ./install -y
```

On a fresh install, the script:

* creates an `airgradientproxy` system user that the daemon runs as;
* copies the program to the target directory (default `/home/airgradientproxy`);
* generates `<target-dir>/airgradientproxy.conf` from the chosen settings;
* installs the rsyslog, logrotate and logwatch configuration;
* installs, enables and starts the `airgradient-proxy` systemd service.

### Upgrading (re-running the script)

Re-running the script upgrades in place, without prompting:

* `airgradientproxy.conf` is **migrated**, never regenerated from scratch: its values
  are kept (options given on the command line win), options new to the version are
  added with their defaults, and deprecated options are removed.  The previous conf is
  saved as `airgradientproxy.conf.bak`.  (Hand-written comments are not carried over.)
* **Other conf files are never overwritten.**  The rsyslog, logrotate and logwatch
  conf files are installed only when absent; once installed they are yours to
  customize.  If the version shipped with a release differs from what is installed,
  your file is left alone and the shipped version is written alongside as
  `<file>.dpkg-new` for hand merging (removed automatically once the installed file
  matches the shipped one).
* Program files and the logwatch classifier script are refreshed (the classifier
  matches the daemon's log messages verbatim, so it ships with the daemon).  Any
  file that is a **symlink is left in place**, so files symlinked to a source
  checkout keep working.
* The daemon is disturbed as little as possible: it is restarted only when the
  program files, `airgradientproxy.conf` or the systemd unit actually changed, and
  rsyslog is restarted only when its conf was newly installed.  An install that
  changed nothing leaves the running daemon alone.
* An installation that used the old SysV init script is migrated to the systemd
  unit automatically.

To uninstall (the target directory, with its configuration and database, is left in
place):

```sh
sudo ./install --uninstall [<target-dir>]
```

## Managing the service

```sh
sudo systemctl status airgradient-proxy
sudo systemctl restart airgradient-proxy
sudo journalctl -u airgradient-proxy     # service-level messages
tail -f /var/log/airgradient-proxy.log   # the daemon's log
```

The log is rotated weekly (four rotations kept).  If logwatch is installed, an
airgradient-proxy section (readings saved, archive records added, errors categorized)
appears in the regular logwatch report.

## Configuration

`<target-dir>/airgradientproxy.conf` is a flat `key = value` file:

| Key                     | Default | Description |
| ----------------------- | ------- | ----------- |
| `debug`                 | 0       | Log debug messages. |
| `log-to-stdout`         | 0       | Log to stdout instead of syslog. |
| `service-name`          | airgradient-proxy | Syslog program name. |
| `hostname`              | (required) | DNS name or IP address of the AirGradient sensor. |
| `port`                  | 80      | Port of the sensor. |
| `timeout-secs`          | 28      | Timeout for sensor reads. |
| `long-read-secs`        | 10      | Log sensor reads that take longer than this. |
| `server-port`           | 8080    | Port on which the proxy's REST API listens. |
| `poll-freq-secs`        | 30      | How often to poll the sensor. |
| `poll-freq-offset`      | 0       | Offset the polls by this many seconds.  Set a non-zero offset on the second proxy when two proxies poll the same sensor. |
| `archive-interval-secs` | 300     | How often to write an archive record (must be a multiple of `poll-freq-secs`). |
| `gc-interval-secs`      | 3600    | Run a full cyclic garbage collection pass this often; 0 disables. |
| `database-file`         | (required) | Path of the sqlite database. |

## Testing

```sh
tests/test-install             # install script tests; runs unprivileged in a sandbox
python3 tests/test-monitor.py  # offline tests: database, fetch semantics, REST parsing
python3 tests/test-live.py     # live tests against a real sensor (hostname from the conf)
```
