---
title: Configuration
layout: default
nav_order: 4
---

# Configuration

[airgradient-proxy manual](https://chaunceygardiner.github.io/airgradient-proxy/) · [airgradient-proxy on GitHub](https://github.com/chaunceygardiner/airgradient-proxy) · [Report an issue](https://github.com/chaunceygardiner/airgradient-proxy/issues)

---

The daemon reads a single configuration file, `airgradientproxy.conf`, in
the install target directory (`/home/airgradientproxy/airgradientproxy.conf`
by default).  It is a flat `key = value` file with no sections:

```
debug = 0
log-to-stdout = 0
service-name = airgradient-proxy
hostname = airgradient-indoor
port = 80
timeout-secs = 28
long-read-secs = 10
server-port = 8080
poll-freq-secs = 30
poll-freq-offset = 0
archive-interval-secs = 300
gc-interval-secs = 3600
database-file = /home/airgradientproxy/archive/airgradientproxy.sdb
```

You do not normally write this file by hand.  The install script generates
it on a fresh install and migrates it on an upgrade; every setting below has
a matching `install` option, and passing that option on an upgrade changes
the value in place.  See [Upgrading](upgrading.md).

{: .important }
Restart the service after editing the file by hand:
`sudo systemctl restart airgradient-proxy`.  The daemon reads its
configuration only at startup.

---

## hostname

**Default:** none — this is the one setting you must supply.
**Install option:** `--sensor <host>`

The DNS name or IP address of the AirGradient sensor on your local network.
The daemon polls `http://<hostname>:<port>/measures/current`.

## port

**Default:** `80`
**Install option:** `--sensor-port <port>`

The port the sensor serves on.  AirGradient devices serve their local API on
port 80; there is rarely a reason to change this.

## server-port

**Default:** `8080`
**Install option:** `--server-port <port>`

The port the proxy's own REST API listens on.  The listening socket is
bound to `::`, so the proxy answers on both IPv6 and IPv4.

If you run two proxies on the same machine (polling two different sensors),
give them different `server-port` values.

## poll-freq-secs

**Default:** `30`
**Install option:** `--poll-freq <secs>`

How often the sensor is polled, in seconds.  Polls land on multiples of this
value, so `30` means polls at :00 and :30 of every minute.

This is also the resolution of everything downstream: the two-minute average
is computed from the polls in the last two minutes, and an archive record is
the average of the polls in its archive interval.

## poll-freq-offset

**Default:** `0`
**Install option:** `--poll-freq-offset <secs>`

Seconds added to each computed poll time.  Its only purpose is to stagger a
second proxy polling the same sensor, so that the two never hit the sensor
at the same instant.  With `poll-freq-secs = 30`, setting the second proxy's
offset to `15` puts its polls at :15 and :45.

Leave it at `0` on the first proxy.

## archive-interval-secs

**Default:** `300`
**Install option:** `--archive-interval <secs>`

How often an archive record is written to the database, in seconds.  The
record is the average of every sane reading taken during the interval.

{: .important }
This must be a multiple of [`poll-freq-secs`](#poll-freq-secs).  The install
script refuses a value that is not, and so does the daemon at startup.

If a client backfills from this proxy's archive history, set this to match
the client's own archive interval.  For WeeWX and the
[weewx-airgradient](https://github.com/chaunceygardiner/weewx-airgradient)
extension, that is WeeWX's `archive_interval`, which is 300 seconds on most
installations.

## timeout-secs

**Default:** `28`
**Install option:** `--timeout <secs>`

How long to wait for the sensor to answer before giving up on a reading.  A
read that times out is logged and skipped; the daemon carries on.

Keep this shorter than [`poll-freq-secs`](#poll-freq-secs) if you can, so a
hung read cannot delay the next poll.  The shipped default of 28 seconds is
deliberately close to the 30 second default poll frequency: an AirGradient
sensor under load can take a long time to answer, and a late reading is
worth more than no reading.

## long-read-secs

**Default:** `10`
**Install option:** `--long-read <secs>`

Reads that take longer than this are logged as
`Event took longer than expected`.  Nothing is skipped or rejected — this is
purely a warning that the sensor is getting slow.  It is the earliest sign
that too many clients are talking to the sensor directly.

## gc-interval-secs

**Default:** `3600`
**Install option:** `--gc-interval <secs>`

How often to run a full cyclic garbage collection pass, in seconds.  `0`
disables it.

The pass runs only on a poll that is not also writing an archive record, so
its pause never stacks on top of archive work.  The number of objects
collected is logged (`Garbage collected N objects in T seconds.`), which is
what makes a slow memory leak visible in the weekly logwatch report.

## database-file

**Default:** `<target-dir>/archive/airgradientproxy.sdb`
**Install option:** `--database-file <path>`

Path of the sqlite database holding the current reading, the two-minute
average and the archive history.

The directory is created if it does not exist, but it must be somewhere the
`airgradientproxy` user can write.  A database on a filesystem that fills up
or unmounts is the usual cause of `Could not save ... to database` in the
log.

Do not discard this file lightly: the archive records in it are the history
that lets a client fill in a period it was not running for, and nothing else
holds them.  An upgrade never touches it, and neither does `--uninstall`.

## service-name

**Default:** `airgradient-proxy`
**Install option:** `--service-name <name>`

The syslog program name the daemon logs under.

{: .important }
The shipped rsyslog, logrotate and logwatch configuration is keyed to
`airgradient-proxy`.  Changing this means the log no longer lands in
`/var/log/airgradient-proxy.log`, is no longer rotated, and no longer
appears in the logwatch report, until you edit those files to match.

## debug

**Default:** `0`
**Install option:** `--debug <0|1>`

Set to `1` to log debug messages — every poll's elapsed time, the seconds
until the next poll, and similar.  Useful while diagnosing a sensor that
answers slowly; noisy enough that you will want it back at `0` afterwards.

## log-to-stdout

**Default:** `0`
**Install option:** `--log-to-stdout <0|1>`

Set to `1` to log to stdout instead of syslog.  This is for running the
daemon by hand in a terminal; under systemd, leave it at `0`.
