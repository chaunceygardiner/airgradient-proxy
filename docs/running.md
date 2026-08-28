---
title: Running the proxy
layout: default
nav_order: 6
---

# Running the proxy

[airgradient-proxy manual](https://chaunceygardiner.github.io/airgradient-proxy/) · [airgradient-proxy on GitHub](https://github.com/chaunceygardiner/airgradient-proxy) · [Report an issue](https://github.com/chaunceygardiner/airgradient-proxy/issues)

---

## The service

The daemon runs under systemd as the `airgradientproxy` system user, with
`Restart=on-failure`.

```sh
sudo systemctl status airgradient-proxy
sudo systemctl restart airgradient-proxy
sudo systemctl stop airgradient-proxy
sudo systemctl start airgradient-proxy
sudo journalctl -u airgradient-proxy      # service-level messages
```

The service is enabled at install time, so it comes back after a reboot.

## The log

rsyslog routes everything the daemon logs to its own file:

```sh
tail -f /var/log/airgradient-proxy.log
```

A healthy log is repetitive.  At the default settings you see two lines
every 30 seconds and a third every five minutes:

```
Saved current reading 2026-08-27 00:15:00 PDT (1787469300) in 0 seconds.
Saved two minute reading 2026-08-27 00:15:00 PDT (1787469300) in 0 seconds (5 samples).
Added record 2026-08-27 00:15:00 PDT (1787469300) to archive (10 samples).
```

The sample counts are worth a glance.  `(5 samples)` on the two-minute
record and `(10 samples)` on a five-minute archive record are what a 30
second poll frequency should produce; consistently fewer means readings are
being skipped, and [Troubleshooting](troubleshooting.md) has the reasons.

Once an hour, if [`gc-interval-secs`](configuration.md#gc-interval-secs) is
not `0`:

```
Garbage collected 143 objects in 0.031 seconds.
```

Each REST request the proxy answers is logged too, by name —
`fetch-current-record`, `fetch-two-minute-record`, `fetch-archive-records`,
`get-earliest-timestamp`, `get-version`.  That is how you tell which clients
are actually using the proxy.

At startup the daemon logs its whole configuration, which is the quickest
way to confirm what it is really running with (`debug`, `log_to_stdout`,
`conf_file` and `pidfile` are logged too, before and after these):

```
Version        : 2.1
host:port      : airgradient-indoor:80
server_port    : 8080
timeout_secs   : 28
long_read_secs : 10
pollfreq_secs  : 30
pollfreq_offset: 0
arcint_secs    : 300
gc_interval_secs: 3600
db_file        : /home/airgradientproxy/archive/airgradientproxy.sdb
service_name   : airgradient-proxy
```

### Rotation

The log is rotated weekly, four rotations kept, with `copytruncate` — so the
daemon does not need to be signalled or restarted when it happens.

## The logwatch report

If logwatch is installed, an `airgradient-proxy` section appears in the
regular logwatch report.  It counts the routine work and categorizes the
errors:

```
counts:
  Archive Records Added                           2016
  Garbage Collections                              168
  Saved 2m Readings                              20160
  Saved Curr. Readings                           20160
  Startups                                           1
  fetch-current-record                           20160

errors:
  JSON decoding error (skipped reading)              1
  Read timeouts (skipped reading)                    3
```

Below the counts, the errors that carry detail are listed in full, so you
can see the actual log lines behind a number.

{: .note }
The classifier matches the daemon's log messages verbatim, so it ships with
the daemon and is refreshed on every install.  Its category names are report
headings, not text from the log — do not grep the log for
`JSON decoding error`; grep it for what the daemon actually writes, which
[Troubleshooting](troubleshooting.md) lists.

## Dumping the database

To see what is actually stored, run the daemon with `--dump`.  It prints the
current reading and every archive record, then exits without starting the
service:

```sh
sudo -u airgradientproxy /home/airgradientproxy/bin/airgradientproxyd --dump \
    /home/airgradientproxy/airgradientproxy.conf
```

```
----------------------------
* Dumping current reading  *
----------------------------
Reading(measurementTime=datetime.datetime(2026, 8, 27, 13, 8, 0, 75227, tzinfo=tzfile('/usr/share/zoneinfo/UTC')), serialno='d83bda1b9464', wifi=-44.0, pm01=0.0, pm02=0.0, ...)
```

{: .note }
`--dump` logs the same startup block as a real start before it dumps, so it
goes into `/var/log/airgradient-proxy.log` like any other start and adds one
to the logwatch report's "Startups" count.  A surprise startup in a week you
did not restart anything is usually a `--dump` you ran.

{: .important }
The archive dump is one line per record, and a proxy that has been running
for a year holds tens of thousands of them.  Pipe it through `head`, or
fetch the range you want with
[`/fetch-archive-records`](rest-api.md#fetch-archive-records) instead.

The database is ordinary sqlite, so `sqlite3` works on it too — but query it
read-only while the daemon is running.

## Where everything lives

| Path | What it is |
| --- | --- |
| `/home/airgradientproxy/bin/airgradientproxyd` | The daemon |
| `/home/airgradientproxy/airgradientproxy.conf` | [Configuration](configuration.md) |
| `/home/airgradientproxy/airgradientproxy.conf.bak` | The conf as it was before the last upgrade |
| `/home/airgradientproxy/archive/airgradientproxy.sdb` | The sqlite database |
| `/etc/systemd/system/airgradient-proxy.service` | The systemd unit |
| `/etc/rsyslog.d/airgradient-proxy.conf` | Routes the log to its own file |
| `/etc/logrotate.d/airgradient-proxy` | Weekly rotation |
| `/etc/logwatch/conf/logfiles/airgradient-proxy.conf` | Tells logwatch about the log |
| `/etc/logwatch/conf/services/airgradient-proxy.conf` | Tells logwatch about the service |
| `/etc/logwatch/scripts/services/airgradient-proxy` | The log classifier |
| `/var/log/airgradient-proxy.log` | The log |

The paths under `/home/airgradientproxy` follow the install target, which
`--target` can change.
