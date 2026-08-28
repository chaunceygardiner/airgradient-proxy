---
title: Troubleshooting
layout: default
nav_order: 7
---

# Troubleshooting

[airgradient-proxy manual](https://chaunceygardiner.github.io/airgradient-proxy/) · [airgradient-proxy on GitHub](https://github.com/chaunceygardiner/airgradient-proxy) · [Report an issue](https://github.com/chaunceygardiner/airgradient-proxy/issues)

---

Almost everything shows up in `/var/log/airgradient-proxy.log`, so start
there:

```sh
tail -50 /var/log/airgradient-proxy.log
```

{: .note }
The messages below are what the **daemon writes**.  The logwatch report
groups them under headings of its own (`JSON decoding error`,
`Read timeouts (skipped reading)`, and so on) — those headings do not appear
in the log, so grep for the text on this page instead.

## Nothing is in the log at all

Check the service first:

```sh
sudo systemctl status airgradient-proxy
sudo journalctl -u airgradient-proxy -n 50
```

If the service is running but `/var/log/airgradient-proxy.log` is empty or
missing, the log is going to the journal instead, which means rsyslog is not
installed, is not running, or its conf was not installed:

```sh
systemctl is-active rsyslog
ls -l /etc/rsyslog.d/airgradient-proxy.conf
```

The conf is installed only if it was absent, so if you have an
`airgradient-proxy.conf.dpkg-new` sitting beside it, your own file is in
use.  Also check that
[`service-name`](configuration.md#service-name) in `airgradientproxy.conf`
still matches the program name that conf routes on — changing it silently
takes the log out of the file.

## The daemon will not start

Run it in the foreground and read the error:

```sh
sudo -u airgradientproxy /home/airgradientproxy/bin/airgradientproxyd \
    /home/airgradientproxy/airgradientproxy.conf
```

| What it says | Cause |
| --- | --- |
| `archive-interval-secs must be a multiple of poll-frequency-secs` | Fix one of the two; see [`archive-interval-secs`](configuration.md#archive-interval-secs) |
| A traceback ending in `sqlite3.OperationalError: unable to open database file` | The `airgradientproxy` user cannot write the directory holding [`database-file`](configuration.md#database-file).  A directory that does not exist is not the problem — the daemon creates it — a directory it may not write in is |

## Readings are being saved but no client can reach the proxy

The log looks completely healthy — `Saved current reading` every poll,
archive records appearing — and yet every request to the proxy is refused.

The REST server runs on its own thread, and that thread failing does not
stop the polling.  The usual cause is something else already holding
[`server-port`](configuration.md#server-port), most often a second proxy on
the same machine that was given the same port.

This failure does **not** appear in `/var/log/airgradient-proxy.log`, and
the logwatch report will not show it either: the thread dies with a Python
traceback on stderr rather than a logged message.  Look in the journal:

```sh
sudo journalctl -u airgradient-proxy | grep -A3 'Exception in thread'
```

```
Exception in thread airgradientproxy_daemon_server:
...
OSError: [Errno 98] Address already in use
```

Give the two proxies different `server-port` values and restart.  Check what
holds the port with `sudo ss -lntp | grep <port>`.

## Readings are being skipped

Every skipped reading is one line beginning `Skipping reading because of:`,
followed by the exception.  The daemon never exits over one; it resets its
HTTP session and tries again at the next poll.

| In the log | What it means |
| --- | --- |
| `ConnectTimeout ... Caused by ConnectTimeoutError` | The sensor did not accept the connection in time |
| `ReadTimeout(ReadTimeoutError` | It accepted the connection and then did not answer |
| `ConnectionError ... Connection refused` | Something answered at that address but nothing is serving |
| `ConnectionError ... Connection aborted` | The sensor dropped the connection mid-request |
| `Connection broken: ... IncompleteRead` | The response ended early |
| `ChunkedEncodingError ... InvalidChunkLength` | The response was malformed — a sensor under load |
| `Temporary failure in name resolution` | DNS is down, or [`hostname`](configuration.md#hostname) is wrong |
| `Failed to establish a new connection: ... Name or service not known` | [`hostname`](configuration.md#hostname) does not resolve at all |
| `Failed to establish a new connection: ... No route to host` | The sensor is off, or off the network |
| `[Errno 101] Network is unreachable` | This machine's own networking is down |

An occasional timeout is normal; a steady stream of them means the sensor is
overloaded.  If anything besides the proxy is polling it, point that at the
proxy too — absorbing exactly this is what the proxy is for.  A second proxy
polling the same sensor should have a non-zero
[`poll-freq-offset`](configuration.md#poll-freq-offset), or the two collide
on every poll.

### The sensor sent something that is not JSON

```
parse_response: '{"pm02": 5, ' raised exception JSONDecodeError(...)
Skipping reading because of: JSONDecodeError(...)
```

One malformed response produces **both** lines: the first shows the payload
the sensor actually sent, which is what you want to look at.  Isolated
occurrences are the sensor truncating a response under load.  Continuous
ones mean whatever is at [`hostname`](configuration.md#hostname) is not an
AirGradient sensor — check that the address and
[`port`](configuration.md#port) are right by asking it yourself:

```sh
curl 'http://<sensor>/measures/current'
```

### The reading arrived but was rejected

```
Reading found insane due to:  <reason>: <the whole reading>
```

The reading was received and then failed a sanity check, so it was not
stored.  The reason is one of:

* **`measurementTime more than 20s off: <n>`** — you will almost certainly
  never see this.  `measurementTime` is stamped by the proxy when it parses
  the response, and this check compares that stamp against the same clock a
  fraction of a millisecond later, so only a clock step landing inside that
  window can trip it — and then for one reading.
* **`<field> not instance of str`** (or `int`) — the sensor sent one of the
  uncoerced fields with the wrong type.  Only `serialno`, `boot`,
  `bootCount`, `ledMode`, `firmware` and `model` can produce this: the
  numeric fields are converted with `float()` as the response is parsed, so a
  wrong type there raises during parsing instead and is skipped with a
  `TypeError` or `ValueError` (see above).  A JSON boolean where `boot` or
  `bootCount` belongs is rejected this way, which is a known way for a sensor
  firmware bug to show up.
* **`measurementTime not instance of datetime`** — internal; report it.

## No records are being written

```
Skipping two_minute record because there have been zero readings this two minute period.
Skipping archive record because there have been zero readings this archive period.
```

These mean no reading survived in that whole window — the proxy has nothing
to average.  They are a consequence, not a cause: look for the
`Skipping reading because of:` lines above them and fix that.

## Records cannot be saved

```
Could not save current reading to database: <db>: <error>
Could not save two minute reading to database: <db>: <error>
Could not save archive reading to database: <db>: <error>
```

The reading was fine; writing it failed.  Usual causes are a full
filesystem, a database file the `airgradientproxy` user can no longer write
(often after the file was restored or copied as root), or an unmounted
volume.  These are logged at critical level and the daemon keeps running.

## Readings are late, or the log says reads are slow

```
Event took longer than expected: 12.480000 seconds.
```

The read succeeded but took longer than
[`long-read-secs`](configuration.md#long-read-secs).  This is the early
warning for a sensor being asked for too much: nothing is wrong yet, but
timeouts follow if it gets worse.  Find the other clients talking to the
sensor and point them at the proxy.

## A client gets an error from the proxy

Every REST error is HTTP 404 with the reason in the body, and each one is
logged as `request_error:`.  The message says which argument was wrong; see
[Errors](rest-api.md#errors) for the full list.

The one that catches everybody: **arguments are comma-separated, not
ampersand-separated**.  `?since_ts=100&max_ts=200` is not valid here and
comes back as `The since_ts argument must be an integer`.

## Archive history is shorter than expected

```sh
curl 'http://localhost:8080/get-earliest-timestamp'
```

This is the oldest archive record the proxy holds.  If it is more recent
than you expect, the database was replaced or the proxy was reinstalled to a
new [`database-file`](configuration.md#database-file) — an upgrade never
discards it, and neither does `--uninstall`.  An empty response, `{}`, means
there are no archive records at all yet, which is normal for the first few
minutes after a fresh install.

Two proxies polling the same sensor keep **separate** databases, so one may
reach further back than the other.

## A gap in this proxy's archive history

A period the proxy was down for is simply not in its database, and nothing
can put it back — the sensor kept no copy.  Check what the proxy does hold:

```sh
curl 'http://localhost:8080/fetch-archive-records?since_ts=<start>,max_ts=<end>'
```

An empty array means no record for that span.  A client backfilling WeeWX's
catchup records from this proxy will leave that period empty too, which is
the honest answer.

Two proxies polling the same sensor are the mitigation: they keep separate
databases, so a period one missed the other usually has.  A client that
backfills should be configured with both.

If gaps are frequent, the cause is upstream — see
[Readings are being skipped](#readings-are-being-skipped).

## The logwatch section is missing

logwatch must be installed **before** airgradient-proxy: the install script
lays down the logwatch configuration and the classifier only if
`/etc/logwatch` already exists.  Install logwatch, then re-run
`sudo ./install -y`.

## After an upgrade, a setting went back to its default

Look for `airgradientproxy.conf.bak` next to the conf.  Migration keeps
existing values, so a value that reverted was probably written in a way this
version no longer recognizes and was dropped as deprecated.  The install
prints `Removing deprecated option: <key>.` when that happens.

{: .note }
If `airgradientproxy.conf` is a symlink, it is deliberately **not**
migrated — the installer leaves the symlink alone and prints
`Leaving symlink <conf> in place; settings are not migrated.`  Edit the file
it points at.

## Still stuck

Turn on [`debug`](configuration.md#debug) logging, restart, and watch a few
polls:

```
debug = 1
```

Then
[open an issue](https://github.com/chaunceygardiner/airgradient-proxy/issues)
with the relevant part of the log.
