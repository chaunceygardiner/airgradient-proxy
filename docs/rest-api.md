---
title: REST API
layout: default
nav_order: 5
---

# REST API

[airgradient-proxy manual](https://chaunceygardiner.github.io/airgradient-proxy/) · [airgradient-proxy on GitHub](https://github.com/chaunceygardiner/airgradient-proxy) · [Report an issue](https://github.com/chaunceygardiner/airgradient-proxy/issues)

---

The proxy answers HTTP GET requests on
[`server-port`](configuration.md#server-port), 8080 by default, on both IPv6
and IPv4.  Every successful response is JSON with
`Content-Type: application/json`.

All the examples below were run against a live proxy and show its actual
output.

{: .important }
Arguments are separated by **commas**, not ampersands.  This is not a normal
query string — the proxy splits the part after `?` on `,`.  A request
written with `&` fails with a message about the argument not being an
integer, because everything after the first `&` was swallowed into the first
argument's value.

## Endpoints at a glance

| Request | Answers with |
| --- | --- |
| [`/measures/current`](#measurescurrent) | The most recent reading |
| [`/fetch-current-record`](#fetch-current-record) | The same thing |
| [`/fetch-two-minute-record`](#fetch-two-minute-record) | Average of the last two minutes |
| [`/fetch-archive-records`](#fetch-archive-records) | Archive history, a period at a time |
| [`/get-earliest-timestamp`](#get-earliest-timestamp) | Oldest archive record's timestamp |
| [`/get-version`](#get-version) | Version of this command set |

## `/measures/current`

The most recent reading the proxy took, as the AirGradient device itself
would serve it, with `measurementTime` and `rco2Unfiltered` fields added and
`rco2` the CO2 median (see [CO2](#co2-rco2-and-rco2unfiltered)).  This is
what lets a client point at the proxy instead of the sensor without changing
anything else.

```sh
curl 'http://localhost:8080/measures/current'
```

```json
{"pm01": 0.0, "pm02": 0.0, "pm10": 0.0, "pm01Standard": 0.0, "pm02Standard": 0.0,
 "pm10Standard": 0.0, "pm003Count": 89.33, "pm005Count": 75.67, "pm01Count": 16.0,
 "pm02Count": 4.33, "pm50Count": 0.0, "pm10Count": 0.0, "pm02Compensated": 2.4,
 "atmp": 19.6, "atmpCompensated": 19.6, "rhum": 59.88, "rhumCompensated": 59.88,
 "rco2": 657.0, "rco2Unfiltered": 657.0, "tvocIndex": 103.0, "tvocRaw": 33894.25,
 "noxIndex": 1.0, "noxRaw": 22988.42, "boot": 96775, "bootCount": 96775,
 "wifi": -63.0, "ledMode": "co2", "serialno": "d83bda1b9464", "firmware": "3.7.0",
 "model": "I-9PSL", "measurementTime": "2026-10-27T16:28:30.131692Z"}
```

This request takes no arguments; giving it any is an error.

{: .note }
`measurementTime` is when the **proxy** read the sensor, in UTC, not a clock
value reported by the device.  A field the sensor did not send, or sent
as `NaN` or infinity, is omitted from the response entirely rather than
sent as null — an Open Air unit serves fewer fields than the indoor unit
shown here.

### CO2: `rco2` and `rco2Unfiltered`

The sensor's CO2 sometimes jumps, up or down, for exactly one reading.
Served as is, that one reading becomes the day's high or low in every
station that records it, and stays in one archive record for good.  So the
proxy serves `rco2` as the **median of the last three readings** that
carried CO2: a lone outlier is always outvoted, and never reaches `rco2` in
any record the proxy writes.

* **`rco2`** is that median.  On a steady rise or fall it is the middle of
  the three readings, so CO2 is served about one poll late.  A reading
  stops counting once it is older than 120 seconds or three
  [`poll-freq-secs`](configuration.md#poll-freq-secs), whichever is
  longer, plus half a poll: 135 seconds at the default 30.
* **`rco2Unfiltered`** is CO2 as the sensor reported it, before the
  median: the way to see what the median removed.
* After the daemon starts there is no median yet, so `rco2` is **absent
  for the first two readings** and returns with the third, 60 to 90
  seconds after the start.  (The first poll waits for its scheduled slot,
  so the three readings are a full poll apart.)  That holds for the
  current record and for the two-minute record alike, since the
  two-minute record has no CO2 median to average until then.
  `rco2Unfiltered` and every other field are there from the first
  reading.  `rco2` is likewise absent for two readings after an outage
  longer than the age limit.
* A CO2 value the sensor did not send, or sent as something other than a
  finite number, leaves both fields out.

The cost is that a real plume that lasts only one reading is removed too.
A change that lasts longer gets through, one poll late.

## `/fetch-current-record`

Identical to [`/measures/current`](#measurescurrent).  Use whichever name
reads better in your client; `/measures/current` exists to mimic the device,
`/fetch-current-record` to sit alongside the other `fetch-` requests.

## `/fetch-two-minute-record`

The average of the sane readings taken in the last two minutes, in the
same shape as a current record.  This is the one to graph or report: a
single spot reading of particulate counts is noisy, and this smooths it
without hiding a real change.

```sh
curl 'http://localhost:8080/fetch-two-minute-record'
```

```json
{"pm01": 0.0, "pm02": 0.0, "pm10": 0.0, "pm01Standard": 0.0, "pm02Standard": 0.0,
 "pm10Standard": 0.0, "pm003Count": 85.75, "pm005Count": 73.2525, "pm01Count": 15.0,
 "pm02Count": 3.0, "pm50Count": 0.0825, "pm10Count": 0.0, "pm02Compensated": 2.32,
 "atmp": 19.5875, "atmpCompensated": 19.5875, "rhum": 59.9275, "rhumCompensated": 59.9275,
 "rco2": 657.335, "rco2Unfiltered": 657.4175, "tvocIndex": 103.0, "tvocRaw": 33902.085,
 "noxIndex": 1.0, "noxRaw": 22987.0, "boot": 96775, "bootCount": 96775,
 "wifi": -63.0, "ledMode": "co2", "serialno": "d83bda1b9464", "firmware": "3.7.0",
 "model": "I-9PSL", "measurementTime": "2026-10-27T16:28:30.131692Z"}
```

The record is rewritten on **every** poll, not once every two minutes, so it
is never more than one [`poll-freq-secs`](configuration.md#poll-freq-secs)
stale.  `measurementTime` is the time of the newest reading in the window —
that is, the end of the span the average covers, not its middle.

Each field is averaged over the readings in the window that carry it, and
is missing only when none does.  So `rco2` here is the average of the CO2
medians in the window, and trails the sensor by about one poll, while
`rco2Unfiltered` is the average of the values as the sensor reported them.
Archive records are built the same way.

Not every field is averaged.  `serialno`, `ledMode`, `firmware` and `model`
are strings, and `boot` and `bootCount` are counters; all six are carried
through from the newest reading in the window as they stand.

## `/fetch-archive-records`

The archive history: one averaged record per
[`archive-interval-secs`](configuration.md#archive-interval-secs).  This is
the request a client uses to fill in a period it was not running for.

| Argument | Required | Meaning |
| --- | --- | --- |
| `since_ts` | yes | Return records with timestamp **greater than** this |
| `max_ts` | no | Return records with timestamp **less than or equal to** this |
| `limit` | no | Return at most this many records |

Timestamps are seconds since the epoch.  `since_ts=0` fetches everything.

```sh
curl 'http://localhost:8080/fetch-archive-records?since_ts=1787835096,max_ts=1787835996'
```

```json
[  {"pm01": 0.0, "pm02": 0.033, "pm10": 0.033, ..., "measurementTime": "2026-08-27T12:55:00.000000Z"},
   {"pm01": 0.0, "pm02": 0.017, "pm10": 0.017, ..., "measurementTime": "2026-08-27T13:00:00.000000Z"} ]
```

The response is a JSON array, oldest record first.  `limit` therefore
returns the **oldest** matching records, not the newest; to walk history
forward in chunks, pass the last timestamp you saw as the next `since_ts`.

{: .note }
`since_ts` is exclusive and `max_ts` is inclusive, which is exactly one
WeeWX archive period: a record stamped at the end of the period is included,
and the record ending the previous period is not.

Arguments may be combined, comma-separated:

```sh
curl 'http://localhost:8080/fetch-archive-records?since_ts=0,max_ts=1787835996,limit=100'
```

## `/get-earliest-timestamp`

The timestamp of the oldest archive record in the database — how far back
this proxy can answer for.  A client backfilling history asks this first, so
it does not request periods that predate the proxy.

```sh
curl 'http://localhost:8080/get-earliest-timestamp'
```

```json
{"timestamp": 1761629700.0}
```

Note that it is a float.  If there are no archive records yet — a proxy
installed less than one archive interval ago — the response is an empty
object, `{}`, with no `timestamp` member at all.

## `/get-version`

The version of this REST command set.

```sh
curl 'http://localhost:8080/get-version'
```

```json
{"version": "1.0"}
```

{: .important }
This is **not** the version of the program.  It changes only when the set of
requests above changes, so that a client can tell what it may ask for.  For
the program version, see the `Version` line the daemon logs at startup, or
run `airgradientproxyd --dump` (see
[Running the proxy](running.md#dumping-the-database)).

## Errors

Every error is HTTP **404**, with the reason in the body of the standard
error page:

```sh
curl 'http://localhost:8080/fetch-archive-records'
```

```
<p>Error code: 404</p>
<p>Message: fetch-archive-records requires since_ts argument.</p>
```

The messages you are most likely to meet:

| Message | Cause |
| --- | --- |
| `A command must be specified.` | The request was just `/` |
| `Unknown command: /whatever.` | No such request |
| `If measures/current cmd is specified, args must be empty.` | Arguments given to `/measures/current` |
| `fetch-archive-records requires since_ts argument` | `since_ts` omitted |
| `The since_ts argument must be an integer, found: '...'` | Unparsable `since_ts` — including the case where `&` was used instead of `,` |
| `The max_ts argument must be an integer, found: '...'` | Unparsable `max_ts` |
| `The limit argument must be an integer, found: '...'` | Unparsable `limit` |

Every one of these is also logged by the daemon as `request_error:`, and
counted in the logwatch report as `Request Errors`.
