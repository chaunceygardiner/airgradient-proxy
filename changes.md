# airgradient-proxy change history

## 2.2 (pending)

- CO2 (`rco2`) is served as the median of the last three readings.  The
  sensor's CO2 sometimes jumps, up or down, for exactly one reading, and
  that one reading became the day's high or low in every station recording
  the proxy, and stayed in one archive record for good.  A median of three
  never passes a lone outlier through, so the spike reaches no `rco2` in
  any record the proxy writes: current, two-minute or archive.  The cost
  is that CO2 is served about one poll late, and that a real plume lasting
  a single reading is removed too.  A reading stops counting toward the
  median once it is older than 120 seconds or three polls, whichever is
  longer, plus half a poll, so two missed polls in a row (at 30 second
  polling) keep the median going.
- New field `rco2Unfiltered`, in every record and response: CO2 as the
  sensor reported it, before the median.  It is the way to see what the
  median removed, and weewx-airgradient 4.1 and later use its presence to
  tell a proxy that filters CO2 from one that does not.
- After the daemon starts, `rco2` is withheld for the first two readings,
  until there are three to take a median of; `rco2Unfiltered` and every
  other field are served from the first reading.  The first poll after a
  start now waits for its scheduled slot instead of running at once, so
  those three readings are a full poll apart.
- With a `poll-freq-offset`, the next poll is the next offset slot.  The
  offset used to be added after rounding down to a multiple of
  `poll-freq-secs`, so a start, or a read that ran past the multiple,
  skipped the offset slot just ahead and waited up to a poll longer.
- Each field of a two-minute or archive record is now the average of the
  readings that carry that field, and is missing only when none does.  It
  used to be missing if any one reading lacked it, so one invalid CO2 read
  (the firmware omits `rco2` then) emptied a whole archive record's CO2.
- A number the sensor sends as `NaN` or infinity counts as absent, in
  every field, instead of being stored, averaged and served as a bare
  `NaN`.
- An archive period's samples are discarded when its record cannot be
  saved, instead of being folded into the next period's average.  The
  record is lost either way; carrying its readings forward quietly
  averaged two periods into one on top of the record already lost.
- The database gains one column, `rco2Unfiltered`.  It is added once, the
  first time the upgraded daemon (or `--dump`) opens a database made by an
  earlier release, and logged as `Added column rco2Unfiltered to the
  Reading table.`, which the logwatch report counts.  The current and
  two-minute records are cleared at the same time, because their CO2 is
  unfiltered, so the proxy answers `{}` for them until its first poll.
  Archive records written before the upgrade are kept, without
  `rco2Unfiltered`.  An earlier release can still read the database.

## 2.1 (08/27/2026)

- A [manual](https://chaunceygardiner.github.io/airgradient-proxy/)
  covering installation, upgrading, every configuration setting, the REST
  API, running the service and troubleshooting.  `systemctl status` links
  to it, and the README links to it, to the download and to the issue
  tracker.
- The daemon's built-in fallbacks for a conf missing `server-port` or
  `timeout-secs` were 8000 and 25 while the installer, the fleet and the
  documentation all said 8080 and 28.  They are now 8080 and 28.
- The logwatch report counts every skipped reading.  A payload that could
  not be turned into a reading used to land in "unmatched lines" unless it
  was a JSON decoding failure; it is now counted as "Other error (skipped
  reading)", with the line itemized beneath it.

## 2.0 (07/16/2026)

- The daemon is restructured to match purple-proxy 4.0: `airgradientproxyd`
  owns option and config parsing, `--dump`, and the version, and
  `monitor.py` is split into `monitor/model.py`, `monitor/database.py` and
  `monitor/service.py`.  Every log message is unchanged.
- Fixed: `since_ts=0` tripped an assertion and the client got a dropped
  connection instead of the archive records.
- Fixed: a sensor read that blocked across an archive boundary skipped that
  period's record and folded its samples into the next one.  The record is
  written by the first poll at or past the boundary, correctly timestamped.
- Fixed: all SQL is parameterized.
- Fixed: `/measures/current` with arguments was silently accepted; it is
  now rejected as intended.
- Fixed: sanity checks reject JSON booleans masquerading as integers, and
  error responses no longer carry stray header bytes.
- A systemd unit replaces the init script and its nohup wrapper.  The
  daemon is supervised, runs as the dedicated `airgradientproxy` system
  user rather than root, and crash tracebacks land in the journal.
- The installer takes every setting as a named flag and never silently
  overwrites configuration: `airgradientproxy.conf` is migrated, and every
  other conf file is installed only when absent, with a changed shipped
  version written alongside as `<file>.dpkg-new`.  It also creates the
  `airgradientproxy` user and migrates a SysV installation.
- New option `gc-interval-secs` (default 3600), a periodic garbage
  collection pass on a non-archive poll.
- **ACTION REQUIRED, breaking changes:** the positional install form is
  gone and exits with the equivalent option-based command;
  `airgradientproxyd --test` is gone, replaced by
  `python3 tests/test-live.py`; the daemon runs as the `airgradientproxy`
  user, which takes ownership of the target directory including the
  database.

## 1.0 (10/27/2025)

- Initial release, cloned from purple-proxy: poll an AirGradient sensor,
  sanity check and average readings, keep a sqlite archive, and serve them
  over a small REST API.
- Later fixes on 1.0: AirGradient Open Air units, which omit some fields,
  are supported; the installer sets the logrotate file's mode so Debian
  trixie's logrotate accepts it; logwatch classifier fixes.
