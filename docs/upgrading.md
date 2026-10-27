---
title: Upgrading
layout: default
nav_order: 3
---

# Upgrading

[airgradient-proxy manual](https://chaunceygardiner.github.io/airgradient-proxy/) · [airgradient-proxy on GitHub](https://github.com/chaunceygardiner/airgradient-proxy) · [Report an issue](https://github.com/chaunceygardiner/airgradient-proxy/issues)

---

Upgrading is re-running the install script over an existing installation.
Get the new source as in [Installation](installation.md#1-get-the-source),
then:

```sh
cd <airgradient-proxy-src-dir>
sudo ./install -y
```

Nothing is prompted for on an upgrade — an existing
`<target-dir>/airgradientproxy.conf` is what tells the script this is one.
Your settings come from that file; you only need to pass an option if you
want to change it.

## What an upgrade touches

**`airgradientproxy.conf` is migrated, never regenerated.**  Existing values
are kept, options given on the command line are applied, options new to this
version are added with their defaults, and options this version no longer
knows about are dropped.  The previous file is saved as
`airgradientproxy.conf.bak`.

{: .note }
Hand-written comments in the deployed conf do not survive migration.  Values
do — a per-machine
[`poll-freq-offset`](configuration.md#poll-freq-offset) is not lost.

**Other conf files are never overwritten.**  The rsyslog, logrotate and the
two logwatch conf files are installed only when absent; once installed they
are yours to customize.  If the version shipped in a release differs from
what is installed, your file is left alone and the shipped version is
written alongside as `<file>.dpkg-new` for hand merging.  The `.dpkg-new`
copy is removed automatically once your installed file matches what ships.

**Program files and the logwatch classifier are refreshed every time.**  The
classifier matches the daemon's log messages verbatim, so it ships with the
daemon and is replaced along with it.

**A symlinked destination is left in place.**  Any file the installer would
write that is a symlink is skipped, so files symlinked to a source checkout
keep working.  This includes `airgradientproxy.conf`: a symlinked conf is
not migrated.

**The database is kept.**  Your archive history survives upgrades, and
survives an uninstall too.  Upgrading to 2.2 from an earlier release adds
one column, `rco2Unfiltered`, to the database, once, the first time the
upgraded daemon starts, and logs:

```
Added column rco2Unfiltered to the Reading table.
```

At the same moment the current and two-minute records are cleared, since
their CO2 is unfiltered, so until the first poll lands (up to one
[`poll-freq-secs`](configuration.md#poll-freq-secs)) `/measures/current`
and `/fetch-two-minute-record` answer `{}`.  Archive records written before
the upgrade are kept, with their CO2 as it was and no `rco2Unfiltered`.  If the column cannot be added, the daemon exits and
systemd retries it every 10 seconds, each failure in
`journalctl -u airgradient-proxy`.  Going back to an earlier release is
safe: it ignores the extra column.

## What an upgrade restarts

As little as possible.  The daemon is not stopped up front.  At the end of
the install:

* the service is **started** if it was not running;
* it is **restarted** only if the program files, `airgradientproxy.conf` or
  the systemd unit actually changed;
* otherwise it is **left alone** — an install that changed nothing prints
  `Nothing changed; leaving the running daemon alone`.
* rsyslog is restarted only when its conf file was newly installed.

## Upgrading from a pre-2.0 installation

Versions before 2.0 ran the daemon from a SysV init script under a `nohup`
wrapper.  The install script migrates such an installation to the systemd
unit automatically: it stops the old daemon, removes the init script and the
wrapper, and installs, enables and starts the `airgradient-proxy` service.
The configuration and the database are carried forward unchanged.

Nothing needs to be done by hand.  The database is carried forward, with
the one-time column add described above.
