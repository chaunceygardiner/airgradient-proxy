---
title: Installation
layout: default
nav_order: 2
---

# Installation

[airgradient-proxy manual](https://chaunceygardiner.github.io/airgradient-proxy/) · [airgradient-proxy on GitHub](https://github.com/chaunceygardiner/airgradient-proxy) · [Report an issue](https://github.com/chaunceygardiner/airgradient-proxy/issues)

---

**Requirements:** Debian or Raspberry Pi OS, systemd, and Python 3 with
`python3-configobj`, `python3-dateutil` and `python3-requests`.  There is no
virtual environment and nothing is installed with pip.  rsyslog is strongly
recommended — it is what routes the daemon's log to
`/var/log/airgradient-proxy.log`; without it the log lives only in the
systemd journal.  logwatch is optional; if it is present, a log classifier
is installed with the daemon.

{: .note }
These instructions and the `install` script have been tested on Debian and
Raspberry Pi OS.  On another platform the script still serves as a precise
specification of the steps required.

## 1. Get the source

Clone the repository:

```sh
git clone https://github.com/chaunceygardiner/airgradient-proxy
```

or download
[airgradient-proxy.zip](https://github.com/chaunceygardiner/airgradient-proxy/releases/latest/download/airgradient-proxy.zip)
from the
[releases page](https://github.com/chaunceygardiner/airgradient-proxy/releases)
and unzip it.  Either way, the resulting directory (`airgradient-proxy` when
cloned, `airgradient-proxy-main` when unzipped) is the source directory
below.

## 2. Install the packages

```sh
sudo apt install rsyslog python3-configobj python3-dateutil python3-requests
```

## 3. Run the install script

```sh
cd <airgradient-proxy-src-dir>
sudo ./install --sensor <sensor-dns-name>
```

Every setting is a named command line option; `./install -h` lists them all.
On a fresh install the script prompts for anything not given on the command
line, showing the default — press Enter to accept it.  `-y` accepts every
default silently.  The one setting with no default is `--sensor`, the DNS
name or IP address of the AirGradient sensor on your network.

```sh
# Fully interactive:
sudo ./install

# Scripted; defaults for everything not given:
sudo ./install --sensor airgradient-indoor --poll-freq-offset 15 -y
```

{: .important }
The pre-2.0 positional form of the command
(`./install <src> <target> <archive-interval> <sensor>`) is rejected.  Given
exactly those four arguments, the script prints the equivalent flags and exits
without changing anything; any other number of arguments is reported as an
unknown option instead.

On a fresh install the script:

* creates an `airgradientproxy` system user for the daemon to run as;
* copies the program to the target directory (default
  `/home/airgradientproxy`) and chowns it to that user;
* generates `<target-dir>/airgradientproxy.conf` from the settings chosen;
* installs the rsyslog, logrotate and logwatch configuration;
* installs, enables and starts the `airgradient-proxy` systemd service.

See [Configuration](configuration.md) for what every setting means, and
[Upgrading](upgrading.md) for what re-running the script does to an existing
installation.

## 4. Check that it is running

```sh
sudo systemctl status airgradient-proxy
curl 'http://localhost:8080/get-version'
```

The second command should answer `{"version": "1.0"}`.  Within one poll
interval — 30 seconds at the default settings —
`curl 'http://localhost:8080/measures/current'` will return a reading.  See [Running the proxy](running.md) for the log and
[Troubleshooting](troubleshooting.md) if nothing arrives.

{: .note }
`/get-version` reports the version of the REST command set, not the version
of the program.  For the program version, see the `Version` line the daemon
logs at startup.

## A second proxy for redundancy

The sensor is happy to be polled by two proxies, and two proxies on two
machines mean a client can still get readings when one machine is down.
Install the second exactly as above, with
[`--poll-freq-offset`](configuration.md#poll-freq-offset) set to something
other than `0` so the two never hit the sensor at the same moment:

```sh
sudo ./install --sensor airgradient-indoor --poll-freq-offset 15 -y
```

Each proxy keeps its own database, so each accumulates its own archive
history.

## Uninstalling

```sh
sudo ./install --uninstall [<target-dir>]
```

The target directory is left in place, with its configuration and its
database — the archive history is not thrown away by an uninstall.
