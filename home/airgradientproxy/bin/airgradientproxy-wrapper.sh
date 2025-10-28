#!/bin/sh

# Copyright (c) 2025 John A Kline
# See the file LICENSE for your full rights.

cd /home/airgradientproxy/bin
nohup /home/airgradientproxy/bin/airgradientproxyd /home/airgradientproxy/airgradientproxy.conf $@ > /dev/null 2>&1 &
