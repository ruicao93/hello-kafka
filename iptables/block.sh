#!/usr/bin/env bash

iptables -A OUTPUT -p tcp --dport 9092 -j DROP
