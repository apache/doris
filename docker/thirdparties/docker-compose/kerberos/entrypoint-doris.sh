#!/usr/bin/env bash

set -euo pipefail

cp /root/conf/doris-krb5.conf /etc/krb5.conf

rm -rf /root/output/fe/doris-meta/*
rm -rf /root/output/fe/log/*

rm -rf /root/output/be/storage/*
rm -rf /root/output/be/log/*

/root/output/fe/bin/start_fe.sh --daemon
/root/output/be/bin/start_be.sh

