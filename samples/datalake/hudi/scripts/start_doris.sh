#!/usr/bin/env bash

cp -r /opt/doris-bin /opt/doris

/opt/doris/fe/bin/start_fe.sh --daemon
/opt/doris/be/bin/start_be.sh --daemon
tail -F /dev/null
