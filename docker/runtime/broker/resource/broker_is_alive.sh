#!/bin/bash

log_stderr()
{
    echo "[`date`] $@" >&2
}

rpc_port=$1
if [[ "x$rpc_port" == "x" ]]; then
    echo "need broker rpc_port as paramter!"
    exit 1
fi

netstat -nltu | grep ":$rpc_port " > /dev/null

if [ $? -eq 0 ]; then
#  log_stderr "broker ($rpc_port)alive，ProbeHandler ExecAction get exit 0"
  exit 0
else
#  log_stderr "broker($rpc_port)not alive，ProbeHandler ExecAction get exit 1"
  exit 1
fi
