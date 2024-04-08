#!/bin/bash

DORIS_ROOT=${DORIS_ROOT:-"/opt/apache-doris"}
DORIS_HOME=${DORIS_ROOT}/fe
$DORIS_HOME/bin/stop_fe.sh
