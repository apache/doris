#!/bin/bash

DORIS_ROOT=${DORIS_ROOT:-"/opt/apache-doris"}
DORIS_HOME=${DORIS_ROOT}/be
$DORIS_HOME/bin/stop_be.sh
