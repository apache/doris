#!/bin/bash

DORIS_ROOT=${DORIS_ROOT:-"/opt/apache-doris"}
DORIS_HOME=${DORIS_ROOT}/apache_hdfs_broker
$DORIS_HOME/bin/stop_broker.sh
