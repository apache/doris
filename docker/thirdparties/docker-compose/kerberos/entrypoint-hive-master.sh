#!/usr/bin/env bash

set -euo pipefail

echo "Copying kerberos keytabs to keytabs/"
mkdir -p /etc/hadoop-init.d/
cp /etc/trino/conf/* /keytabs/
/usr/local/hadoop-run.sh

kinit -kt /etc/hive/conf/hive.keytab hive/hadoop-master@LABS.TERADATA.COM
hive  -f /usr/local/sql/create_kerberos_hive_table.sql


