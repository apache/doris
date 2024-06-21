#!/usr/bin/env bash

set -euo pipefail

echo "Copying kerberos keytabs to /keytabs/"
mkdir -p /etc/hadoop-init.d/
cp /etc/trino/conf/hive-presto-master.keytab /keytabs/other-hive-presto-master.keytab
cp /etc/trino/conf/presto-server.keytab /keytabs/other-presto-server.keytab
cp /keytabs/update-location.sh /etc/hadoop-init.d/update-location.sh
/usr/local/hadoop-run.sh

sleep 20

echo "Init kerberos test data"
kinit -kt /etc/hive/conf/hive.keytab hive/hadoop-master-2@OTHERREALM.COM
hive  -f /usr/local/sql/create_kerberos_hive_table.sql

tail -f /dev/null
