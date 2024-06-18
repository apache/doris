#!/usr/bin/env bash

set -euo pipefail

echo "Copying kerberos keytabs to keytabs/"
mkdir -p /etc/hadoop-init.d/
cp /etc/trino/conf/* /keytabs/
/usr/local/hadoop-run.sh

hive -f /usr/local/sql/create_kerberos_hive_table.sql
