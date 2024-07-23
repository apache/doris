#!/bin/bash
set -x

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

# create table
hive -f "${CUR_DIR}"/some_serde_table.hql


