#!/bin/bash
set -x

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

# create table
hive -f "${CUR_DIR}"/orc_predicate_table.hql


