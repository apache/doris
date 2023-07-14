#!/bin/bash

# usage: sh run.sh dlf parquet
FE_HOST=$1
USER=$2
PORT=$3
if [ -z "$4" ]; then
    echo 'need catalog name'
    exit
else 
    catalog_name=$4
fi

if [ -z "$5" ]; then
    echo "run all test"
else 
    case=$5
fi

if [[ -z ${TYPE} ]]; then
  TYPE=obj
fi
echo "execute ${case} benchmark for ${TYPE}..."

if [ "${case}" = 'ssb' ] ; then
    # ssb
    sh run_queries.sh "${FE_HOST}" "${USER}" "${PORT}" "${catalog_name}".ssb100_parquet_"${TYPE}" queries/ssb_queries.sql
    sh run_queries.sh "${FE_HOST}" "${USER}" "${PORT}" "${catalog_name}".ssb100_orc_"${TYPE}" queries/ssb_queries.sql
elif [ "${case}" = 'ssb_flat' ]; then
    # ssb_flat
    sh run_queries.sh "${FE_HOST}" "${USER}" "${PORT}" "${catalog_name}".ssb100_parquet_"${TYPE}" queries/ssb_flat_queries.sql
    sh run_queries.sh "${FE_HOST}" "${USER}" "${PORT}" "${catalog_name}".ssb100_orc_"${TYPE}" queries/ssb_flat_queries.sql
elif [ "${case}" = 'tpch' ]; then
    # tpch
    sh run_queries.sh "${FE_HOST}" "${USER}" "${PORT}" "${catalog_name}".tpch100_parquet_"${TYPE}" queries/tpch_queries.sql
    sh run_queries.sh "${FE_HOST}" "${USER}" "${PORT}" "${catalog_name}".tpch100_orc_"${TYPE}" queries/tpch_queries.sql
elif [ "${case}" = 'clickbench' ]; then
    # clickbench
    sh run_queries.sh "${FE_HOST}" "${USER}" "${PORT}" "${catalog_name}".clickbench_parquet_"${TYPE}" queries/clickbench_queries.sql
    sh run_queries.sh "${FE_HOST}" "${USER}" "${PORT}" "${catalog_name}".clickbench_orc_"${TYPE}" queries/clickbench_queries.sql
fi