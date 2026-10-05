#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

##############################################################
# This script is used to generate TPC-H data set
##############################################################

set -eo pipefail

ROOT=$(dirname "$0")
ROOT=$(
    cd "${ROOT}"
    pwd
)

CURDIR="${ROOT}"

usage() {
    echo "
Usage: $0 <options>
  Optional options:
     -s             scale factor, default is 500m
     -c             parallelism to generate data of (lineitem, orders, partsupp) table, default is 10

  Eg.
    $0              generate data using default value.
    $0 -s 1b       generate data with scale 1b.
  "
    exit 1
}

OPTS=$(getopt \
    -n "$0" \
    -o '' \
    -o 'hs:c:' \
    -- "$@")

eval set -- "${OPTS}"

SCALE_FACTOR="500m"
HELP=0

if [[ $# == 0 ]]; then
    usage
fi

while true; do
    case "$1" in
    -h)
        HELP=1
        shift
        ;;
    -s)
        SCALE_FACTOR=$2
        shift 2
        ;;
    --)
        shift
        break
        ;;
    *)
        echo "Internal error"
        exit 1
        ;;
    esac
done

if [[ "${HELP}" -eq 1 ]]; then
    usage
fi

echo "Scale Factor: ${SCALE_FACTOR}"

source "${CURDIR}/../conf/doris-cluster.conf"
export MYSQL_PWD=${PASSWORD:-}

echo "FE_HOST: ${FE_HOST:='127.0.0.1'}"
echo "FE_QUERY_PORT: ${FE_QUERY_PORT:='9030'}"
echo "USER: ${USER:='root'}"
echo "DB: ${DB:='coffee_bench'}"

run_sql() {
    mysql -h"${FE_HOST}" -u"${USER}" -P"${FE_QUERY_PORT}" -D"${DB}" -e "$*"
}

# Load dim_products data from S3
run_sql "INSERT INTO dim_products
SELECT
    COALESCE(record_id, '') AS record_id,
    COALESCE(product_id, '') AS product_id,
    COALESCE(name, '') AS name,
    COALESCE(category, '') AS category,
    COALESCE(subcategory, '') AS subcategory,
    COALESCE(standard_cost, 0.0) AS standard_cost,
    COALESCE(standard_price, 0.0) AS standard_price,
    COALESCE(from_date, '1970-01-01') AS from_date,
    COALESCE(to_date, '9999-12-31') AS to_date
FROM s3(
    'uri' = 's3://doris-regression/coffee_bench/dim_products/*',
    's3.endpoint' = 's3.us-east-1.amazonaws.com',
    's3.region' = 'us-east-1',
    'format' = 'parquet'
);"

# Load dim_locations data from S3
run_sql "INSERT INTO dim_locations
SELECT
    COALESCE(record_id, '') AS record_id,
    COALESCE(location_id, '') AS location_id,
    COALESCE(city, '') AS city,
    COALESCE(state, '') AS state,
    COALESCE(country, '') AS country,
    COALESCE(region, '') AS region
FROM s3(
    'uri' = 's3://doris-regression/coffee_bench/dim_locations/*',
    's3.endpoint' = 's3.us-east-1.amazonaws.com',
    's3.region' = 'us-east-1',
    'format' = 'parquet'
);"

# load fact_sale
run_sql "INSERT INTO fact_sales
SELECT
    COALESCE(order_id, '') AS order_id,
    COALESCE(order_line_id, '') AS order_line_id,
    COALESCE(order_date, '1970-01-01') AS order_date,
    COALESCE(time_of_day, '') AS time_of_day,
    COALESCE(season, '') AS season,
    COALESCE(month, 0) AS month,
    COALESCE(location_id, '') AS location_id,
    COALESCE(region, '') AS region,
    COALESCE(product_name, '') AS product_name,
    COALESCE(quantity, 0) AS quantity,
    COALESCE(sales_amount, 0.0) AS sales_amount,
    COALESCE(discount_percentage, 0) AS discount_percentage,
    COALESCE(product_id, '') AS product_id
FROM s3(
    'uri' = 's3://doris-regression/coffee_bench/fact_sales_${SCALE_FACTOR}/*',
    's3.endpoint' = 's3.us-east-1.amazonaws.com',
    's3.region' = 'us-east-1',
    'format' = 'parquet'
);"

echo "load finish"
