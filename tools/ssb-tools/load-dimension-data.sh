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
# This script is used to load generated ssb data set to Doris
# Only for 4 dimension tables: customer, part, supplier and date.
# Usage:
#       sh load-dimension-data.sh
##############################################################

set -eo pipefail

ROOT=`dirname "$0"`
ROOT=`cd "$ROOT"; pwd`

CURDIR=${ROOT}
SSB_DATA_DIR=$CURDIR/ssb-data/

# check if ssb-data exists
if [[ ! -d $SSB_DATA_DIR/ ]]; then
    echo "$SSB_DATA_DIR does not exist. Run sh gen-ssb-data.sh first."
    exit 1
fi

check_prerequest() {
    local CMD=$1
    local NAME=$2
    if ! $CMD; then
        echo "$NAME is missing. This script depends on cURL to load data to Doris."
        exit 1
    fi
}

check_prerequest "curl --version" "curl"

# load 4 small dimension tables

source $CURDIR/doris-cluster.conf

echo "FE_HOST: $FE_HOST"
echo "FE_HTTP_PORT: $FE_HTTP_PORT"
echo "USER: $USER"
echo "PASSWORD: $PASSWORD"
echo "DB: $DB"

echo 'Loading data for table: part'
curl --location-trusted -u $USER:$PASSWORD -H "column_separator:|" -H "columns:p_partkey,p_name,p_mfgr,p_category,p_brand,p_color,p_type,p_size,p_container,p_dummy" -T $SSB_DATA_DIR/part.tbl http://$FE_HOST:$FE_HTTP_PORT/api/$DB/part/_stream_load
echo 'Loading data for table: date'
curl --location-trusted -u $USER:$PASSWORD -H "column_separator:|" -H "columns:d_datekey,d_date,d_dayofweek,d_month,d_year,d_yearmonthnum,d_yearmonth,d_daynuminweek,d_daynuminmonth,d_daynuminyear,d_monthnuminyear,d_weeknuminyear,d_sellingseason,d_lastdayinweekfl,d_lastdayinmonthfl,d_holidayfl,d_weekdayfl,d_dummy" -T $SSB_DATA_DIR/date.tbl http://$FE_HOST:$FE_HTTP_PORT/api/$DB/date/_stream_load
echo 'Loading data for table: supplier'
curl --location-trusted -u $USER:$PASSWORD -H "column_separator:|" -H "columns:s_suppkey,s_name,s_address,s_city,s_nation,s_region,s_phone,s_dummy" -T $SSB_DATA_DIR/supplier.tbl http://$FE_HOST:$FE_HTTP_PORT/api/$DB/supplier/_stream_load
echo 'Loading data for table: customer'
curl --location-trusted -u $USER:$PASSWORD -H "column_separator:|" -H "columns:c_custkey,c_name,c_address,c_city,c_nation,c_region,c_phone,c_mktsegment,no_use" -T $SSB_DATA_DIR/customer.tbl http://$FE_HOST:$FE_HTTP_PORT/api/$DB/customer/_stream_load
