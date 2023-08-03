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
# See emr_tools.sh
##############################################################

if [[ -z "$1" ]]; then
    echo 'the first argument is database location'
    exit
else
    db_loc=$1
fi

if [[ -z "$2" ]]; then
    echo 'the second argument is database name'
    exit
else
    db=$2
fi

if [[ -z "$3" ]]; then
    format=parquet
else
    format=$3
fi
# shellcheck disable=SC2016
echo '
CREATE DATABASE IF NOT EXISTS '"${db}"';
USE '"${db}"';

CREATE TABLE IF NOT EXISTS `customer`(
  `c_custkey` BIGINT COMMENT "",
  `c_name` VARCHAR(26) COMMENT "",
  `c_address` VARCHAR(41) COMMENT "",
  `c_city` VARCHAR(11) COMMENT "",
  `c_nation` VARCHAR(16) COMMENT "",
  `c_region` VARCHAR(13) COMMENT "",
  `c_phone` VARCHAR(16) COMMENT "",
  `c_mktsegment` VARCHAR(11) COMMENT "")
USING '"${format}"'
LOCATION "'"${db_loc}"/customer'";

CREATE TABLE IF NOT EXISTS `dates`(
  `d_datekey` BIGINT COMMENT "",
  `d_date` VARCHAR(20) COMMENT "",
  `d_dayofweek` VARCHAR(10) COMMENT "",
  `d_month` VARCHAR(11) COMMENT "",
  `d_year` BIGINT COMMENT "",
  `d_yearmonthnum` BIGINT COMMENT "",
  `d_yearmonth` VARCHAR(9) COMMENT "",
  `d_daynuminweek` BIGINT COMMENT "",
  `d_daynuminmonth` BIGINT COMMENT "",
  `d_daynuminyear` BIGINT COMMENT "",
  `d_monthnuminyear` BIGINT COMMENT "",
  `d_weeknuminyear` BIGINT COMMENT "",
  `d_sellingseason` VARCHAR(14) COMMENT "",
  `d_lastdayinweekfl` BIGINT COMMENT "",
  `d_lastdayinmonthfl` BIGINT COMMENT "",
  `d_holidayfl` BIGINT COMMENT "",
  `d_weekdayfl` BIGINT COMMENT "")
USING '"${format}"'
LOCATION "'"${db_loc}"/dates'";

CREATE TABLE IF NOT EXISTS `lineorder`(
  `lo_orderkey` BIGINT COMMENT "",
  `lo_linenumber` BIGINT COMMENT "",
  `lo_custkey` BIGINT COMMENT "",
  `lo_partkey` BIGINT COMMENT "",
  `lo_suppkey` BIGINT COMMENT "",
  `lo_orderdate` BIGINT COMMENT "",
  `lo_orderpriority` VARCHAR(16) COMMENT "",
  `lo_shippriority` BIGINT COMMENT "",
  `lo_quantity` BIGINT COMMENT "",
  `lo_extendedprice` BIGINT COMMENT "",
  `lo_ordtotalprice` BIGINT COMMENT "",
  `lo_discount` BIGINT COMMENT "",
  `lo_revenue` BIGINT COMMENT "",
  `lo_supplycost` BIGINT COMMENT "",
  `lo_tax` BIGINT COMMENT "",
  `lo_commitdate` BIGINT COMMENT "",
  `lo_shipmode` VARCHAR(11) COMMENT "")
USING '"${format}"'
LOCATION "'"${db_loc}"/lineorder'";

CREATE TABLE IF NOT EXISTS `part`(
  `p_partkey` BIGINT COMMENT "",
  `p_name` VARCHAR(23) COMMENT "",
  `p_mfgr` VARCHAR(7) COMMENT "",
  `p_category` VARCHAR(8) COMMENT "",
  `p_brand` VARCHAR(10) COMMENT "",
  `p_color` VARCHAR(12) COMMENT "",
  `p_type` VARCHAR(26) COMMENT "",
  `p_size` BIGINT COMMENT "",
  `p_container` VARCHAR(11) COMMENT "")
USING '"${format}"'
LOCATION "'"${db_loc}"/part'";

CREATE TABLE IF NOT EXISTS `supplier`(
  `s_suppkey` BIGINT COMMENT "",
  `s_name` VARCHAR(26) COMMENT "",
  `s_address` VARCHAR(26) COMMENT "",
  `s_city` VARCHAR(11) COMMENT "",
  `s_nation` VARCHAR(16) COMMENT "",
  `s_region` VARCHAR(13) COMMENT "",
  `s_phone` VARCHAR(16) COMMENT "")
USING '"${format}"'
LOCATION "'"${db_loc}"/supplier'";

CREATE TABLE IF NOT EXISTS `lineorder_flat` (
  `lo_orderdate` BIGINT COMMENT "",
  `lo_orderkey` BIGINT COMMENT "",
  `lo_linenumber` TINYINT COMMENT "",
  `lo_custkey` BIGINT COMMENT "",
  `lo_partkey` BIGINT COMMENT "",
  `lo_suppkey` BIGINT COMMENT "",
  `lo_orderpriority` VARCHAR(100) COMMENT "",
  `lo_shippriority` TINYINT COMMENT "",
  `lo_quantity` TINYINT COMMENT "",
  `lo_extendedprice` BIGINT COMMENT "",
  `lo_ordtotalprice` BIGINT COMMENT "",
  `lo_discount` TINYINT COMMENT "",
  `lo_revenue` BIGINT COMMENT "",
  `lo_supplycost` BIGINT COMMENT "",
  `lo_tax` TINYINT COMMENT "",
  `lo_commitdate` BIGINT COMMENT "",
  `lo_shipmode` VARCHAR(100) COMMENT "",
  `c_name` VARCHAR(100) COMMENT "",
  `c_address` VARCHAR(100) COMMENT "",
  `c_city` VARCHAR(100) COMMENT "",
  `c_nation` VARCHAR(100) COMMENT "",
  `c_region` VARCHAR(100) COMMENT "",
  `c_phone` VARCHAR(100) COMMENT "",
  `c_mktsegment` VARCHAR(100) COMMENT "",
  `s_name` VARCHAR(100) COMMENT "",
  `s_address` VARCHAR(100) COMMENT "",
  `s_city` VARCHAR(100) COMMENT "",
  `s_nation` VARCHAR(100) COMMENT "",
  `s_region` VARCHAR(100) COMMENT "",
  `s_phone` VARCHAR(100) COMMENT "",
  `p_name` VARCHAR(100) COMMENT "",
  `p_mfgr` VARCHAR(100) COMMENT "",
  `p_category` VARCHAR(100) COMMENT "",
  `p_brand` VARCHAR(100) COMMENT "",
  `p_color` VARCHAR(100) COMMENT "",
  `p_type` VARCHAR(100) COMMENT "",
  `p_size` TINYINT COMMENT "",
  `p_container` VARCHAR(100) COMMENT "")
USING '"${format}"'
LOCATION "'"${db_loc}"/lineorder_flat'";
'
