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
CREATE DATABASE IF NOT EXISTS '"${db}"' ;
USE '"${db}"';

CREATE TABLE IF NOT EXISTS `customer`(
  `c_custkey` int,
  `c_name` string,
  `c_address` string,
  `c_nationkey` int,
  `c_phone` string,
  `c_acctbal` decimal(12,2),
  `c_mktsegment` string,
  `c_comment` string)
USING '"${format}"'
LOCATION "'"${db_loc}"/customer'";

CREATE TABLE IF NOT EXISTS `lineitem`(
  `l_orderkey` int,
  `l_partkey` int,
  `l_suppkey` int,
  `l_linenumber` int,
  `l_quantity` decimal(12,2),
  `l_extendedprice` decimal(12,2),
  `l_discount` decimal(12,2),
  `l_tax` decimal(12,2),
  `l_returnflag` string,
  `l_linestatus` string,
  `l_shipdate` date,
  `l_commitdate` date,
  `l_receiptdate` date,
  `l_shipinstruct` string,
  `l_shipmode` string,
  `l_comment` string)
USING '"${format}"'
LOCATION "'"${db_loc}"/lineitem'";

CREATE TABLE IF NOT EXISTS `nation`(
  `n_nationkey` int,
  `n_name` string,
  `n_regionkey` int,
  `n_comment` string)
USING '"${format}"'
LOCATION "'"${db_loc}"/nation'";

CREATE TABLE IF NOT EXISTS `orders`(
  `o_orderkey` int,
  `o_custkey` int,
  `o_orderstatus` string,
  `o_totalprice` decimal(12,2),
  `o_orderdate` date,
  `o_orderpriority` string,
  `o_clerk` string,
  `o_shippriority` int,
  `o_comment` string)
USING '"${format}"'
LOCATION "'"${db_loc}"/orders'";

CREATE TABLE IF NOT EXISTS `part`(
  `p_partkey` int,
  `p_name` string,
  `p_mfgr` string,
  `p_brand` string,
  `p_type` string,
  `p_size` int,
  `p_container` string,
  `p_retailprice` decimal(12,2),
  `p_comment` string)
USING '"${format}"'
LOCATION "'"${db_loc}"/part'";

CREATE TABLE IF NOT EXISTS `partsupp`(
  `ps_partkey` int,
  `ps_suppkey` int,
  `ps_availqty` int,
  `ps_supplycost` decimal(12,2),
  `ps_comment` string)
USING '"${format}"'
LOCATION "'"${db_loc}"/partsupp'";

CREATE TABLE IF NOT EXISTS `region` (
  `r_regionkey` int,
  `r_name` string,
  `r_comment` string)
USING '"${format}"'
LOCATION "'"${db_loc}"/region'";

CREATE TABLE IF NOT EXISTS `supplier`(
  `s_suppkey` int,
  `s_name` string,
  `s_address` string,
  `s_nationkey` int,
  `s_phone` string,
  `s_acctbal` decimal(12,2),
  `s_comment` string)
USING '"${format}"'
LOCATION "'"${db_loc}"/supplier'";

'
