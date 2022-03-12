-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
-- 
--   http://www.apache.org/licenses/LICENSE-2.0
-- 
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.
CREATE TABLE `region` (
  `r_regionkey` integer NOT NULL,
  `r_name` char(25) NOT NULL,
  `r_comment` varchar(152)
) DISTRIBUTED BY HASH(`r_regionkey`) BUCKETS 1 PROPERTIES ("replication_num" = "1");

CREATE TABLE `nation` (
  `n_nationkey` integer NOT NULL,
  `n_name` char(25) NOT NULL,
  `n_regionkey` integer NOT NULL,
  `n_comment` varchar(152)
) DISTRIBUTED BY HASH(`n_nationkey`) BUCKETS 1 PROPERTIES ("replication_num" = "1");

CREATE TABLE `part` (
  `p_partkey` integer NOT NULL,
  `p_name` varchar(55) NOT NULL,
  `p_mfgr` char(25) NOT NULL,
  `p_brand` char(10) NOT NULL,
  `p_type` varchar(25) NOT NULL,
  `p_size` integer NOT NULL,
  `p_container` char(10) NOT NULL,
  `p_retailprice` decimal(12, 2) NOT NULL,
  `p_comment` varchar(23) NOT NULL
) DISTRIBUTED BY HASH(`p_partkey`) BUCKETS 32 PROPERTIES ("replication_num" = "1");

CREATE TABLE `supplier` (
  `s_suppkey` integer NOT NULL,
  `s_name` char(25) NOT NULL,
  `s_address` varchar(40) NOT NULL,
  `s_nationkey` integer NOT NULL,
  `s_phone` char(15) NOT NULL,
  `s_acctbal` decimal(12, 2) NOT NULL,
  `s_comment` varchar(101) NOT NULL
) DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 32 PROPERTIES ("replication_num" = "1");

CREATE TABLE `customer` (
  `c_custkey` integer NOT NULL,
  `c_name` varchar(25) NOT NULL,
  `c_address` varchar(40) NOT NULL,
  `c_nationkey` integer NOT NULL,
  `c_phone` char(15) NOT NULL,
  `c_acctbal` decimal(12, 2) NOT NULL,
  `c_mktsegment` char(10) NOT NULL,
  `c_comment` varchar(117) NOT NULL
) DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 32 PROPERTIES ("replication_num" = "1");

CREATE TABLE `partsupp` (
  `ps_partkey` integer NOT NULL,
  `ps_suppkey` integer NOT NULL,
  `ps_availqty` integer NOT NULL,
  `ps_supplycost` decimal(12, 2) NOT NULL,
  `ps_comment` varchar(199) NOT NULL
) DISTRIBUTED BY HASH(`ps_partkey`, `ps_suppkey`) BUCKETS 32 PROPERTIES ("replication_num" = "1");

CREATE TABLE `orders` (
  `o_orderkey` integer NOT NULL,
  `o_custkey` integer NOT NULL,
  `o_orderstatus` char(1) NOT NULL,
  `o_totalprice` decimal(12, 2) NOT NULL,
  `o_orderdate` date NOT NULL,
  `o_orderpriority` char(15) NOT NULL,
  `o_clerk` char(15) NOT NULL,
  `o_shippriority` integer NOT NULL,
  `o_comment` varchar(79) NOT NULL
) DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 32 PROPERTIES ("replication_num" = "1");

CREATE TABLE `lineitem` (
  `l_orderkey` integer NOT NULL,
  `l_linenumber` integer NOT NULL,
  `l_partkey` integer NOT NULL,
  `l_suppkey` integer NOT NULL,
  `l_quantity` decimal(12, 2) NOT NULL,
  `l_extendedprice` decimal(12, 2) NOT NULL,
  `l_discount` decimal(12, 2) NOT NULL,
  `l_tax` decimal(12, 2) NOT NULL,
  `l_returnflag` char(1) NOT NULL,
  `l_linestatus` char(1) NOT NULL,
  `l_shipdate` date NOT NULL,
  `l_commitdate` date NOT NULL,
  `l_receiptdate` date NOT NULL,
  `l_shipinstruct` char(25) NOT NULL,
  `l_shipmode` char(10) NOT NULL,
  `l_comment` varchar(44) NOT NULL
) DISTRIBUTED BY HASH(`l_orderkey`, `l_linenumber`) BUCKETS 48 PROPERTIES ("replication_num" = "1");
