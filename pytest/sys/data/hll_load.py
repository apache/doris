#!/bin/env python
# -*- coding: utf-8 -*-
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

"""
the data set for test delete on palo
Date: 2015/03/23 15:10:39
"""


hll_load='CREATE TABLE `%s` ( \
         `k1` tinyint(4) NULL COMMENT "",   \
         `k2` smallint(6) NULL COMMENT "",  \
         `k3` int(11) NULL COMMENT "",      \
         `k4` bigint(20) NULL COMMENT "",   \
         `k5` decimal(9, 3) NULL COMMENT "",\
         `k6` char(5) NULL COMMENT "",      \
         `k10` date NULL COMMENT "",        \
         `k11` datetime NULL COMMENT "",    \
         `k7` varchar(20) NULL COMMENT "",  \
         `k8` double MAX NULL COMMENT "",   \
         `k9` float SUM NULL COMMENT "",    \
         `k1_hll` hll hll_union COMMENT "",     \
         `k2_hll` hll hll_union COMMENT "",     \
         `k3_hll` hll hll_union COMMENT "",     \
         `k4_hll` hll hll_union COMMENT "",     \
         `k5_hll` hll hll_union COMMENT "",     \
         `k6_hll` hll hll_union COMMENT "",     \
         `k10_hll` hll hll_union COMMENT "",    \
         `k11_hll` hll hll_union COMMENT "",    \
         `k7_hll` hll hll_union COMMENT "",     \
         `k8_hll` hll hll_union COMMENT "",     \
         `k9_hll` hll hll_union COMMENT ""      \
        ) ENGINE=OLAP                       \
         DISTRIBUTED BY HASH(`k1`) BUCKETS 5 \
         PROPERTIES (                        \
        "storage_type" = "COLUMN"            \
        );'

columns = ['k1_hll', 'k1', 'k2_hll', 'k2', 'k3_hll', 'k3', 'k4_hll', 'k4', 'k5_hll', 'k5',
            'k6_hll', 'k6', 'k7_hll', 'k7', 'k8_hll', 'k8',
            'k9_hll', 'k9', 'k10_hll', 'k10', 'k11_hll', 'k11']

key_columns = ['k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k10', 'k11', 'k7']

data_columns = ['k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k10', 'k11', 'k7', 'k8', 'k9']
data_columns_1 = ['k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7', 'k10', 'k11', 'k8', 'k9', 'k12', 'k13']

columns_func = ['k1_hll = hll_hash(k1)', 'k2_hll = hll_hash(k2)',
                'k3_hll = hll_hash(k3)', 'k4_hll = hll_hash(k4)',
                'k5_hll = hll_hash(k5)', 'k6_hll = hll_hash(k6)',
                'k7_hll = hll_hash(k7)', 'k8_hll = hll_hash(k8)',
                'k9_hll = hll_hash(k9)', 'k10_hll = hll_hash(k10)',
                'k11_hll = hll_hash(k11)']

columns_func_1 = ['k1_hll = hll_hash_tinyint(k1)', 'k2_hll = hll_hash_smallint(k2)',
                'k3_hll = hll_hash_int(k3)', 'k4_hll = hll_hash_bigint(k4)',
                'k5_hll = hll_hash_decimal(k5)', 'k6_hll = hll_hash_string(k6)',
                'k7_hll = hll_hash_string(k7)', 'k8_hll = hll_hash_double(k8)',
                'k9_hll = hll_hash_float(k9)', 'k10_hll = hll_hash_date(k10)',
                'k11_hll = hll_hash_datetime(k11)']

k1_hll = 'CREATE TABLE `%s` ( \
    `k1` tinyint(4) NULL COMMENT "",   \
    `k2` smallint(6) replace NULL COMMENT "",  \
    `k3` int(11) replace NULL COMMENT "",      \
    `k4` bigint(20) replace NULL COMMENT "",   \
    `k5` decimal(9, 3) replace NULL COMMENT "",\
    `k6` char(11) replace NULL COMMENT "",      \
    `k10` date replace NULL COMMENT "",        \
    `k11` datetime replace NULL COMMENT "",    \
    `k7` varchar(51) replace NULL COMMENT "",  \
    `k8` double MAX NULL COMMENT "",   \
    `k9` float SUM NULL COMMENT "",    \
    `k1_hll` hll hll_union COMMENT "",     \
    `k2_hll` hll hll_union COMMENT "",     \
    `k3_hll` hll hll_union COMMENT "",     \
    `k4_hll` hll hll_union COMMENT "",     \
    `k5_hll` hll hll_union COMMENT "",     \
    `k6_hll` hll hll_union COMMENT "",     \
    `k10_hll` hll hll_union COMMENT "",    \
    `k11_hll` hll hll_union COMMENT "",    \
    `k7_hll` hll hll_union COMMENT "",     \
    `k8_hll` hll hll_union COMMENT "",     \
    `k9_hll` hll hll_union COMMENT ""      \
   ) ENGINE=OLAP                       \
    PARTITION BY RANGE (k1)(            \
    PARTITION p1 VALUES LESS THAN ("-100"),\
    PARTITION p2 VALUES LESS THAN ("0"), \
    PARTITION p3 VALUES LESS THAN ("100"),\
    PARTITION p4 VALUES LESS THAN MAXVALUE)\
    DISTRIBUTED BY HASH(`k1`) BUCKETS 5 \
    PROPERTIES (                        \
   "storage_type" = "COLUMN"            \
   );'

k2_hll = 'CREATE TABLE `%s` ( \
    `k2` smallint(6) NULL COMMENT "",  \
    `k1` tinyint(4) replace NULL COMMENT "",   \
    `k3` int(11) replace NULL COMMENT "",      \
    `k4` bigint(20) replace NULL COMMENT "",   \
    `k5` decimal(9, 3) replace NULL COMMENT "",\
    `k6` char(11) replace NULL COMMENT "",      \
    `k10` date replace NULL COMMENT "",        \
    `k11` datetime replace NULL COMMENT "",    \
    `k7` varchar(51) replace NULL COMMENT "",  \
    `k8` double MAX NULL COMMENT "",   \
    `k9` float SUM NULL COMMENT "",     \
    `k1_hll` hll hll_union COMMENT "",      \
    `k2_hll` hll hll_union COMMENT "",     \
    `k3_hll` hll hll_union COMMENT "",     \
    `k4_hll` hll hll_union COMMENT "",     \
    `k5_hll` hll hll_union COMMENT "",     \
    `k6_hll` hll hll_union COMMENT "",     \
    `k10_hll` hll hll_union COMMENT "",    \
    `k11_hll` hll hll_union COMMENT "",    \
    `k7_hll` hll hll_union COMMENT "",     \
    `k8_hll` hll hll_union COMMENT "",     \
    `k9_hll` hll hll_union COMMENT ""      \
   ) ENGINE=OLAP                       \
    PARTITION BY RANGE (k2)(            \
    PARTITION p1 VALUES LESS THAN ("-15600"),\
    PARTITION p2 VALUES LESS THAN ("0"), \
    PARTITION p3 VALUES LESS THAN ("15600"),\
    PARTITION p4 VALUES LESS THAN MAXVALUE)\
    DISTRIBUTED BY HASH(k2) BUCKETS 5 \
    PROPERTIES (                        \
   "storage_type" = "COLUMN"            \
   );'

# PARTITION
k3_hll = 'CREATE TABLE `%s` ( \
    `k3` int(11) not null COMMENT "",      \
    `k1` tinyint(4) replace NOT NULL COMMENT "",   \
    `k2` smallint(6) replace NOT NULL COMMENT "",  \
    `k4` bigint(20) replace NOT NULL COMMENT "",   \
    `k5` decimal(9, 3) replace NOT NULL COMMENT "",\
    `k6` char(11) replace NOT NULL COMMENT "",      \
    `k10` date replace NOT NULL COMMENT "",        \
    `k11` datetime replace NOT NULL COMMENT "",    \
    `k7` varchar(51) replace NOT NULL COMMENT "",  \
    `k8` double MAX NOT NULL COMMENT "",   \
    `k9` float SUM NOT NULL COMMENT "",     \
    `k1_hll` hll hll_union NOT NULL COMMENT "",      \
    `k2_hll` hll hll_union NOT NULL COMMENT "",     \
    `k3_hll` hll hll_union NOT NULL COMMENT "",     \
    `k4_hll` hll hll_union NOT NULL COMMENT "",     \
    `k5_hll` hll hll_union NOT NULL COMMENT "",     \
    `k6_hll` hll hll_union NOT NULL COMMENT "",     \
    `k10_hll` hll hll_union NOT NULL COMMENT "",    \
    `k11_hll` hll hll_union NOT NULL COMMENT "",    \
    `k7_hll` hll hll_union NOT NULL COMMENT "",     \
    `k8_hll` hll hll_union NOT NULL COMMENT "",     \
    `k9_hll` hll hll_union NOT NULL COMMENT ""      \
   ) ENGINE=OLAP                       \
    PARTITION BY RANGE (k3)(             \
    PARTITION p1 VALUES LESS THAN ("0"), \
    PARTITION p2 VALUES LESS THAN MAXVALUE)\
    DISTRIBUTED BY HASH(`k3`) BUCKETS 5 \
    PROPERTIES (                        \
   "storage_type" = "COLUMN"            \
   );'

# RANGE BUCKETS
k4_hll = 'CREATE TABLE `%s` ( \
    `k4` bigint(20) NULL COMMENT "",   \
    `k1` tinyint(4) replace NULL COMMENT "",   \
    `k2` smallint(6) replace NULL COMMENT "",  \
    `k3` int(11) replace NULL COMMENT "",      \
    `k5` decimal(9, 3) replace NULL COMMENT "",\
    `k6` char(11) replace NULL COMMENT "",      \
    `k10` date replace NULL COMMENT "",        \
    `k11` datetime replace NULL COMMENT "",    \
    `k7` varchar(51) replace NULL COMMENT "",  \
    `k8` double MAX NULL COMMENT "",   \
    `k9` float SUM NULL COMMENT "",     \
    `k1_hll` hll hll_union COMMENT "",      \
    `k2_hll` hll hll_union COMMENT "",     \
    `k3_hll` hll hll_union COMMENT "",     \
    `k4_hll` hll hll_union COMMENT "",     \
    `k5_hll` hll hll_union COMMENT "",     \
    `k6_hll` hll hll_union COMMENT "",     \
    `k10_hll` hll hll_union COMMENT "",    \
    `k11_hll` hll hll_union COMMENT "",    \
    `k7_hll` hll hll_union COMMENT "",     \
    `k8_hll` hll hll_union COMMENT "",     \
    `k9_hll` hll hll_union COMMENT ""      \
   ) ENGINE=OLAP                       \
    PARTITION BY RANGE (k4)(            \
    PARTITION p1 VALUES LESS THAN ("0"), \
    PARTITION p2 VALUES LESS THAN MAXVALUE)\
    DISTRIBUTED BY HASH(k4) BUCKETS 5 \
    PROPERTIES (                        \
   "storage_type" = "COLUMN"            \
   );'

k5_hll = 'CREATE TABLE `%s` ( \
    `k5` decimal(9, 3) NULL COMMENT "",\
    `k1` tinyint(4) replace NULL COMMENT "",   \
    `k2` smallint(6) replace NULL COMMENT "",  \
    `k3` int(11) replace NULL COMMENT "",      \
    `k4` bigint(20) replace NULL COMMENT "",   \
    `k6` char(11) replace NULL COMMENT "",      \
    `k10` date replace NULL COMMENT "",        \
    `k11` datetime replace NULL COMMENT "",    \
    `k7` varchar(51) replace NULL COMMENT "",  \
    `k8` double MAX NULL COMMENT "",   \
    `k9` float SUM NULL COMMENT "",     \
    `k1_hll` hll hll_union COMMENT "",      \
    `k2_hll` hll hll_union COMMENT "",     \
    `k3_hll` hll hll_union COMMENT "",     \
    `k4_hll` hll hll_union COMMENT "",     \
    `k5_hll` hll hll_union COMMENT "",     \
    `k6_hll` hll hll_union COMMENT "",     \
    `k10_hll` hll hll_union COMMENT "",    \
    `k11_hll` hll hll_union COMMENT "",    \
    `k7_hll` hll hll_union COMMENT "",     \
    `k8_hll` hll hll_union COMMENT "",     \
    `k9_hll` hll hll_union COMMENT ""      \
   ) ENGINE=OLAP                       \
    DISTRIBUTED BY HASH(`k5`) BUCKETS 5 \
    PROPERTIES (                        \
   "storage_type" = "COLUMN"            \
   );'

k6_hll = 'CREATE TABLE `%s` ( \
    `k6` char(11) NULL COMMENT "",      \
    `k1` tinyint(4) replace NULL COMMENT "",   \
    `k2` smallint(6) replace NULL COMMENT "",  \
    `k3` int(11) replace NULL COMMENT "",      \
    `k4` bigint(20) replace NULL COMMENT "",   \
    `k5` decimal(9, 3) replace NULL COMMENT "",\
    `k10` date replace NULL COMMENT "",        \
    `k11` datetime replace NULL COMMENT "",    \
    `k7` varchar(51) replace NULL COMMENT "",  \
    `k8` double MAX NULL COMMENT "",   \
    `k9` float SUM NULL COMMENT "" ,    \
    `k1_hll` hll hll_union COMMENT "",      \
    `k2_hll` hll hll_union COMMENT "",     \
    `k3_hll` hll hll_union COMMENT "",     \
    `k4_hll` hll hll_union COMMENT "",     \
    `k5_hll` hll hll_union COMMENT "",     \
    `k6_hll` hll hll_union COMMENT "",     \
    `k10_hll` hll hll_union COMMENT "",    \
    `k11_hll` hll hll_union COMMENT "",    \
    `k7_hll` hll hll_union COMMENT "",     \
    `k8_hll` hll hll_union COMMENT "",     \
    `k9_hll` hll hll_union COMMENT ""      \
   ) ENGINE=OLAP                       \
    DISTRIBUTED BY HASH(k6) BUCKETS 5 \
    PROPERTIES (                        \
   "storage_type" = "COLUMN"            \
   );'

k7_hll = 'CREATE TABLE `%s` ( \
    `k7` varchar(51) NULL COMMENT "",  \
    `k1` tinyint(4) replace NULL COMMENT "",   \
    `k2` smallint(6) replace NULL COMMENT "",  \
    `k3` int(11) replace NULL COMMENT "",      \
    `k4` bigint(20) replace NULL COMMENT "",   \
    `k5` decimal(9, 3) replace NULL COMMENT "",\
    `k6` char(11) replace NULL COMMENT "",      \
    `k10` date replace NULL COMMENT "",        \
    `k11` datetime replace NULL COMMENT "",    \
    `k8` double MAX NULL COMMENT "",   \
    `k9` float SUM NULL COMMENT "" ,    \
    `k1_hll` hll hll_union COMMENT "",      \
    `k2_hll` hll hll_union COMMENT "",     \
    `k3_hll` hll hll_union COMMENT "",     \
    `k4_hll` hll hll_union COMMENT "",     \
    `k5_hll` hll hll_union COMMENT "",     \
    `k6_hll` hll hll_union COMMENT "",     \
    `k10_hll` hll hll_union COMMENT "",    \
    `k11_hll` hll hll_union COMMENT "",    \
    `k7_hll` hll hll_union COMMENT "",     \
    `k8_hll` hll hll_union COMMENT "",     \
    `k9_hll` hll hll_union COMMENT ""      \
   ) ENGINE=OLAP                       \
    DISTRIBUTED BY HASH(`k7`) BUCKETS 5 \
    PROPERTIES (                        \
   "storage_type" = "COLUMN"            \
   );'

k10_hll = 'CREATE TABLE `%s` ( \
    `k10` date NULL COMMENT "",        \
    `k1` tinyint(4) replace NULL COMMENT "",   \
    `k2` smallint(6) replace NULL COMMENT "",  \
    `k3` int(11) replace NULL COMMENT "",      \
    `k4` bigint(20) replace NULL COMMENT "",   \
    `k5` decimal(9, 3) replace NULL COMMENT "",\
    `k6` char(11) replace NULL COMMENT "",      \
    `k11` datetime replace NULL COMMENT "",    \
    `k7` varchar(51) replace NULL COMMENT "",  \
    `k8` double MAX NULL COMMENT "",   \
    `k9` float SUM NULL COMMENT "" ,    \
    `k1_hll` hll hll_union COMMENT "",      \
    `k2_hll` hll hll_union COMMENT "",     \
    `k3_hll` hll hll_union COMMENT "",     \
    `k4_hll` hll hll_union COMMENT "",     \
    `k5_hll` hll hll_union COMMENT "",     \
    `k6_hll` hll hll_union COMMENT "",     \
    `k10_hll` hll hll_union COMMENT "",    \
    `k11_hll` hll hll_union COMMENT "",    \
    `k7_hll` hll hll_union COMMENT "",     \
    `k8_hll` hll hll_union COMMENT "",     \
    `k9_hll` hll hll_union COMMENT ""      \
   ) ENGINE=OLAP                       \
    DISTRIBUTED BY HASH(k10) BUCKETS 5 \
    PROPERTIES (                        \
   "storage_type" = "COLUMN"            \
   );'

k11_hll = 'CREATE TABLE `%s` ( \
    `k11` datetime NULL COMMENT "",    \
    `k1` tinyint(4) replace NULL COMMENT "",   \
    `k2` smallint(6) replace NULL COMMENT "",  \
    `k3` int(11) replace NULL COMMENT "",      \
    `k4` bigint(20) replace NULL COMMENT "",   \
    `k5` decimal(9, 3) replace NULL COMMENT "",\
    `k6` char(11) replace NULL COMMENT "",      \
    `k10` date replace NULL COMMENT "",        \
    `k7` varchar(51) replace NULL COMMENT "",  \
    `k8` double MAX NULL COMMENT "",   \
    `k9` float SUM NULL COMMENT "" ,    \
    `k1_hll` hll hll_union COMMENT "",      \
    `k2_hll` hll hll_union COMMENT "",     \
    `k3_hll` hll hll_union COMMENT "",     \
    `k4_hll` hll hll_union COMMENT "",     \
    `k5_hll` hll hll_union COMMENT "",     \
    `k6_hll` hll hll_union COMMENT "",     \
    `k10_hll` hll hll_union COMMENT "",    \
    `k11_hll` hll hll_union COMMENT "",    \
    `k7_hll` hll hll_union COMMENT "",     \
    `k8_hll` hll hll_union COMMENT "",     \
    `k9_hll` hll hll_union COMMENT ""      \
   ) ENGINE=OLAP                       \
    DISTRIBUTED BY HASH(`k11`) BUCKETS 5 \
    PROPERTIES (                        \
   "storage_type" = "COLUMN"            \
   );'






