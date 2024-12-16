// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases/aggregate
// and modified by Doris.

suite("test_avg") {
    def tableName = "test_avg_tbl"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                c_bigint bigint
            )
            DUPLICATE KEY(c_bigint)
            DISTRIBUTED BY HASH(c_bigint) BUCKETS 1
            PROPERTIES (
              "replication_num" = "1"
            )
        """

    for (i in range(1, 100)) {
        sql """ INSERT INTO ${tableName} values (10000000000000${i}) """
    }
    sql "sync"
    qt_select """select c_bigint from ${tableName} order by c_bigint"""
    qt_sum """ SELECT SUM(c_bigint) FROM ${tableName} """
    qt_count """ SELECT COUNT(c_bigint) FROM ${tableName} """
    qt_avg """ SELECT AVG(c_bigint) FROM ${tableName} """
    sql""" DROP TABLE IF EXISTS ${tableName} """


    sql """ drop table if exists avg_test; """
    sql """
            CREATE TABLE `avg_test` (
            `k1` tinyint(4) NULL,
            `k2` smallint(6) NULL,
            `k3` int(11) NULL,
            `k4` bigint(20) NULL,
            `k5` DECIMAL NULL,
            `k6` char(5) NULL,
            `k10` date NULL,
            `k11` datetime NULL,
            `k7` varchar(20) NULL,
            `k8` double MAX NULL,
            `k9` float SUM NULL
            ) ENGINE=OLAP
            AGGREGATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`, `k6`, `k10`, `k11`, `k7`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`k1`) BUCKETS 5
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );
        """
    qt_select2 """select avg(distinct k2), avg(distinct cast(k4 as largeint)) from avg_test;"""

    sql """ drop table if exists avg_test; """
    qt_select4 """SELECT avg(col) from ( SELECT 0.01 col  union all  select 0.01 col ) t;"""
}
