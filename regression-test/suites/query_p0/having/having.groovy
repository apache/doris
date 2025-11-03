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

suite("having", "query,p0,arrow_flight_sql") {
    sql """DROP TABLE IF EXISTS supplier"""
    sql """CREATE TABLE `supplier` (
            `s_suppkey` int(11) NOT NULL,
            `s_name` varchar(25) NOT NULL,
            `s_address` varchar(40) NOT NULL,
            `s_nationkey` int(11) NOT NULL,
            `s_phone` varchar(15) NOT NULL,
            `s_acctbal` DECIMAL(15, 2) NOT NULL,
            `s_comment` varchar(101) NOT NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`s_suppkey`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 12
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "is_being_synced" = "false",
            "storage_format" = "V2",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "false",
            "enable_single_replica_compaction" = "false"
            ); """
    sql """explain 
        select count(*)
        from supplier s
        group by s_nationkey,s_suppkey
        having  s_nationkey=1 or s_suppkey=1;"""
}
