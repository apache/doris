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

suite("test_zone_map_delete") {
    def tableName = "test_zone_map_delete_tbl"

    // comparison predicate
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE IF NOT EXISTS ${tableName} (   `k1` int(11) NULL,   `k2` int(11) NULL,   `v1` int(11) NULL )DUPLICATE KEY(`k1`,k2) DISTRIBUTED BY HASH(`k1`) BUCKETS 1 PROPERTIES("replication_num" = "1");"""
    sql """insert into ${tableName} values(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5);"""
    sql """delete from ${tableName} where v1 > 3;"""
    qt_sql """select * from ${tableName} ORDER BY k1;"""

    // in predicate
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE IF NOT EXISTS ${tableName} (   `k1` int(11) NULL,   `k2` int(11) NULL,   `v1` int(11) NULL )DUPLICATE KEY(`k1`,k2) DISTRIBUTED BY HASH(`k1`) BUCKETS 1 PROPERTIES("replication_num" = "1");"""
    sql """insert into ${tableName} values(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5);"""
    sql """delete from ${tableName} where v1 in (3);"""
    qt_sql """select * from ${tableName} ORDER BY k1;"""

    // null predicate
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE IF NOT EXISTS ${tableName} (   `k1` int(11) NULL,   `k2` int(11) NULL,   `v1` int(11) NULL )DUPLICATE KEY(`k1`,k2) DISTRIBUTED BY HASH(`k1`) BUCKETS 1 PROPERTIES("replication_num" = "1");"""
    sql """insert into ${tableName} values(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5);"""
    sql """delete from ${tableName} where v1 IS NOT NULL;"""
    qt_sql """select * from ${tableName} ORDER BY k1;"""

    // not in predicate
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE IF NOT EXISTS ${tableName} (   `k1` int(11) NULL,   `k2` int(11) NULL,   `v1` int(11) NULL )DUPLICATE KEY(`k1`,k2) DISTRIBUTED BY HASH(`k1`) BUCKETS 1 PROPERTIES("replication_num" = "1");"""
    sql """insert into ${tableName} values(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5);"""
    sql """delete from ${tableName} where v1 not in (3);"""
    qt_sql """select * from ${tableName} ORDER BY k1;"""

    // not in predicate
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE IF NOT EXISTS ${tableName} (   `k1` int(11) NULL,   `k2` int(11) NULL,   `v1` int(11) NULL )DUPLICATE KEY(`k1`,k2) DISTRIBUTED BY HASH(`k1`) BUCKETS 1 PROPERTIES("replication_num" = "1");"""
    sql """insert into ${tableName} values(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5),(1,1,1), (2,2,2),(3,3,3),(4,4,4),(5,5,5);"""
    sql """delete from ${tableName} where v1 not in (0);"""
    qt_sql """select * from ${tableName} ORDER BY k1;"""

    sql """ DROP TABLE IF EXISTS ${tableName} """
}
