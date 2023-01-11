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

suite("nereids_using_join") {
    sql """
        SET enable_fallback_to_original_planner=false
    """

    sql """
        SET enable_nereids_planner=true
    """

    sql """DROP TABLE IF EXISTS t1"""

    sql """
        CREATE TABLE `t1` (
            `col1` varchar(4) NULL,
            `col2` int(11) NULL,
            `col3` int(11) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`col1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`col3`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """DROP TABLE IF EXISTS t2"""

    sql """
        CREATE TABLE `t2` (
            `col1` varchar(4) NULL,
            `col2` int(11) NULL,
            `col3` int(11) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`col1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`col3`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """INSERT INTO t1 VALUES('1', 1, 1)"""
    sql """INSERT INTO t1 VALUES('2', 2, 1)"""
    sql """INSERT INTO t1 VALUES('3', 3, 1)"""
    sql """INSERT INTO t1 VALUES('4', 4, 1)"""

    sql """INSERT INTO t2 VALUES('1', 1, 1)"""
    sql """INSERT INTO t2 VALUES('2', 2, 1)"""
    sql """INSERT INTO t2 VALUES('6', 3, 1)"""
    sql """INSERT INTO t2 VALUES('7', 4, 1)"""

    order_qt_sql """
        SELECT t1.col1 FROM t1 JOIN t2 USING (col1)
    """

    order_qt_sql """
        SELECT t1.col1 FROM t1 JOIN t2 USING (col1, col2)
    """

}