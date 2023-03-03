/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

suite("aggregate_output_null") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    sql "DROP TABLE IF EXISTS t1;"
    sql """
        CREATE TABLE IF NOT EXISTS `t1`
        (
           
            `a`                varchar(255) NULL ,
            `b`                varchar(255) NULL ,
            `c`                varchar(255) NULL ,
            `d`                int(11) NULL 
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(`c`) BUCKETS 3
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """DROP TABLE IF EXISTS t2;"""
    sql """
        CREATE TABLE IF NOT EXISTS `t2`
        (
            `e` varchar(11) NOT NULL ,
            `a`      varchar(6)  NOT NULL 
        ) ENGINE=OLAP
        UNIQUE KEY(`e`, `a`)
        DISTRIBUTED BY HASH(`e`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """INSERT INTO t1 VALUES("test", "test", "test",1 );"""
    sql """INSERT INTO t1 VALUES("test", "test", "test",1 );"""
    sql """INSERT INTO t1 VALUES("test", "test", "test",1 );"""
    sql """INSERT INTO t1 VALUES("test", "test", "test",1 );"""
    sql """INSERT INTO t1 VALUES("test", "test", "test",1 );"""
    sql """INSERT INTO t1 VALUES("test", "test", "test",1 );"""
    sql """INSERT INTO t1 VALUES("test", "test", "test",1 );"""
    sql """INSERT INTO t1 VALUES("test", "test", "test",1 );"""
    sql """INSERT INTO t1 VALUES("test", "test", "test",1 );"""
    sql """INSERT INTO t1 VALUES("test", "test", "test",1 );"""

    sql """
        INSERT INTO t2 (e,a) VALUES
           ('','VZL8y'),
           ('18hurd','EH'),
           ('3','54m'),
           ('6KldLAISE6N','wI5WN'),
           ('AswEmp','1q'),
           ('BmT4OGW','O'),
           ('P6zKDh','pw'),
           ('iqQ0NzI','Av6BE'),
           ('oXtDwu','BMIG3U'),
           ('z178NhOZ','b');
    """

    qt_select """
        SELECT
            t2.a,
            t1.c,
            sum(d)
        FROM
            t1
            LEFT JOIN t2 ON t2.e = t1.b
        GROUP BY
            t2.a,
            t1.c;
    """

    sql "DROP TABLE t1"
    sql "DROP TABLE t2"
}