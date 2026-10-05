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

import com.mysql.cj.jdbc.StatementImpl

suite("test_insert_row_count") {
    def dbName = "regression_test_insert_p0"
    def srcTableName = "test_insert_row_count_src"
    def dstTableName = "test_insert_row_count_dst"
    def srcName = dbName + "." + srcTableName
    def dstName = dbName + "." + dstTableName

    // create table
    sql """ drop table if exists ${srcName}; """
    sql """
        CREATE TABLE ${srcName} (
            `key` int NOT NULL,
            `id` int NULL,
            `type` varchar(1) NULL,
            `score` int NULL default "-1"
        ) ENGINE=OLAP
        UNIQUE KEY(`key`)
        DISTRIBUTED BY HASH(`key`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    sql """ drop table if exists ${dstName}; """
    sql """
        CREATE TABLE ${dstName} (
            `id` int NOT NULL,
            `type` varchar(1) NULL,
            `score` int NULL default "-1"
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    // prepare data
    sql """
        INSERT INTO ${srcName} (`key`, `id`, `type`, `score`)
        VALUES (0, 1, "A", 9), (1, 2, NULL, 7), (2, 3, "B", 10), (3, NULL, "N", 6), (4, 5, "X", NULL), (5, NULL, "Z", 8);
    """

    sql """ SET enable_insert_strict = false; """

    sql """ SET enable_strict_consistency_dml = false; """

    qt_insert1 """
        INSERT INTO ${dstName} SELECT `id`, `type`, `score` FROM ${srcName};
    """

    sql """ SET enable_strict_consistency_dml = true; """

    qt_insert2 """
        INSERT INTO ${dstName} SELECT `id`, `type`, `score` FROM ${srcName};
    """

}
