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

suite("test_insert_partition_fail_url") {
    def dbName = "regression_test_insert_p0"
    def srcTableName = "test_insert_partition_fail_url_src"
    def dstTableName = "test_insert_partition_fail_url_dst"
    def srcName = dbName + "." + srcTableName
    def dstName = dbName + "." + dstTableName

    // create table
    sql """ drop table if exists ${srcName}; """
    sql """
        CREATE TABLE ${srcName} (
            `id` int NOT NULL,
            `score` int NULL default "-1"
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    sql """ drop table if exists ${dstName}; """
    sql """
        CREATE TABLE ${dstName} (
            `id` int NOT NULL,
            `score` int NULL default "-1"
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        PARTITION BY RANGE(`id`)
        (
            PARTITION `p0` VALUES LESS THAN ("0"),
            PARTITION `p1` VALUES LESS THAN ("2")
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    sql """ SET enable_insert_strict = true; """

    // prepare data
    sql """
        INSERT INTO ${srcName} (`id`, `score`)
        VALUES (0, 9), (1, 7), (2, 10), (3, 6), (4, 5), (5, 8);
    """
    sql """
        INSERT INTO ${srcName} SELECT * FROM ${srcName};
    """
    sql """
        INSERT INTO ${srcName} SELECT * FROM ${srcName};
    """
    sql """
        INSERT INTO ${srcName} SELECT * FROM ${srcName};
    """

    expectExceptionLike({
        sql """
            INSERT INTO ${dstName} SELECT `id`, `score` FROM ${srcName};
        """
    }, "Insert has filtered data in strict mode. url: ")

    sql """
        INSERT INTO ${srcName} SELECT * FROM ${srcName};
    """

    expectExceptionLike({
        sql """
            INSERT INTO ${dstName} SELECT `id`, `score` FROM ${srcName};
        """
    }, "[DATA_QUALITY_ERROR]Encountered unqualified data, stop processing. url: ")
}
