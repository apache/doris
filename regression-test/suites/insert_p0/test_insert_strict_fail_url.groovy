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

suite("test_insert_strict_fail_url") {
    def dbName = "regression_test_insert_p0"
    def srcTableName = "test_insert_strict_fail_url_src"
    def dstTableName = "test_insert_strict_fail_url_dst"
    def srcName = dbName + "." + srcTableName
    def dstName = dbName + "." + dstTableName

    // create table
    sql """ drop table if exists ${srcName}; """
    sql """
        CREATE TABLE ${srcName} (
            `key` int NOT NULL,
            `id` int NULL,
            `score` int NULL default "-1"
        ) ENGINE=OLAP
        DUPLICATE KEY(`key`)
        DISTRIBUTED BY HASH(`key`) BUCKETS 1
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
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    sql """ SET enable_insert_strict = true; """

    // prepare data
    sql """
        INSERT INTO ${srcName} (`key`, `id`, `score`)
        VALUES (0, 1, 9), (1, 2, 7), (2, NULL, 10), (3, NULL, 6), (4, NULL, 5), (5, NULL, 8);
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

    // The error message may vary due to variations in fuzzy execution instance number or batch size.
    // like this:
    // Insert has filtered data in strict mode. url: http://172.16.0.10:8041/api/_load_error_log?
    // file=__shard_303/error_log_insert_stmt_a1ccfb9c67ba40f5-900d0db1d06a19dd_a1ccfb9c67ba40f5_900d0db1d06a19dd
    // or like this:
    // [DATA_QUALITY_ERROR]Encountered unqualified data, stop processing. url: http://172.16.0.10:8041/api/_load_error_log?
    // file=__shard_303/error_log_insert_stmt_a1ccfb9c67ba40f5-900d0db1d06a19dd_a1ccfb9c67ba40f5_900d0db1d06a19dd
    expectExceptionLike({
        sql """
            INSERT INTO ${dstName} SELECT `id`, `score` FROM ${srcName};
        """
    }, "error_log")

    sql """
        INSERT INTO ${srcName} SELECT * FROM ${srcName};
    """

    expectExceptionLike({
        sql """
            INSERT INTO ${dstName} SELECT `id`, `score` FROM ${srcName};
        """
    }, "error_log") 
}
