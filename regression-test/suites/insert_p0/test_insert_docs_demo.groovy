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

suite("test_insert_docs_demo") {
    def dbName = "regression_test_insert_p0"
    def srcTableName = "test_insert_docs_demo_src"
    def dstTableName = "test_insert_docs_demo_dst"
    def srcName = dbName + "." + srcTableName
    def dstName = dbName + "." + dstTableName

    // create table
    sql """ drop table if exists ${srcName}; """
    sql """
        CREATE TABLE ${srcName}(
            user_id            BIGINT       NOT NULL COMMENT "user id",
            name               VARCHAR(20)           COMMENT "name",
            age                INT                   COMMENT "age"
        ) DUPLICATE KEY(user_id)
        DISTRIBUTED BY HASH(user_id) BUCKETS 10;
    """

    sql """ drop table if exists ${dstName}; """
    sql """
        CREATE TABLE ${dstName}(
            user_id            BIGINT       NOT NULL COMMENT "user id",
            name               VARCHAR(20)           COMMENT "name",
            age                INT                   COMMENT "age"
        ) DUPLICATE KEY(user_id)
        DISTRIBUTED BY HASH(user_id) BUCKETS 10;
    """

    sql """ SET enable_insert_strict = true; """

    // insert into values
    sql """
        INSERT INTO ${srcName} (user_id, name, age)
        VALUES (1, "Emily", 25),
            (2, "Benjamin", 35),
            (3, "Olivia", 28),
            (4, "Alexander", 60),
            (5, "Ava", 17);
    """

    // insert into
    sql """
        INSERT INTO ${dstName}
        SELECT * FROM ${srcName} WHERE age < 30;
    """

    qt_insert """
        SELECT * FROM ${dstName} ORDER BY age;
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
            INSERT INTO ${srcName} (user_id, name, age)
            VALUES (1, "Emily", 25),
                (2, "Benjamin", 35),
                (3, "Olivia", 28),
                (NULL, "Alexander", 60),
                (5, "Ava", 17);
        """
    }, "error_log")

    sql """ SET enable_insert_strict = false; """

    sql """
        INSERT INTO ${srcName} (user_id, name, age)
        VALUES (1, "Emily", 25),
            (2, "Benjamin", 35),
            (3, "Olivia", 28),
            (NULL, "Alexander", 60),
            (5, "Ava", 17);
    """

    sql """ SET insert_max_filter_ratio = 0.1 """

    expectExceptionLike({
        sql """
            INSERT INTO ${srcName} (user_id, name, age)
            VALUES (1, "Emily", 25),
                (2, "Benjamin", 35),
                (3, "Olivia", 28),
                (NULL, "Alexander", 60),
                (5, "Ava", 17);
        """
    }, "Insert has too many filtered data 1/5 insert_max_filter_ratio is 0.1")
}
