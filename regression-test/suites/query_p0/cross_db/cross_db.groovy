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

suite("cross_db") {
    sql """CREATE DATABASE IF NOT EXISTS cross_db_1"""
    sql """CREATE DATABASE IF NOT EXISTS cross_db_2"""
    sql """
    CREATE TABLE IF NOT EXISTS cross_db_1.cnt_table (
        `id` LARGEINT NOT NULL,
        `count` LARGEINT  DEFAULT "0")
    AGGREGATE KEY(`id`, `count`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES ("replication_num" = "1")
    """
    sql """
    CREATE TABLE IF NOT EXISTS cross_db_2.cnt_table (
        `id` LARGEINT NOT NULL,
        `count` LARGEINT  DEFAULT "0")
    AGGREGATE KEY(`id`, `count`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES ("replication_num" = "1")
    """
    sql """
    INSERT INTO cross_db_1.cnt_table VALUES
        (1, 10),
        (2, 32),
        (3, 40),
        (4, 33)
    """

    sql """
    INSERT INTO cross_db_2.cnt_table VALUES
        (1, 10),
        (2, 32),
        (3, 40),
        (4, 40)
    """

    sql """SELECT * FROM cross_db_1.cnt_table,cross_db_2.cnt_table"""

    sql """DROP DATABASE cross_db_1"""
    sql """DROP DATABASE cross_db_2"""
}

