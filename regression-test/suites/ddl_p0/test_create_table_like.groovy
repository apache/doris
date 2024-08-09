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

// this suite is for creating table with timestamp datatype in defferent
// case. For example: 'year' and 'Year' datatype should also be valid in definition


suite("test_create_table_like") {

    sql """DROP TABLE IF EXISTS decimal_test"""
    sql """CREATE TABLE decimal_test
        (
            `name` varchar COMMENT "1m size",
            `id` SMALLINT COMMENT "[-32768, 32767]",
            `timestamp0` decimal null comment "c0",
            `timestamp1` decimal(10, 0) null comment "c1",
            `timestamp2` decimal(10, 1) null comment "c2",
            `timestamp3` decimalv3(10, 0) null comment "c3",
            `timestamp4` decimalv3(10, 1) null comment "c4"
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ('replication_num' = '1')"""
    qt_desc_create_table """desc decimal_test all"""

    sql """DROP TABLE IF EXISTS decimal_test_like"""
    sql """CREATE TABLE decimal_test_like LIKE decimal_test"""

    qt_desc_create_table_like """desc decimal_test_like all"""


    sql """INSERT INTO decimal_test_like
        (`name`, `id`, `timestamp0`, `timestamp1`, `timestamp2`, `timestamp3`, `timestamp4`)
        VALUES ("test1", 1, 123456789, 1234567891, 123456789, 1234567891, 123456789)"""

    qt_select_table_like """select * from decimal_test_like"""

    def resource_name = "test_create_table_like_use_resource"
    def policy_name = "test_create_table_like_use_policy"
    def table1 = "create_table_partion_use_created_policy"
    def table2 = "create_table_partion_like_use_created_policy"

    sql """
        CREATE RESOURCE IF NOT EXISTS $resource_name
        PROPERTIES(
            "type"="s3",
            "AWS_REGION" = "bj",
            "AWS_ENDPOINT" = "bj.s3.comaaaa",
            "AWS_ROOT_PATH" = "path/to/rootaaaa",
            "AWS_SECRET_KEY" = "aaaa",
            "AWS_ACCESS_KEY" = "bbba",
            "AWS_BUCKET" = "test-bucket",
            "s3_validity_check" = "false"
        );
    """
    sql """
        CREATE STORAGE POLICY IF NOT EXISTS $policy_name
        PROPERTIES(
        "storage_resource" = "$resource_name",
        "cooldown_ttl" = "10"
        );
    """

    sql """DROP TABLE IF EXISTS $table1"""
    sql """DROP TABLE IF EXISTS $table2"""

    sql """
        CREATE TABLE $table1
        (
            k1 DATE,
            k2 INT,
            V1 VARCHAR(2048) REPLACE
        ) PARTITION BY RANGE (k1) (
            PARTITION p1 VALUES LESS THAN ("2022-01-01") ("storage_policy" = "$policy_name"),
            PARTITION p2 VALUES LESS THAN ("2022-02-01") ("storage_policy" = "$policy_name")
        ) 
        DISTRIBUTED BY HASH(k2) BUCKETS 1
        PROPERTIES (
            "replication_num"="1"
        );
    """

    sql """
        CREATE TABLE $table2 LIKE $table1
    """

    sql """DROP TABLE IF EXISTS $table1"""
    sql """DROP TABLE IF EXISTS $table2"""
    sql """DROP STORAGE POLICY $policy_name"""
    sql """DROP RESOURCE $resource_name"""
}