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
suite("test_bloom_filter") {
    // todo: test bloom filter, such alter table bloom filter, create table with bloom filter
    sql "SHOW ALTER TABLE COLUMN"

    // bloom filter index for ARRAY column
    def test_tb = "test_array_bloom_filter_tb"
    sql """DROP TABLE IF EXISTS ${test_tb}"""
    test {
        sql """CREATE TABLE IF NOT EXISTS ${test_tb} (
                `k1` int(11) NOT NULL,
                `a1` array<boolean> NOT NULL
                ) ENGINE=OLAP
                DUPLICATE KEY(`k1`)
                DISTRIBUTED BY HASH(`k1`) BUCKETS 5
                PROPERTIES (
                    "replication_num" = "1",
                    "bloom_filter_columns" = "k1,a1"
            )"""
        exception "not supported in bloom filter index"
    }

    sql """CREATE TABLE IF NOT EXISTS ${test_tb} (
            `k1` int(11) NOT NULL,
            `a1` array<boolean> NOT NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 5
            PROPERTIES (
                "replication_num" = "1",
                "bloom_filter_columns" = "k1"
        )"""
    test {
        sql """ALTER TABLE ${test_tb} SET("bloom_filter_columns" = "k1,a1")"""
        exception "not supported in bloom filter index"
    }
}
