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
suite("test_ngram_bloom_filter") {
    sql "SHOW ALTER TABLE COLUMN"

    def test_tb = "test_ngram_bloom_filter_tb"
    sql """DROP TABLE IF EXISTS ${test_tb}"""
    sql """CREATE TABLE IF NOT EXISTS ${test_tb} (
            `k1` int(11) NOT NULL,
            `a1` VARCHAR(500) NOT NULL, 
            `a2` string NOT NULL, 
            `a3` text NOT NULL, 
            INDEX idx_ngrambf_a1 (`a1`) USING NGRAM_BF PROPERTIES("gram_size"="3", "bf_size"="512") COMMENT 'a1 ngram_bf index',
            INDEX idx_ngrambf_a2 (`a2`) USING NGRAM_BF PROPERTIES("gram_size"="3", "bf_size"="512") COMMENT 'a2 ngram_bf index',
            INDEX idx_ngrambf_a3 (`a3`) USING NGRAM_BF PROPERTIES("gram_size"="3", "bf_size"="512") COMMENT 'a3 ngram_bf index'
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1"
        )"""

    sql """insert into ${test_tb} values(1, 'aaa.abcd.com', 'aaa.abcd.com', 'aaa.abcd.com')"""
    sql """insert into ${test_tb} values(2, 'aaa.abcd1.com', 'aaa.abcd.1com', 'aaa.abcd.5com')"""
    sql """insert into ${test_tb} values(3, 'aaa.ab1cd1.com', 'aaa.ab2cd2.com', 'aaa.ab3cd3.com')"""

    qt_select_like_a1_tb "SELECT * FROM ${test_tb} where a1 like '%.abcd.%' order by k1"
    qt_select_like_a2_tb "SELECT * FROM ${test_tb} where a2 like '%.abcd.%' order by k1"
    qt_select_like_a3_tb "SELECT * FROM ${test_tb} where a3 like '%.abcd.%' order by k1"

}
