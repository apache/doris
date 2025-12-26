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

suite("test_warmup_select") {
    sql """
    drop table if exists lineitem
    """
    sql """
    CREATE TABLE IF NOT EXISTS lineitem (
      l_orderkey    INTEGER NOT NULL,
      l_partkey     INTEGER NOT NULL,
      l_suppkey     INTEGER NOT NULL,
      l_linenumber  INTEGER NOT NULL,
      l_quantity    DECIMALV3(15,2) NOT NULL,
      l_extendedprice  DECIMALV3(15,2) NOT NULL,
      l_discount    DECIMALV3(15,2) NOT NULL,
      l_tax         DECIMALV3(15,2) NOT NULL,
      l_returnflag  CHAR(1) NOT NULL,
      l_linestatus  CHAR(1) NOT NULL,
      l_shipdate    DATE NOT NULL,
      l_commitdate  DATE NOT NULL,
      l_receiptdate DATE NOT NULL,
      l_shipinstruct CHAR(25) NOT NULL,
      l_shipmode     CHAR(10) NOT NULL,
      l_comment      VARCHAR(44) NOT NULL
    )
    DUPLICATE KEY(l_orderkey, l_partkey, l_suppkey, l_linenumber)
    PARTITION BY RANGE(l_shipdate) (
    PARTITION `day_2` VALUES LESS THAN ('2023-12-9'),
    PARTITION `day_3` VALUES LESS THAN ("2023-12-11"),
    PARTITION `day_4` VALUES LESS THAN ("2023-12-30")
    )
    DISTRIBUTED BY HASH(l_orderkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    )
    """

    sql """
    insert into lineitem values
    (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-08', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (2, 4, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-09', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (3, 2, 4, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-10', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (4, 3, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-11', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (5, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-12-12', '2023-12-12', '2023-12-13', 'c', 'd', 'xxxxxxxxx');
    """

    def test_basic_warmup = {
        // Enable file cache for warm up functionality
        sql "set disable_file_cache=false"

        sql "WARM UP SELECT * FROM lineitem"

        sql "WARM UP SELECT l_orderkey, l_discount FROM lineitem"

        sql "WARM UP SELECT l_orderkey, l_discount FROM lineitem WHERE l_quantity > 10"
    }

    def test_warmup_negative_cases = {
        // Enable file cache for warm up functionality
        sql "set disable_file_cache=false"

        // These should fail as warm up select doesn't support these operations
        try {
            sql "WARM UP SELECT * FROM lineitem LIMIT 5"
            assert false : "Expected ParseException for LIMIT clause"
        } catch (Exception e) {
            // Expected to fail
            println "LIMIT clause correctly rejected for WARM UP SELECT"
        }

        try {
            sql "WARM UP SELECT l_shipmode, COUNT(*) FROM lineitem GROUP BY l_shipmode"
            assert false : "Expected ParseException for GROUP BY clause"
        } catch (Exception e) {
            // Expected to fail
            println "GROUP BY clause correctly rejected for WARM UP SELECT"
        }

        try {
            sql "WARM UP SELECT * FROM lineitem t1 JOIN lineitem t2 ON t1.l_orderkey = t2.l_orderkey"
            assert false : "Expected ParseException for JOIN clause"
        } catch (Exception e) {
            // Expected to fail
            println "JOIN clause correctly rejected for WARM UP SELECT"
        }

        try {
            sql "WARM UP SELECT * FROM lineitem UNION SELECT * FROM lineitem"
            assert false : "Expected ParseException for UNION clause"
        } catch (Exception e) {
            // Expected to fail
            println "UNION clause correctly rejected for WARM UP SELECT"
        }
    }

    test_basic_warmup()
    test_warmup_negative_cases()
}