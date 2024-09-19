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

suite("test_partial_update_mow_with_sync_mv") {

    sql """drop table if exists test_partial_update_mow_with_sync_mv"""

    sql """
        CREATE TABLE `test_partial_update_mow_with_sync_mv` (
          `l_orderkey` BIGINT NULL,
          `l_linenumber` INT NULL,
          `l_partkey` INT NULL,
          `l_suppkey` INT NULL,
          `l_shipdate` DATE not NULL,
          `l_quantity` DECIMAL(15, 2) NULL,
          `l_extendedprice` DECIMAL(15, 2) NULL,
          `l_discount` DECIMAL(15, 2) NULL,
          `l_tax` DECIMAL(15, 2) NULL,
          `l_returnflag` VARCHAR(1) NULL,
          `l_linestatus` VARCHAR(1) NULL,
          `l_commitdate` DATE NULL,
          `l_receiptdate` DATE NULL,
          `l_shipinstruct` VARCHAR(25) NULL,
          `l_shipmode` VARCHAR(10) NULL,
          `l_comment` VARCHAR(44) NULL
        ) 
        unique KEY(l_orderkey, l_linenumber, l_partkey, l_suppkey, l_shipdate)
        DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
        PROPERTIES (
        "replication_num" = "1",
        "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        insert into test_partial_update_mow_with_sync_mv values 
        (null, 1, 2, 3, '2023-10-17', 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
        (1, null, 3, 1, '2023-10-17', 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
        (3, 3, null, 2, '2023-10-19', 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx'),
        (1, 2, 3, null, '2023-10-17', 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
        (2, 3, 2, 1, '2023-10-18', 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
        (3, 1, 1, 2, '2023-10-19', 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', null, 'c', 'd', 'xxxxxxxxx'),
        (1, 3, 2, 2, '2023-10-17', 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
        (null, 1, 2, 3, '2023-10-17', 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
        (1, null, 3, 1, '2023-10-17', 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
        (3, 3, null, 2, '2023-10-19', 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx'),
        (1, 2, 3, null, '2023-10-17', 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy'),
        (2, 3, 2, 1, '2023-10-18', 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy'),
        (3, 1, 1, 2, '2023-10-19', 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', null, 'c', 'd', 'xxxxxxxxx'),
        (1, 3, 2, 2, '2023-10-17', 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy')
    """

    createMV ("""
        CREATE MATERIALIZED VIEW mv
        AS
                select l_orderkey, l_linenumber, l_partkey, l_suppkey, l_shipdate,
            substring(concat(l_returnflag, l_linestatus), 1)
            from test_partial_update_mow_with_sync_mv;
    """)

    for (def use_nereids : [true, false]) {
        if (use_nereids) {
            sql "set enable_nereids_planner=true"
            sql "set enable_fallback_to_original_planner=false"
        } else {
            sql "set enable_nereids_planner=false"
        }

        sql "set enable_unique_key_partial_update=true;"
        sql "sync;"

        test {
            sql """insert into test_partial_update_mow_with_sync_mv(l_orderkey, l_linenumber, l_partkey, l_suppkey, l_shipdate, l_returnflag) values
            (2, 3, 2, 1, '2023-10-18', 'k'); """
            exception "Can't do partial update on merge-on-write Unique table with sync materialized view."
        }
    }

    streamLoad {
        table "test_partial_update_mow_with_sync_mv"
        set 'column_separator', ','
        set 'format', 'csv'
        set 'partial_columns', 'true'
        set 'columns', 'l_orderkey, l_linenumber, l_partkey, l_suppkey, l_shipdate, l_returnflag'

        file 'test_partial_update_mow_with_sync_mv.csv'
        time 10000
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertTrue(json.Message.contains("Can't do partial update on merge-on-write Unique table with sync materialized view."))
        }
    }
}
