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

suite("test_skip_agg_table_value_column") {

    def wait_row_count_reported = { db, table, row, column, expected ->
        def result = sql """show frontends;"""
        logger.info("show frontends result origin: " + result)
        def host
        def port
        for (int i = 0; i < result.size(); i++) {
            if (result[i][8] == "true") {
                host = result[i][1]
                port = result[i][4]
            }
        }
        def tokens = context.config.jdbcUrl.split('/')
        def url=tokens[0] + "//" + host + ":" + port
        logger.info("Master url is " + url)
        connect(context.config.jdbcUser, context.config.jdbcPassword, url) {
            sql """use ${db}"""
            result = sql """show frontends;"""
            logger.info("show frontends result master: " + result)
            for (int i = 0; i < 120; i++) {
                Thread.sleep(5000)
                result = sql """SHOW DATA FROM ${table};"""
                logger.info("result " + result)
                if (result[row][column] == expected) {
                    return;
                }
            }
            throw new Exception("Row count report timeout.")
        }

    }

    sql """drop database if exists test_skip_agg_table_value_column"""
    sql """create database test_skip_agg_table_value_column"""
    sql """use test_skip_agg_table_value_column"""
    sql """set global force_sample_analyze=false"""
    sql """set global enable_auto_analyze=false"""

    // Test duplicate table
    sql """CREATE TABLE dup (
            key1 bigint NOT NULL,
            key2 bigint NOT NULL,
            value1 int NOT NULL,
            value2 int NOT NULL,
            value3 int NOT NULL
        )ENGINE=OLAP
        DUPLICATE KEY(`key1`, `key2`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`key1`) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    createMV("create materialized view mv1 as select key1 as a1 from dup;")
    createMV("create materialized view mv2 as select key2 as a2 from dup;")
    createMV("create materialized view mv3 as select key1 as a3, key2 as a4, sum(value1) as a5, max(value2) as a6, min(value3) as a7 from dup group by key1, key2;")
    sql """insert into dup values (1, 2, 3, 4, 5), (1, 2, 3, 4, 5), (10, 20, 30, 40, 50), (10, 20, 30, 40, 50), (100, 200, 300, 400, 500), (1001, 2001, 3001, 4001, 5001);"""

    // Test aggregate table.
    sql """CREATE TABLE agg (
            key1 bigint NOT NULL,
            key2 bigint NOT NULL,
            value1 int SUM NOT NULL,
            value2 int MAX NOT NULL,
            value3 int MIN NOT NULL
        )ENGINE=OLAP
        AGGREGATE KEY(`key1`, `key2`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`key1`) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
    """
    createMV("create materialized view mv1 as select key2 as b1 from agg group by key2;")
    createMV("create materialized view mv3 as select key1 as b2, key2 as b3, sum(value1) as b4, max(value2) as b5, min(value3) as b6 from agg group by key1, key2;")
    createMV("create materialized view mv6 as select key1 as b7, sum(value1) as b8 from agg group by key1;")
    sql """insert into agg values (1, 2, 3, 4, 5), (1, 11, 22, 33, 44), (10, 20, 30, 40, 50), (100, 200, 300, 400, 500), (1001, 2001, 3001, 4001, 5001);"""

    sql """CREATE TABLE agg_partition (
            key1 bigint NOT NULL,
            key2 bigint NOT NULL,
            value1 int SUM NOT NULL,
            value2 int MAX NOT NULL,
            value3 int MIN NOT NULL
        )ENGINE=OLAP
        AGGREGATE KEY(`key1`, `key2`)
        COMMENT "OLAP"
        PARTITION BY RANGE(`key1`)
        (PARTITION p1 VALUES [("0"), ("100")),
        PARTITION p2 VALUES [("100"), ("10000")))
        DISTRIBUTED BY HASH(`key1`) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
    """
    createMV("create materialized view mv1 as select key2 as c1 from agg_partition group by key2;")
    createMV("create materialized view mv3 as select key1 as c2, key2 as c3, sum(value1) as c4, max(value2) as c5, min(value3) as c6 from agg_partition group by key1, key2;")
    createMV("create materialized view mv6 as select key1 as c7, sum(value1) as c8 from agg_partition group by key1;")
    sql """analyze table agg_partition(key1) with sync"""
    sql """insert into agg_partition values (1, 2, 3, 4, 5), (1, 11, 22, 33, 44), (10, 20, 30, 40, 50), (100, 200, 300, 400, 500), (1001, 2001, 3001, 4001, 5001);"""

    // Test unique table
    sql """
        CREATE TABLE uni_mor (
            key1 bigint NOT NULL,
            key2 bigint NOT NULL,
            value1 int NOT NULL,
            value2 int NOT NULL,
            value3 int NOT NULL
        )ENGINE=OLAP
        UNIQUE KEY(`key1`, `key2`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`key1`) BUCKETS 2
        PROPERTIES (
            "enable_unique_key_merge_on_write" = false,
            "replication_num" = "1"
        );
    """
    createMV("create materialized view mv1 as select key1 as d1, key2 as d2 from uni_mor;")
    createMV("create materialized view mv6 as select key1 as d3, key2 as d4, value2 as d5, value3 as d6 from uni_mor;")
    sql """insert into uni_mor values (1, 2, 3, 4, 5), (1, 2, 3, 7, 8), (1, 11, 22, 33, 44), (10, 20, 30, 40, 50), (10, 20, 30, 40, 50), (100, 200, 300, 400, 500), (1001, 2001, 3001, 4001, 5001);"""

    sql """
        CREATE TABLE uni_mor_partition (
            key1 bigint NOT NULL,
            key2 bigint NOT NULL,
            value1 int NOT NULL,
            value2 int NOT NULL,
            value3 int NOT NULL
        )ENGINE=OLAP
        UNIQUE KEY(`key1`, `key2`)
        COMMENT "OLAP"
        PARTITION BY RANGE(`key1`)
        (PARTITION p1 VALUES [("0"), ("100")),
        PARTITION p2 VALUES [("100"), ("10000")))
        DISTRIBUTED BY HASH(`key1`) BUCKETS 2
        PROPERTIES (
            "enable_unique_key_merge_on_write" = false,
            "replication_num" = "1"
        );
    """
    createMV("create materialized view mv1 as select key1 as e1, key2 as e2 from uni_mor_partition;")
    createMV("create materialized view mv6 as select key1 as e3, key2 as e4, value2 as e5, value3 as e6 from uni_mor_partition;")
    sql """analyze table uni_mor_partition(key1) with sync"""
    sql """insert into uni_mor_partition values (1, 2, 3, 4, 5), (1, 2, 3, 7, 8), (1, 11, 22, 33, 44), (10, 20, 30, 40, 50), (10, 20, 30, 40, 50), (100, 200, 300, 400, 500), (1001, 2001, 3001, 4001, 5001);"""

    sql """
        CREATE TABLE uni_mow (
            key1 bigint NOT NULL,
            key2 bigint NOT NULL,
            value1 int NOT NULL,
            value2 int NOT NULL,
            value3 int NOT NULL
        )ENGINE=OLAP
        UNIQUE KEY(`key1`, `key2`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`key1`) BUCKETS 2
        PROPERTIES (
            "enable_unique_key_merge_on_write" = true,
            "replication_num" = "1"
        );
    """
    createMV("create materialized view mv1 as select key1 as f1, key2 as f2 from uni_mow;")
    createMV("create materialized view mv6 as select key1 as f3, key2 as f4, value2 as f5, value3 as f6 from uni_mow;")
    sql """insert into uni_mow values (1, 2, 3, 4, 5), (1, 2, 3, 7, 8), (1, 11, 22, 33, 44), (10, 20, 30, 40, 50), (10, 20, 30, 40, 50), (100, 200, 300, 400, 500), (1001, 2001, 3001, 4001, 5001);"""

    wait_row_count_reported("test_skip_agg_table_value_column", "dup", 0, 4, "6")
    wait_row_count_reported("test_skip_agg_table_value_column", "dup", 1, 4, "6")
    wait_row_count_reported("test_skip_agg_table_value_column", "dup", 2, 4, "6")
    wait_row_count_reported("test_skip_agg_table_value_column", "dup", 3, 4, "4")
    wait_row_count_reported("test_skip_agg_table_value_column", "agg", 0, 4, "5")
    wait_row_count_reported("test_skip_agg_table_value_column", "agg", 1, 4, "5")
    wait_row_count_reported("test_skip_agg_table_value_column", "agg", 2, 4, "5")
    wait_row_count_reported("test_skip_agg_table_value_column", "agg", 3, 4, "4")
    wait_row_count_reported("test_skip_agg_table_value_column", "agg_partition", 0, 4, "5")
    wait_row_count_reported("test_skip_agg_table_value_column", "agg_partition", 1, 4, "5")
    wait_row_count_reported("test_skip_agg_table_value_column", "agg_partition", 2, 4, "5")
    wait_row_count_reported("test_skip_agg_table_value_column", "agg_partition", 3, 4, "4")
    wait_row_count_reported("test_skip_agg_table_value_column", "uni_mor", 0, 4, "5")
    wait_row_count_reported("test_skip_agg_table_value_column", "uni_mor", 1, 4, "5")
    wait_row_count_reported("test_skip_agg_table_value_column", "uni_mor", 2, 4, "5")
    wait_row_count_reported("test_skip_agg_table_value_column", "uni_mor_partition", 0, 4, "5")
    wait_row_count_reported("test_skip_agg_table_value_column", "uni_mor_partition", 1, 4, "5")
    wait_row_count_reported("test_skip_agg_table_value_column", "uni_mor_partition", 2, 4, "5")
    wait_row_count_reported("test_skip_agg_table_value_column", "uni_mow", 0, 4, "5")
    wait_row_count_reported("test_skip_agg_table_value_column", "uni_mow", 1, 4, "5")
    wait_row_count_reported("test_skip_agg_table_value_column", "uni_mow", 2, 4, "5")

    sql """analyze table dup with sync"""
    def result = sql """show column stats dup"""
    assertEquals(12, result.size())
    sql """drop stats dup"""
    result = sql """show column stats dup"""
    assertEquals(0, result.size())
    sql """analyze table dup with sync with sample rows 400000"""
    result = sql """show column stats dup"""
    assertEquals(9, result.size())
    sql """drop stats dup"""
    result = sql """show column stats dup"""
    assertEquals(0, result.size())

    sql """analyze table agg with sync"""
    result = sql """show column stats agg"""
    assertEquals(13, result.size())
    sql """drop stats agg"""
    result = sql """show column stats agg"""
    assertEquals(0, result.size())
    sql """analyze table agg with sync with sample rows 400000"""
    result = sql """show column stats agg"""
    assertEquals(6, result.size())
    sql """drop stats agg"""
    result = sql """show column stats agg"""
    assertEquals(0, result.size())

    result = sql """show table stats agg_partition"""
    assertEquals("true", result[0][6])
    sql """analyze table agg_partition with sync with sample rows 400000"""
    result = sql """show column stats agg_partition"""
    assertEquals(6, result.size())
    result = sql """show table stats agg_partition"""
    assertEquals("false", result[0][6])

    sql """analyze table uni_mor with sync"""
    result = sql """show column stats uni_mor"""
    assertEquals(11, result.size())
    sql """drop stats uni_mor"""
    result = sql """show column stats uni_mor"""
    assertEquals(0, result.size())
    sql """analyze table uni_mor with sync with sample rows 400000"""
    result = sql """show column stats uni_mor"""
    assertEquals(6, result.size())
    sql """drop stats uni_mor"""
    result = sql """show column stats uni_mor"""
    assertEquals(0, result.size())

    result = sql """show table stats uni_mor_partition"""
    assertEquals("true", result[0][6])
    sql """analyze table uni_mor_partition with sync with sample rows 400000"""
    result = sql """show column stats uni_mor_partition"""
    assertEquals(6, result.size())
    result = sql """show table stats uni_mor_partition"""
    assertEquals("false", result[0][6])

    sql """analyze table uni_mow with sync"""
    result = sql """show column stats uni_mow"""
    assertEquals(11, result.size())
    sql """drop stats uni_mow"""
    result = sql """show column stats uni_mow"""
    assertEquals(0, result.size())
    sql """analyze table uni_mow with sync with sample rows 400000"""
    result = sql """show column stats uni_mow"""
    assertEquals(11, result.size())
    sql """drop stats uni_mow"""
    result = sql """show column stats uni_mow"""
    assertEquals(0, result.size())

    // Test PREAGGOPEN hint.
    explain {
        sql("SELECT CONCAT(1744255158798, '-', 1744255158812, '-', 'mv_key2') AS `id`, 0 AS `catalog_id`, 1744255158742 AS `db_id`, 1744255158798 AS `tbl_id`, 1744255158812 AS `idx_id`, 'mv_key2' AS `col_id`, NULL AS `part_id`, 5 AS `row_count`, ROUND(NDV(`mv_key2`) * 1) as `ndv`, ROUND(SUM(CASE WHEN `mv_key2` IS NULL THEN 1 ELSE 0 END) * 1) AS `null_count`, SUBSTRING(CAST('2' AS STRING), 1, 1024) AS `min`, SUBSTRING(CAST('2001' AS STRING), 1, 1024) AS `max`, COUNT(1) * 8 * 1 AS `data_size`, NOW() FROM ( SELECT * FROM `internal`.`test_skip_agg_table_value_column`.`uni_mor` index `mv1`  ) as t /*+PREAGGOPEN*/")
        contains "PREAGGREGATION: ON"
        notContains "PREAGGREGATION: OFF"
    }
    explain {
        sql("SELECT CONCAT(1744255158798, '-', 1744255158812, '-', 'mv_key2') AS `id`, 0 AS `catalog_id`, 1744255158742 AS `db_id`, 1744255158798 AS `tbl_id`, 1744255158812 AS `idx_id`, 'mv_key2' AS `col_id`, NULL AS `part_id`, 5 AS `row_count`, ROUND(NDV(`mv_key2`) * 1) as `ndv`, ROUND(SUM(CASE WHEN `mv_key2` IS NULL THEN 1 ELSE 0 END) * 1) AS `null_count`, SUBSTRING(CAST('2' AS STRING), 1, 1024) AS `min`, SUBSTRING(CAST('2001' AS STRING), 1, 1024) AS `max`, COUNT(1) * 8 * 1 AS `data_size`, NOW() FROM ( SELECT * FROM `internal`.`test_skip_agg_table_value_column`.`uni_mor` index `mv1`  ) as t")
        contains "PREAGGREGATION: OFF"
        notContains "PREAGGREGATION: ON"
    }
    explain {
        sql("SELECT CONCAT(1744255159211, '-', 1744255159224, '-', 'mv_key1') AS `id`, 0 AS `catalog_id`, 1744255159182 AS `db_id`, 1744255159211 AS `tbl_id`, 1744255159224 AS `idx_id`, 'mv_key1' AS `col_id`, NULL AS `part_id`, 5 AS `row_count`, ROUND(NDV(`mv_key1`) * 1) as `ndv`, ROUND(SUM(CASE WHEN `mv_key1` IS NULL THEN 1 ELSE 0 END) * 1) AS `null_count`, SUBSTRING(CAST('1' AS STRING), 1, 1024) AS `min`, SUBSTRING(CAST('1001' AS STRING), 1, 1024) AS `max`, COUNT(1) * 8 * 1 AS `data_size`, NOW() FROM ( SELECT * FROM `internal`.`test_skip_agg_table_value_column`.`agg` index `mv3`  ) as t /*+PREAGGOPEN*/")
        contains "PREAGGREGATION: ON"
        notContains "PREAGGREGATION: OFF"
    }
    explain {
        sql("SELECT CONCAT(1744255159211, '-', 1744255159224, '-', 'mv_key1') AS `id`, 0 AS `catalog_id`, 1744255159182 AS `db_id`, 1744255159211 AS `tbl_id`, 1744255159224 AS `idx_id`, 'mv_key1' AS `col_id`, NULL AS `part_id`, 5 AS `row_count`, ROUND(NDV(`mv_key1`) * 1) as `ndv`, ROUND(SUM(CASE WHEN `mv_key1` IS NULL THEN 1 ELSE 0 END) * 1) AS `null_count`, SUBSTRING(CAST('1' AS STRING), 1, 1024) AS `min`, SUBSTRING(CAST('1001' AS STRING), 1, 1024) AS `max`, COUNT(1) * 8 * 1 AS `data_size`, NOW() FROM ( SELECT * FROM `internal`.`test_skip_agg_table_value_column`.`agg` index `mv3`  ) as t")
        contains "PREAGGREGATION: OFF"
        notContains "PREAGGREGATION: ON"
    }

    sql """drop database if exists test_skip_agg_table_value_column"""
}
