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


suite("test_drop_index_on_partition", "inverted_index") {
    def timeout = 300000
    def delta_time = 1000

    def wait_for_build_index_on_partition_finish = { table_name, OpTimeout ->
        def useTime = 0
        for (int t = delta_time; t <= OpTimeout; t += delta_time) {
            def alter_res = sql """SHOW BUILD INDEX WHERE TableName = "${table_name}";"""
            def expected_finished_num = alter_res.size();
            def finished_num = 0;
            for (int i = 0; i < expected_finished_num; i++) {
                logger.info(table_name + " build index job state: " + alter_res[i][7] + " idx: " + i)
                if (alter_res[i][7] == "FINISHED") {
                    ++finished_num;
                }
            }
            if (finished_num == expected_finished_num) {
                logger.info(table_name + " all build index jobs finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_build_index_on_partition_finish timeout")
    }

    // case 1: basic DROP INDEX ON PARTITION
    def tableName1 = "test_drop_idx_on_partition_basic"
    sql "DROP TABLE IF EXISTS ${tableName1}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName1} (
            k1 int NOT NULL,
            v1 varchar(50),
            v2 string
        )
        DUPLICATE KEY(`k1`)
        PARTITION BY RANGE(`k1`) (
            PARTITION p1 VALUES LESS THAN (100),
            PARTITION p2 VALUES LESS THAN (200),
            PARTITION p3 VALUES LESS THAN (300)
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS 3
        properties("replication_num" = "1");
    """

    sql "INSERT INTO ${tableName1} VALUES (10, 'hello world', 'foo bar'), (50, 'test data', 'baz qux')"
    sql "INSERT INTO ${tableName1} VALUES (110, 'hello doris', 'abc def'), (150, 'test query', 'ghi jkl')"
    sql "INSERT INTO ${tableName1} VALUES (210, 'hello index', 'mno pqr'), (250, 'test drop', 'stu vwx')"

    // create inverted index
    sql "CREATE INDEX idx_v2 ON ${tableName1}(v2) USING INVERTED"
    wait_for_last_build_index_finish(tableName1, timeout)

    // verify index exists
    def show_idx = sql "SHOW INDEX FROM ${tableName1}"
    logger.info("show index: " + show_idx)
    assertTrue(show_idx.toString().contains("idx_v2"))

    // drop index on partition p1 only
    sql "DROP INDEX idx_v2 ON ${tableName1} PARTITION (p1)"
    wait_for_build_index_on_partition_finish(tableName1, timeout)

    // verify index definition still exists in table schema
    show_idx = sql "SHOW INDEX FROM ${tableName1}"
    logger.info("show index after drop on partition: " + show_idx)
    assertTrue(show_idx.toString().contains("idx_v2"))

    // query with fallback should work
    sql """set enable_fallback_on_missing_inverted_index = true"""
    order_qt_select1 """SELECT * FROM ${tableName1} WHERE v2 = 'foo bar' ORDER BY k1"""
    order_qt_select2 """SELECT * FROM ${tableName1} WHERE v2 = 'abc def' ORDER BY k1"""

    // case 2: drop index on multiple partitions
    def tableName2 = "test_drop_idx_on_multi_partitions"
    sql "DROP TABLE IF EXISTS ${tableName2}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName2} (
            k1 int NOT NULL,
            v1 varchar(50)
        )
        DUPLICATE KEY(`k1`)
        PARTITION BY RANGE(`k1`) (
            PARTITION p1 VALUES LESS THAN (100),
            PARTITION p2 VALUES LESS THAN (200),
            PARTITION p3 VALUES LESS THAN (300)
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS 3
        properties("replication_num" = "1");
    """

    sql "INSERT INTO ${tableName2} VALUES (10, 'hello'), (110, 'world'), (210, 'doris')"

    sql "CREATE INDEX idx_v1 ON ${tableName2}(v1) USING INVERTED"
    wait_for_last_build_index_finish(tableName2, timeout)

    // drop index on p1 and p2
    sql "DROP INDEX idx_v1 ON ${tableName2} PARTITIONS (p1, p2)"
    wait_for_build_index_on_partition_finish(tableName2, timeout)

    // index definition should still exist
    show_idx = sql "SHOW INDEX FROM ${tableName2}"
    assertTrue(show_idx.toString().contains("idx_v1"))

    // queries should work with fallback
    sql """set enable_fallback_on_missing_inverted_index = true"""
    order_qt_select3 """SELECT * FROM ${tableName2} ORDER BY k1"""

    // case 3: error - non-partitioned table
    def tableName3 = "test_drop_idx_on_partition_non_partitioned"
    sql "DROP TABLE IF EXISTS ${tableName3}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName3} (
            k1 int NOT NULL,
            v1 varchar(50)
        )
        DUPLICATE KEY(`k1`)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 3
        properties("replication_num" = "1");
    """
    sql "INSERT INTO ${tableName3} VALUES (1, 'test')"
    sql "CREATE INDEX idx_v1 ON ${tableName3}(v1) USING INVERTED"
    wait_for_last_build_index_finish(tableName3, timeout)

    test {
        sql "DROP INDEX idx_v1 ON ${tableName3} PARTITION (p1)"
        exception "is not partitioned"
    }

    // case 4: error - non-inverted index type
    def tableName4 = "test_drop_idx_on_partition_non_inverted"
    sql "DROP TABLE IF EXISTS ${tableName4}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName4} (
            k1 int NOT NULL,
            v1 varchar(50)
        )
        DUPLICATE KEY(`k1`)
        PARTITION BY RANGE(`k1`) (
            PARTITION p1 VALUES LESS THAN (100),
            PARTITION p2 VALUES LESS THAN (200)
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS 3
        properties("replication_num" = "1");
    """
    sql "INSERT INTO ${tableName4} VALUES (10, 'test'), (110, 'data')"
    sql "CREATE INDEX idx_ngram ON ${tableName4}(v1) USING NGRAM_BF PROPERTIES(\"gram_size\"=\"3\", \"bf_size\"=\"1024\")"
    if (isCloudMode()) {
        build_index_on_table("idx_ngram", tableName4)
        wait_for_last_build_index_finish(tableName4, timeout)
    } else {
        wait_for_last_col_change_finish(tableName4, timeout)
    }

    test {
        sql "DROP INDEX idx_ngram ON ${tableName4} PARTITION (p1)"
        exception "Only inverted index supports DROP INDEX ON PARTITION"
    }

    // case 5: error - non-existent partition
    test {
        sql "DROP INDEX idx_v2 ON ${tableName1} PARTITION (non_existent_partition)"
        exception "does not exist"
    }

    // case 6: error - non-existent index
    test {
        sql "DROP INDEX non_existent_idx ON ${tableName1} PARTITION (p1)"
        exception "does not exist"
    }

    // case 7: IF EXISTS with non-existent index should succeed silently
    sql "DROP INDEX IF EXISTS non_existent_idx ON ${tableName1} PARTITION (p1)"

    // case 8: ALTER TABLE form
    def tableName5 = "test_drop_idx_on_partition_alter"
    sql "DROP TABLE IF EXISTS ${tableName5}"
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName5} (
            k1 int NOT NULL,
            v1 string
        )
        DUPLICATE KEY(`k1`)
        PARTITION BY RANGE(`k1`) (
            PARTITION p1 VALUES LESS THAN (100),
            PARTITION p2 VALUES LESS THAN (200)
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS 3
        properties("replication_num" = "1");
    """
    sql "INSERT INTO ${tableName5} VALUES (10, 'hello world'), (110, 'test data')"

    sql "CREATE INDEX idx_v1 ON ${tableName5}(v1) USING INVERTED"
    wait_for_last_build_index_finish(tableName5, timeout)

    // use ALTER TABLE form
    sql "ALTER TABLE ${tableName5} DROP INDEX idx_v1 PARTITION (p1)"
    wait_for_build_index_on_partition_finish(tableName5, timeout)

    // index definition should still exist
    show_idx = sql "SHOW INDEX FROM ${tableName5}"
    assertTrue(show_idx.toString().contains("idx_v1"))

    sql """set enable_fallback_on_missing_inverted_index = true"""
    order_qt_select4 """SELECT * FROM ${tableName5} ORDER BY k1"""
}
