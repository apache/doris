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

    def get_build_index_job_count = { table_name ->
        def res = sql """SHOW BUILD INDEX WHERE TableName = "${table_name}";"""
        return res.size()
    }

    def wait_for_build_index_on_partition_finish = { table_name, expected_job_count, OpTimeout ->
        def useTime = 0
        for (int t = delta_time; t <= OpTimeout; t += delta_time) {
            def alter_res = sql """SHOW BUILD INDEX WHERE TableName = "${table_name}";"""
            if (alter_res.size() < expected_job_count) {
                useTime = t
                sleep(delta_time)
                continue
            }
            def finished_num = 0
            for (int i = 0; i < alter_res.size(); i++) {
                logger.info(table_name + " build index job state: " + alter_res[i][7] + " idx: " + i)
                if (alter_res[i][7] == "FINISHED") {
                    ++finished_num
                }
            }
            if (finished_num == alter_res.size()) {
                logger.info(table_name + " all build index jobs finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_build_index_on_partition_finish timeout")
    }

    // case 1: basic DROP INDEX ON PARTITION with job count verification
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

    // record job count before DROP
    def job_count_before = get_build_index_job_count(tableName1)
    logger.info("job count before drop: " + job_count_before)

    // drop index on partition p1 only — should create new job(s)
    sql "DROP INDEX idx_v2 ON ${tableName1} PARTITION (p1)"

    def job_count_after = get_build_index_job_count(tableName1)
    logger.info("job count after drop: " + job_count_after)
    assertTrue(job_count_after > job_count_before,
            "DROP INDEX ON PARTITION should create new build index jobs, " +
            "before: ${job_count_before}, after: ${job_count_after}")

    wait_for_build_index_on_partition_finish(tableName1, job_count_after, timeout)

    // verify index definition still exists in table schema
    show_idx = sql "SHOW INDEX FROM ${tableName1}"
    logger.info("show index after drop on partition: " + show_idx)
    assertTrue(show_idx.toString().contains("idx_v2"))

    // query with fallback=true should return correct results on all partitions
    sql """set enable_fallback_on_missing_inverted_index = true"""
    def result1 = sql """SELECT * FROM ${tableName1} WHERE v2 = 'foo bar' ORDER BY k1"""
    assertEquals(1, result1.size())
    assertEquals(10, result1[0][0])

    def result2 = sql """SELECT * FROM ${tableName1} WHERE v2 = 'abc def' ORDER BY k1"""
    assertEquals(1, result2.size())
    assertEquals(110, result2[0][0])

    // case 2: drop index on multiple partitions with job count verification
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

    job_count_before = get_build_index_job_count(tableName2)

    // drop index on p1 and p2 — should create at least 2 new jobs
    sql "DROP INDEX idx_v1 ON ${tableName2} PARTITIONS (p1, p2)"

    job_count_after = get_build_index_job_count(tableName2)
    logger.info("multi-partition drop: job count before=${job_count_before}, after=${job_count_after}")
    assertTrue(job_count_after >= job_count_before + 2,
            "DROP INDEX ON 2 PARTITIONS should create at least 2 new jobs")

    wait_for_build_index_on_partition_finish(tableName2, job_count_after, timeout)

    // index definition should still exist
    show_idx = sql "SHOW INDEX FROM ${tableName2}"
    assertTrue(show_idx.toString().contains("idx_v1"))

    // queries should return correct results with fallback
    sql """set enable_fallback_on_missing_inverted_index = true"""
    def result3 = sql """SELECT * FROM ${tableName2} ORDER BY k1"""
    assertEquals(3, result3.size())

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
    wait_for_last_col_change_finish(tableName4, timeout)

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

    // case 8: error - PARTITIONS (*) not supported
    test {
        sql "DROP INDEX idx_v2 ON ${tableName1} PARTITIONS (*)"
        exception "PARTITIONS (*) is not supported"
    }

    // case 9: error - TEMPORARY PARTITION not supported
    test {
        sql "DROP INDEX idx_v2 ON ${tableName1} TEMPORARY PARTITION (p1)"
        exception "does not support temporary partitions"
    }

    // case 10: ALTER TABLE form with job count verification
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

    job_count_before = get_build_index_job_count(tableName5)

    // use ALTER TABLE form
    sql "ALTER TABLE ${tableName5} DROP INDEX idx_v1 PARTITION (p1)"

    job_count_after = get_build_index_job_count(tableName5)
    assertTrue(job_count_after > job_count_before,
            "ALTER TABLE DROP INDEX ON PARTITION should create new jobs")

    wait_for_build_index_on_partition_finish(tableName5, job_count_after, timeout)

    // index definition should still exist
    show_idx = sql "SHOW INDEX FROM ${tableName5}"
    assertTrue(show_idx.toString().contains("idx_v1"))

    // query all data with fallback
    sql """set enable_fallback_on_missing_inverted_index = true"""
    def result4 = sql """SELECT * FROM ${tableName5} ORDER BY k1"""
    assertEquals(2, result4.size())
}
