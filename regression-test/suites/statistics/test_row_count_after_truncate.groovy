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

suite("test_row_count_after_truncate") {
    sql """drop database if exists test_row_count_after_truncate"""
    sql """create database test_row_count_after_truncate"""
    sql """use test_row_count_after_truncate"""

    sql """CREATE TABLE test_row_count_after_truncate_tbl (
            k INT NOT NULL,
            v INT NULL
        ) ENGINE = OLAP
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    sql """INSERT INTO test_row_count_after_truncate_tbl VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)"""

    // TRUNCATE TABLE removes all the data. The backends report the row count of the new tablets with
    // a delay, and the rows loaded right after the truncation are not visible in that report either,
    // so the row count of the table can only come from the stats record kept by TRUNCATE TABLE.
    sql """TRUNCATE TABLE test_row_count_after_truncate_tbl"""
    sql """INSERT INTO test_row_count_after_truncate_tbl VALUES (1, 1), (2, 2), (3, 3)"""

    // The scan must report the rows of the table instead of -1 (which is planned as 1 row), i.e. the stats
    // record kept by TRUNCATE TABLE has accumulated the rows loaded after it.
    explain {
        sql """SELECT * FROM test_row_count_after_truncate_tbl"""
        contains "cardinality=3, "
    }

    // The same from the stats record itself. The backend reports the new tablets with a delay, so the
    // record is the only place the rows loaded so far are visible.
    String updatedRows = ""
    for (int i = 0; i < 30; i++) {
        def stats = sql """SHOW TABLE STATS test_row_count_after_truncate_tbl"""
        updatedRows = stats[0][0]
        if (updatedRows == "3") {
            break
        }
        Thread.sleep(1000)
    }
    assertEquals("3", updatedRows)

    // A scan which selects an aggregate rollup index must report the row count of that index. The rows
    // loaded after the truncation are only known for the base index, they must not be added to an index
    // which aggregates them, otherwise the rollup is estimated with the rows of the whole table.
    sql """DROP TABLE IF EXISTS test_row_count_after_truncate_aggr"""
    sql """CREATE TABLE test_row_count_after_truncate_aggr (
            k1 INT NOT NULL,
            k2 INT NOT NULL,
            v INT SUM NOT NULL
        ) ENGINE = OLAP
        AGGREGATE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    sql """ALTER TABLE test_row_count_after_truncate_aggr ADD ROLLUP r1 (k1, v)"""
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE ROLLUP WHERE TableName='test_row_count_after_truncate_aggr' ORDER BY CreateTime DESC LIMIT 1"""
        time 600
    }
    sql """INSERT INTO test_row_count_after_truncate_aggr SELECT 1, number, 1 FROM numbers("number" = "100")"""
    sql """TRUNCATE TABLE test_row_count_after_truncate_aggr"""
    sql """INSERT INTO test_row_count_after_truncate_aggr SELECT 1, number, 1 FROM numbers("number" = "100")"""

    // 100 rows are loaded into the base index, the rollup aggregates them into a single row. Whether the
    // backends have reported the new tablets or not, the rollup scan reports its own row count and the
    // base index scan reports the 100 loaded rows.
    explain {
        sql """SELECT k1, sum(v) FROM test_row_count_after_truncate_aggr GROUP BY k1"""
        contains "(r1)"
        contains "cardinality=1, "
    }
    explain {
        sql """SELECT k2 FROM test_row_count_after_truncate_aggr"""
        contains "cardinality=100, "
    }

    // A rollup of a duplicate key table which keeps one row per base row has the same row count as the base
    // index, so the rows loaded after the truncation are its rows as well. The rollup drops a key column
    // (k2), which is what makes it a rollup the analyzer accepts: a projection whose columns are a prefix of
    // the base table's columns is rejected as useless for a duplicate key table.
    sql """DROP TABLE IF EXISTS test_row_count_after_truncate_dup_rollup"""
    sql """CREATE TABLE test_row_count_after_truncate_dup_rollup (
            k1 INT NOT NULL,
            k2 INT NOT NULL,
            v INT NULL
        ) ENGINE = OLAP
        DUPLICATE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    sql """ALTER TABLE test_row_count_after_truncate_dup_rollup ADD ROLLUP r_dup (k1, v)"""
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE ROLLUP WHERE TableName='test_row_count_after_truncate_dup_rollup' ORDER BY CreateTime DESC LIMIT 1"""
        time 600
    }
    sql """INSERT INTO test_row_count_after_truncate_dup_rollup VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3)"""
    sql """TRUNCATE TABLE test_row_count_after_truncate_dup_rollup"""
    sql """INSERT INTO test_row_count_after_truncate_dup_rollup VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3)"""

    // The scan of the rollup reports the rows loaded into the table: it keeps one row per base row, so
    // neither its own -1 (the backends have not reported the new rollup tablets yet) nor 1 is right.
    explain {
        sql """SELECT k1, v FROM test_row_count_after_truncate_dup_rollup INDEX r_dup"""
        contains "(r_dup)"
        contains "cardinality=3, "
    }
}
