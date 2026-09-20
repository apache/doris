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
}
