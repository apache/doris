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

import org.awaitility.Awaitility
import static java.util.concurrent.TimeUnit.SECONDS

suite("test_group_commit_variant") {

    sql "set group_commit=async_mode"

    def testTable = "test_group_commit_variant"

    def waitVisibleRows = { String table, int expected ->
        Awaitility.await().atMost(60, SECONDS).pollInterval(1, SECONDS).until({
            def result = sql "select count(*) from ${table}"
            return result[0][0] == expected
        })
    }

    sql "DROP TABLE IF EXISTS ${testTable}"
    sql """
            CREATE TABLE IF NOT EXISTS ${testTable} (
                k bigint,
                id int,
                v1 variant,
                note varchar(32),
                v2 variant,
                v3 variant,
                v4 variant,
                tail bigint
                )
                UNIQUE KEY(`k`)
                DISTRIBUTED BY HASH (`k`) BUCKETS 1
                properties("replication_num" = "1",
                "disable_auto_compaction" = "false");
        """

    try {
        sql "insert into ${testTable} (k) values (1),(2);"
        sql "insert into ${testTable} (k) values (3),(4);"
    } catch (Exception e) {
        // should not throw exception
        logger.info(e.getMessage())
        assertTrue(False)
    }

    // Non-empty VARIANT values have to survive the WAL round trip: SQL NULL,
    // JSON null, one path holding different types in different rows, and sparse
    // paths that are not materialized as subcolumns.
    sql """
            insert into ${testTable} values
                (11, 101, null, 'sql_null', 'null', '{"p": 1}', '{"n": {"x": 1}}', 11),
                (12, 102, '{"p": 1}', 'mixed_type', '{"p": "s"}', '{"p": [1, 2]}', '{"n": {"x": null}}', 12),
                (13, 103, '{"a.b": 1}', 'sparse', '{"c": {"d": true}}', '{"e": [1, {"f": 2}]}', '{"g": 3.5}', 13)
        """
    waitVisibleRows(testTable, 7)
    order_qt_replayed """
            select k, id, v1, note, v2, v3, v4, tail
            from ${testTable} order by k
        """

    // Replay again from the persistent WAL after a restart; the values above
    // must still be there and only once.
    cluster.restartBackends()
    waitVisibleRows(testTable, 7)
    order_qt_after_restart """
            select k, id, v1, note, v2, v3, v4, tail
            from ${testTable} order by k
        """
}
