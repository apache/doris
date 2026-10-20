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

suite("test_sql_digest_insert_select", "nonConcurrent") {
    sql "set global enable_audit_plugin = true"

    // Audit events are persisted asynchronously. Flush and poll until the expected row shows up
    // instead of waiting for a fixed amount of time.
    def waitForAuditRow = { String query ->
        List<List<Object>> rows = []
        awaitUntil(60, 1) {
            sql """call flush_audit_log()"""
            rows = sql query
            return !rows.isEmpty()
        }
        return rows[0]
    }

    sql "drop table if exists sql_digest_insert_select_src"
    sql """
        CREATE TABLE `sql_digest_insert_select_src` (
          `id` bigint,
          `name` varchar(32)
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
          "replication_allocation" = "tag.location.default: 1"
        )
    """
    sql "drop table if exists sql_digest_insert_select_dst"
    sql """
        CREATE TABLE `sql_digest_insert_select_dst` (
          `id` bigint,
          `name` varchar(32)
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
          "replication_allocation" = "tag.location.default: 1"
        )
    """
    sql "insert into sql_digest_insert_select_src values (1, 'a'), (2, 'b')"
    sql "truncate table __internal_schema.audit_log"

    setFeConfigTemporary([sql_digest_generation_threshold_ms: 0]) {
        // INSERT INTO ... SELECT must carry a SELECT-only digest while remaining a non-query statement
        sql "insert into sql_digest_insert_select_dst select * from sql_digest_insert_select_src"

        def expectedDigest = sql("select md5('SELECT * FROM sql_digest_insert_select_src')")[0][0]
        def insertRow = waitForAuditRow("""
            SELECT CAST(is_query AS INT) AS is_query, stmt_type, sql_digest
            FROM __internal_schema.audit_log
            WHERE stmt LIKE 'insert into sql_digest_insert_select_dst select%'
            ORDER BY `time` DESC
            LIMIT 1
        """)
        assertEquals(0, insertRow[0])
        assertEquals("INSERT", insertRow[1])
        assertEquals(expectedDigest, insertRow[2])

        // INSERT ... VALUES has no SELECT part, so sql_digest stays empty
        sql "insert into sql_digest_insert_select_dst values (3, 'c')"

        def valuesRow = waitForAuditRow("""
            SELECT CAST(is_query AS INT) AS is_query, stmt_type, sql_digest
            FROM __internal_schema.audit_log
            WHERE stmt LIKE 'insert into sql_digest_insert_select_dst values%'
            ORDER BY `time` DESC
            LIMIT 1
        """)
        assertEquals(0, valuesRow[0])
        assertEquals("INSERT", valuesRow[1])
        assertTrue(valuesRow[2] == null || valuesRow[2] == "")

        // A WITH wrapper must not turn a VALUES source into a fingerprintable SELECT.
        sql "with c as (select * from sql_digest_insert_select_src) insert into sql_digest_insert_select_dst values (4, 'd')"
        def cteValuesRow = waitForAuditRow("""
            SELECT CAST(is_query AS INT) AS is_query, stmt_type, sql_digest
            FROM __internal_schema.audit_log
            WHERE stmt LIKE 'with c as (select * from sql_digest_insert_select_src) insert into sql_digest_insert_select_dst values (4%'
            ORDER BY `time` DESC
            LIMIT 1
        """)
        assertEquals(0, cteValuesRow[0])
        assertEquals("INSERT", cteValuesRow[1])
        assertTrue(cteValuesRow[2] == null || cteValuesRow[2] == "")

        sql "insert into sql_digest_insert_select_dst with c as (select * from sql_digest_insert_select_src) values (5, 'e')"
        def localCteValuesRow = waitForAuditRow("""
            SELECT CAST(is_query AS INT) AS is_query, stmt_type, sql_digest
            FROM __internal_schema.audit_log
            WHERE stmt LIKE 'insert into sql_digest_insert_select_dst with c as (select * from sql_digest_insert_select_src) values (5%'
            ORDER BY `time` DESC
            LIMIT 1
        """)
        assertEquals(0, localCteValuesRow[0])
        assertEquals("INSERT", localCteValuesRow[1])
        assertTrue(localCteValuesRow[2] == null || localCteValuesRow[2] == "")
    }

    // DML digest generation keeps the same threshold gate as query digest generation.
    setFeConfigTemporary([sql_digest_generation_threshold_ms: 100000]) {
        sql "insert into sql_digest_insert_select_dst select * from sql_digest_insert_select_src where id > 100"

        def overThresholdRow = waitForAuditRow("""
            SELECT CAST(is_query AS INT) AS is_query, stmt_type, sql_digest
            FROM __internal_schema.audit_log
            WHERE stmt LIKE 'insert into sql_digest_insert_select_dst select * from sql_digest_insert_select_src where id > 100%'
            ORDER BY `time` DESC
            LIMIT 1
        """)
        assertEquals(0, overThresholdRow[0])
        assertEquals("INSERT", overThresholdRow[1])
        assertTrue(overThresholdRow[2] == null || overThresholdRow[2] == "")
    }
}
