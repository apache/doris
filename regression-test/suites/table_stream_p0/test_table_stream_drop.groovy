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

// Cover DROP STREAM semantics: drop existing, DROP IF EXISTS silent,
// dropping a non-existing name without IF EXISTS errors, and DROP ... FORCE.
suite("test_table_stream_drop") {
    if (isCloudMode()) {
        return
    }

    sql "DROP DATABASE IF EXISTS test_table_stream_drop_db"
    sql "CREATE DATABASE test_table_stream_drop_db"
    sql "USE test_table_stream_drop_db"

    sql """
        CREATE TABLE base_drop (
            k1 INT, v1 INT
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        )
    """

    // 1. DROP STREAM on an existing stream succeeds.
    sql """
        CREATE STREAM s_drop_exist ON TABLE base_drop
        PROPERTIES ("type" = "append_only")
    """
    sql "INSERT INTO base_drop VALUES (1, 10)"
    sql "sync"
    sleep(1200)
    assertEquals(1, sql("SELECT k1, v1 FROM s_drop_exist").size())
    sql "DROP STREAM s_drop_exist"

    assertEquals(0, sql("""
        SELECT COUNT(*) FROM information_schema.table_streams
        WHERE DB_NAME = 'test_table_stream_drop_db'
          AND STREAM_NAME = 's_drop_exist'
    """)[0][0] as int)
    assertEquals(0, sql("""
        SELECT COUNT(*) FROM information_schema.table_stream_consumption
        WHERE DB_NAME = 'test_table_stream_drop_db'
          AND STREAM_NAME = 's_drop_exist'
    """)[0][0] as int)

    // Dropping a stream must not affect its base table or later writes.
    sql "INSERT INTO base_drop VALUES (2, 20)"
    sql "sync"
    assertEquals([["1", "10"], ["2", "20"]],
            sql("SELECT k1, v1 FROM base_drop ORDER BY k1").collect {
                row -> row.collect { value -> value.toString() }
            })

    // The dropped name can be reused and starts from its own creation offset.
    sql """
        CREATE STREAM s_drop_exist ON TABLE base_drop
        PROPERTIES (
            "type" = "append_only",
            "show_initial_rows" = "false"
        )
    """
    assertEquals(0, sql("SELECT k1, v1 FROM s_drop_exist").size())
    sql "INSERT INTO base_drop VALUES (3, 30)"
    sql "sync"
    sleep(1200)
    assertEquals([["3", "30"]], sql("SELECT k1, v1 FROM s_drop_exist ORDER BY k1").collect {
        row -> row.collect { value -> value.toString() }
    })
    sql "DROP STREAM s_drop_exist"

    // dropping the same name again (no IF EXISTS) must fail.
    test {
        sql "DROP STREAM s_drop_exist"
        exception "Unknown table"
    }

    // 2. DROP STREAM IF EXISTS on a non-existing stream is a silent no-op.
    sql "DROP STREAM IF EXISTS s_never_exists"

    // 3. DROP STREAM without IF EXISTS on a non-existing stream errors.
    test {
        sql "DROP STREAM s_never_exists"
        exception "Unknown table"
    }

    // 4. DROP STREAM FORCE on an existing stream succeeds.
    sql """
        CREATE STREAM s_drop_force ON TABLE base_drop
        PROPERTIES ("type" = "append_only")
    """
    sql "DROP STREAM s_drop_force FORCE"
    assertEquals(0, sql("""
        SELECT COUNT(*) FROM information_schema.table_streams
        WHERE DB_NAME = 'test_table_stream_drop_db'
          AND STREAM_NAME = 's_drop_force'
    """)[0][0] as int)
}
