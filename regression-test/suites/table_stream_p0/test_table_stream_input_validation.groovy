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

suite("test_table_stream_input_validation") {
    sql "DROP STREAM IF EXISTS stream_validation_initial FORCE"
    sql "DROP STREAM IF EXISTS stream_validation_incremental FORCE"
    sql "DROP STREAM IF EXISTS stream_validation_default FORCE"
    sql "DROP TABLE IF EXISTS stream_validation_sink"
    sql "DROP TABLE IF EXISTS stream_validation_source"

    sql """
        CREATE TABLE stream_validation_source (id BIGINT, value INT)
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
    """
    sql """
        CREATE TABLE stream_validation_sink (id BIGINT, value INT)
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql "INSERT INTO stream_validation_source VALUES (1, 10)"
    sql "sync"

    ["garbage", "yes", "1", "", " true "].each { value ->
        test {
            sql """
                CREATE STREAM stream_validation_initial ON TABLE stream_validation_source
                PROPERTIES ("show_initial_rows" = "${value}")
            """
            exception "show_initial_rows must be `true` or `false`"
        }
    }
    // Reusing the failed name proves invalid properties did not publish a Stream.
    sql """
        CREATE STREAM stream_validation_initial ON TABLE stream_validation_source
        PROPERTIES ("show_initial_rows" = "TrUe")
    """
    sql """
        CREATE STREAM stream_validation_incremental ON TABLE stream_validation_source
        PROPERTIES ("show_initial_rows" = "FaLsE")
    """
    sql "CREATE STREAM stream_validation_default ON TABLE stream_validation_source"

    ["snapshot", "reset"].each { mode ->
        ["'unknown'='x'", "x", "`x`", "x,y"].each { arguments ->
            test {
                sql "SELECT * FROM stream_validation_incremental@${mode}(${arguments}) ORDER BY id"
                exception "${mode} does not accept parameters"
            }
            test {
                sql """
                    INSERT INTO stream_validation_sink
                    SELECT id, value FROM stream_validation_incremental@${mode}(${arguments})
                """
                exception "${mode} does not accept parameters"
            }
        }
    }
    sql "SELECT * FROM stream_validation_initial ORDER BY id"
    sql "SELECT * FROM stream_validation_incremental@snapshot() ORDER BY id"
    sql "SELECT * FROM stream_validation_incremental@reset() ORDER BY id"
}
