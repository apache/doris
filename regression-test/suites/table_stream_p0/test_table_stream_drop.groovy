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

    sql "DROP DATABASE IF EXISTS test_table_stream_drop_db"
}
