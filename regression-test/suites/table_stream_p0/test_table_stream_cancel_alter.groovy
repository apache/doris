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

// Covers CANCEL ALTER STREAM on an OLAP table stream.
// NOTE: the CANCEL ALTER STREAM grammar is NOT implemented on the current
// master branch yet, so this case fails at parse time until the feature
// lands. It documents the intended coverage and the expected message for a
// stream that has no in-progress ALTER STREAM job.
// Paimon "pending ALTER ... CANCEL" is out of scope here: table streams do
// not support external catalogs (CreateStreamInfo rejects them), so only the
// OLAP stream path is covered.
suite("test_table_stream_cancel_alter") {
    if (isCloudMode()) {
        return
    }

    sql "DROP DATABASE IF EXISTS test_table_stream_cancel_alter_db"
    sql "CREATE DATABASE test_table_stream_cancel_alter_db"
    sql "USE test_table_stream_cancel_alter_db"

    sql """
        CREATE TABLE base_cancel (
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
    sql """
        CREATE STREAM s_cancel ON TABLE base_cancel
        PROPERTIES ("type" = "append_only")
    """

    // cancelling a stream with no in-progress ALTER STREAM job should report
    // that there is nothing to cancel.
    test {
        sql "CANCEL ALTER STREAM s_cancel"
        exception "no alter stream job"
    }

    sql "DROP DATABASE IF EXISTS test_table_stream_cancel_alter_db"
}
