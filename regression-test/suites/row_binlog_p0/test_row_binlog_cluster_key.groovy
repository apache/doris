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

suite("test_row_binlog_cluster_key", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    sql "DROP TABLE IF EXISTS test_row_binlog_cluster_key FORCE"

    sql """
        CREATE TABLE test_row_binlog_cluster_key (
            id BIGINT NOT NULL,
            cluster_value INT NOT NULL,
            payload VARCHAR(32) NOT NULL
        )
        UNIQUE KEY(id)
        ORDER BY(cluster_value)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
    """

    // Each cluster-key Segment's primary index carries a rowid suffix, even for a single row.
    sql "INSERT INTO test_row_binlog_cluster_key VALUES (1, 30, 'before-update')"
    sql "INSERT INTO test_row_binlog_cluster_key VALUES (2, 10, 'before-delete')"

    // Use a full-row UPSERT because cluster-key tables do not support partial updates.
    sql """
        INSERT INTO test_row_binlog_cluster_key VALUES
            (1, 20, 'after-update')
    """
    sql "DELETE FROM test_row_binlog_cluster_key WHERE id = 2"
    sql "SYNC"

    order_qt_cluster_key_current_rows """
        SELECT id, cluster_value, payload
        FROM test_row_binlog_cluster_key
    """

    qt_cluster_key_before_binlog """
        SELECT __DORIS_BINLOG_OP__,
               id,
               cluster_value,
               payload,
               __BEFORE__cluster_value__,
               __BEFORE__payload__
        FROM binlog("table" = "test_row_binlog_cluster_key")
        WHERE __DORIS_BINLOG_OP__ <> 0
        ORDER BY __DORIS_BINLOG_OP__, id
    """
}
