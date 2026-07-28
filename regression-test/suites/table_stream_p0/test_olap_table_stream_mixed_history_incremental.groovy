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

suite("test_olap_table_stream_mixed_history_incremental") {
    if (isCloudMode()) {
        return
    }

    sql "DROP DATABASE IF EXISTS test_olap_table_stream_mixed_history_incremental_db"
    sql "CREATE DATABASE test_olap_table_stream_mixed_history_incremental_db"
    sql "USE test_olap_table_stream_mixed_history_incremental_db"

    sql """
        CREATE TABLE tbl_mixed_history_incremental (
            sid INT NULL,
            sname VARCHAR(32) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(sid)
        PARTITION BY RANGE(sid)
        (
            PARTITION p1 VALUES LESS THAN (10),
            PARTITION p2 VALUES [(10), (20))
        )
        DISTRIBUTED BY HASH(sid) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        )
    """

    sql "INSERT INTO tbl_mixed_history_incremental VALUES (1, 'history_p1')"

    sql """
        CREATE STREAM s_mixed_history_incremental ON TABLE tbl_mixed_history_incremental
        PROPERTIES (
            "show_initial_rows" = "true"
        )
    """

    sql "INSERT INTO tbl_mixed_history_incremental VALUES (11, 'incremental_p2')"
    sql "sync"
    sleep(1200)

    order_qt_mixed_history_incremental_rows """
        SELECT sid, sname, __DORIS_STREAM_CHANGE_TYPE_COL__
        FROM s_mixed_history_incremental
        ORDER BY sid
    """

    order_qt_mixed_history_incremental_count """
        SELECT sid, count(*)
        FROM s_mixed_history_incremental
        GROUP BY sid
        ORDER BY sid
    """
}
