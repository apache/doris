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

suite("test_row_binlog_auto_partition", "nonConcurrent") {
    sql "DROP TABLE IF EXISTS test_row_binlog_auto_partition FORCE"

    sql """
        CREATE TABLE test_row_binlog_auto_partition (
            event_date DATE NOT NULL,
            id INT,
            value STRING
        )
        DUPLICATE KEY(event_date, id)
        AUTO PARTITION BY RANGE (date_trunc(event_date, 'day')) ()
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        )
    """

    sql """
        INSERT INTO test_row_binlog_auto_partition VALUES
            ('2026-08-10', 1, 'first'),
            ('2026-08-11', 2, 'second')
    """

    order_qt_auto_partition_data """
        SELECT event_date, id, value
        FROM test_row_binlog_auto_partition
        ORDER BY event_date, id
    """

    order_qt_auto_partition_binlog """
        SELECT __DORIS_BINLOG_OP__, event_date, id, value
        FROM binlog("table" = "test_row_binlog_auto_partition")
        ORDER BY event_date, id
    """
}
