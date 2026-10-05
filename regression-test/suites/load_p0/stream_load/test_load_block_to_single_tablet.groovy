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

suite("test_load_block_to_single_tablet", "p0") {
    sql "show tables"

    def tableName = "test_load_block_to_single_tablet"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE `${tableName}` (
            `ddate` date NULL,
            `dhour` varchar(*) NULL,
            `server_time` datetime NULL,
            `log_type` varchar(*) NULL,
            `source_flag` varchar(*) NULL,
            `a` varchar(*) NULL,
            `ext_json` varchar(*) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`ddate`)
        PARTITION BY RANGE(`ddate`)
        (PARTITION p202403 VALUES [('2024-03-01'), ('2024-04-01')),
        PARTITION p202404 VALUES [('2024-04-01'), ('2024-05-01')))
        DISTRIBUTED BY RANDOM BUCKETS 16
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
        """

    streamLoad {
        table "${tableName}"

        set 'column_separator', '\t'
        set 'columns', 'ddate, dhour, server_time, log_type, source_flag, a, ext_json'
        set 'partitions', 'p202403, p202404'

        file 'test_load_block_to_single_tablet.csv'
        time 20000 // limit inflight 10s
    }

    sql "sync"
    qt_sql1 "select count(*) from ${tableName};"
    qt_sql2 "select ddate,count(1) from ${tableName} group by ddate order by ddate;"
}
