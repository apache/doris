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

suite("test_txt_special_delimiter", "p0") {
    sql "show tables"

    def tableName = "test_txt_special_delimiter"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` bigint(20) NULL,
            `k2` bigint(20) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`, `k2`)
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 3
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """
    for ( i in 0..1 ) {
        // should be deleted after new_load_scan is ready
        if (i == 1) {
            sql """ADMIN SET FRONTEND CONFIG ("enable_new_load_scan_node" = "false");"""
        } else {
            sql """ADMIN SET FRONTEND CONFIG ("enable_new_load_scan_node" = "true");"""
        }

        // test special_delimiter success
        streamLoad {
            table "${tableName}"

            set 'column_separator', '01030204'
            set 'line_delimiter', '04020301'
            set 'columns', 'k1, k2'
            set 'strict_mode', 'true'

            file 'test_txt_special_delimiter.csv'
            time 10000 // limit inflight 10s
        }

        sql "sync"
    }
    qt_sql "select * from ${tableName} order by k1, k2"
}

