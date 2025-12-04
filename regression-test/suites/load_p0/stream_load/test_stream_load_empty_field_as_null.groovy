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

suite("test_stream_load_empty_field_as_null", "p0") {

    def tableName = "test_stream_load_empty_field_as_null"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` int(20) NULL,
            `k2` string NULL,
            `v1` date  NULL,
            `v2` string  NULL,
            `v3` datetime  NULL,
            `v4` string  NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 3
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    streamLoad {
        table "${tableName}"
        set 'column_separator', ','

        file "empty_field_as_null.csv"
    }

    sql "sync"
    qt_select_1 """
        SELECT * FROM ${tableName} ORDER BY k1 
    """
    sql "truncate table ${tableName}"

    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'empty_field_as_null', 'true'

        file "empty_field_as_null.csv"
    }

    sql "sync"
    qt_select_2 """
        SELECT * FROM ${tableName} ORDER BY k1 
    """
}
