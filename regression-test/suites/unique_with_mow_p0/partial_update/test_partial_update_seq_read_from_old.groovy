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

suite('test_partial_update_seq_read_from_old') {
    def inspect_rows = { sqlStr ->
        sql "set skip_delete_sign=true;"
        sql "set skip_delete_bitmap=true;"
        sql "sync"
        qt_inspect sqlStr
        sql "set skip_delete_sign=false;"
        sql "set skip_delete_bitmap=false;"
        sql "sync"
    }

    for (def use_row_store : [false, true]) {
        logger.info("current params: use_row_store: ${use_row_store}")
        def tableName = "test_partial_update_seq_read_from_old_${use_row_store}"
        sql """ DROP TABLE IF EXISTS ${tableName} force;"""
        sql """ CREATE TABLE ${tableName} (
            `k` int(11) NULL, 
            `v1` BIGINT NULL,
            `v2` BIGINT NULL,
            `v3` BIGINT NOT NULL DEFAULT "9876",
            `v4` BIGINT NOT NULL DEFAULT "1234",
            `v5` BIGINT NULL
            ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES(
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "enable_unique_key_skip_bitmap_column" = "true",
            "function_column.sequence_col" = "v1",
            "store_row_column" = "${use_row_store}"); """

        sql "insert into ${tableName} values(1,1000,1,1,1,1);"
        qt_sql "select k,v1,v2,v3,v4,v5 from ${tableName} order by k;"

        sql "delete from ${tableName} where k=1;"
        qt_sql1 "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__,__DORIS_DELETE_SIGN__ from ${tableName} order by k;"
        inspect_rows "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__,__DORIS_DELETE_SIGN__,__DORIS_VERSION_COL__ from ${tableName} order by k,__DORIS_VERSION_COL__;"
    
        sql "set enable_unique_key_partial_update=true;"
        sql "sync;"
    
        sql "insert into ${tableName}(k,v4,v5) values(1,777,8888);"
        qt_sql2 "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__,__DORIS_DELETE_SIGN__ from ${tableName} order by k;"
        inspect_rows "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__,__DORIS_DELETE_SIGN__,__DORIS_VERSION_COL__ from ${tableName} order by k,__DORIS_VERSION_COL__;"

        sql "insert into ${tableName}(k,v1,v2,v4) values(1,999,-1,-2);"
        qt_sql3 "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__,__DORIS_DELETE_SIGN__ from ${tableName} order by k;"
        inspect_rows "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__,__DORIS_DELETE_SIGN__,__DORIS_VERSION_COL__ from ${tableName} order by k,__DORIS_VERSION_COL__;"
    }
}