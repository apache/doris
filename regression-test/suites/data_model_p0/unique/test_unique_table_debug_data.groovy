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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_unique_table_debug_data") {

    sql "SET show_hidden_columns=false"
    sql "SET skip_delete_predicate=false"
    sql "SET skip_storage_engine_merge=false"

    def tbName = "test_unique_table_debug_data"
    sql "DROP TABLE IF EXISTS ${tbName}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName} (
                a int, b int
            )
            unique key (a)
            distributed by hash(a) buckets 16
            properties(
                "replication_allocation" = "tag.location.default:1",
                "disable_auto_compaction" = "true",
                "enable_unique_key_merge_on_write" = "false"
            );
        """
        // test mor table

    //BackendId,Cluster,IP,HeartbeatPort,BePort,HttpPort,BrpcPort,LastStartTime,LastHeartbeat,Alive,SystemDecommissioned,ClusterDecommissioned,TabletNum,DataUsedCapacity,AvailCapacity,TotalCapacity,UsedPct,MaxDiskUsedPct,Tag,ErrMsg,Version,Status
    String[][] backends = sql """ show backends; """
    assertTrue(backends.size() > 0)
    StringBuilder sbCommand = new StringBuilder();

    sql "insert into ${tbName} values(1,1),(2,1);"
    sql "insert into ${tbName} values(1,11),(2,11);"
    sql "insert into ${tbName} values(3,1);"
    sql "sync"

    qt_select_init "select * from ${tbName} order by a, b"

    // enable skip_storage_engine_merge and check select result,
    // not merged original rows are returned:
    sql "SET skip_storage_engine_merge=true"
    qt_select_skip_merge "select * from ${tbName} order by a, b"

    // turn off skip_storage_engine_merge
    sql "SET skip_storage_engine_merge=false"

    // batch delete and select again:
    // curl --location-trusted -uroot: -H "column_separator:|" -H "columns:a, b" -H "merge_type: delete" -T delete.csv http://127.0.0.1:8030/api/test_skip/t1/_stream_load
    streamLoad {
        table "${tbName}"

        set 'column_separator', '|'
        set 'columns', 'a, b'
        set 'merge_type', 'delete'

        file 'test_unique_table_debug_data_delete.csv'

        time 10000 // limit inflight 10s
    }
    sql "sync"
    qt_select_batch_delete "select * from ${tbName} order by a, b"

    // delete rows with a = 2:
    sql "delete from ${tbName} where a = 2;"
    sql "sync"
    qt_select_sql_delete "select * from ${tbName} order by a, b"

    // enable skip_delete_predicate, rows deleted with delete statement is returned:
    sql "SET skip_delete_predicate=true"
    qt_select_skip_delete1 "select * from ${tbName} order by a, b"

    sql "SET skip_delete_predicate=false"

    // enable skip_storage_engine_merge and select, rows deleted with delete statement is not returned:
    sql "SET skip_storage_engine_merge=true"
    qt_select_skip_merge_after_delete "select * from ${tbName} order by a, b"

    // enable skip_delete_predicate, rows deleted with delete statement is also returned:
    sql "SET skip_delete_predicate=true"
    qt_select_skip_delete2 "select * from ${tbName} order by a, b"

    sql "DROP TABLE ${tbName}"
}
