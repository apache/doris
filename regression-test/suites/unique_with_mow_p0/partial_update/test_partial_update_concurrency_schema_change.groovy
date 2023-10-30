
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

suite("test_partial_update_concurrency_schema_change", "p0") {

    def tableName = "test_partial_update_concurrency_schema_change"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE ${tableName} (
        k1 int,
        v1 varchar(20),
        v2 varchar(20),
        v3 varchar(20),
        v4 varchar(20),
        v5 varchar(20))
        UNIQUE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES(
            "replication_num" = "1",
            "light_schema_change" = "true",
            "enable_unique_key_merge_on_write" = "true",
            "disable_auto_compaction" = "true")"""


    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', "k1"

        file 'concurrency_update1.csv'
        time 10000 // limit inflight 10s
    }
    qt_sql """ select * from ${tableName} order by k1;"""

    def threads = []
    def threads_num = 5
    def cnt = -1
    for (int i = 1; i <= threads_num; i++) {
        def columns = "k1,v${i}"
        logger.info("in ${i}: columns: ${columns}")
        threads.add(Thread.startDaemon {    
            def num = "${cnt}"
            while (num < 0 || num-- > 0) {
                streamLoad {
                    table "${tableName}"
                    set 'column_separator', ','
                    set 'format', 'csv'
                    set 'partial_columns', 'true'
                    set 'columns', columns
                    file 'concurrency_update.csv'
                    time 10000 // limit inflight 10s
                }
            }
        })
    }

    // def t1 = Thread.startDaemon {
    //     for (int i = 1; i <= threads_num; i++) {
    //         Thread.sleep(1000)
    //         sql """ alter table ${tableName} modify column v${i} varchar(40);"""
    //     }  
    // }
    // t1.join()

    for (int i = 0; i < threads_num; i++) {
        threads[i].join()
    }
    
    qt_sql """ select * from ${tableName} order by k1;"""
}
