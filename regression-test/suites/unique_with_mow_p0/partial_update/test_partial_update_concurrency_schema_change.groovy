
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

import java.util.concurrent.atomic.AtomicInteger

suite("test_partial_update_concurrency_schema_change", "p0") {

    def tableName = "test_partial_update_concurrency_schema_change"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE ${tableName} (
        k1 varchar(20) not null,
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

    AtomicInteger v2_length = new AtomicInteger(20);

    def wait_for_schema_change = {
        def try_times=100
        while(true){
            def res = sql " SHOW ALTER TABLE COLUMN WHERE TableName = '${tableName}' ORDER BY CreateTime DESC LIMIT 1 "
            Thread.sleep(10)
            if(res[0][9].toString() == "FINISHED"){
                break;
            }
            assert(try_times>0)
            try_times--
        }
    }

    def threads = []
    def threads_num = 5
    def cnt = -1
    for (int i = 1; i <= threads_num; i++) {
        def columns = "k1,tmp,v${i}=substr(tmp,1,20)"
        def idx = i
        threads.add(Thread.startDaemon {    
            def num = cnt
            
            while (num < 0 || num-- > 0) {
                logger.info("num: ${num}")
                if (idx == 2) {
                    def cur_length = v2_length.get()
                    logger.info("idx=2, cur_length=${cur_length}")
                    columns = "k1,tmp,v${idx}=substr(tmp,1,${cur_length})"
                }
                logger.info("in ${idx}: columns: ${columns}")
                streamLoad {
                    table "${tableName}"
                    set 'column_separator', ','
                    set 'format', 'csv'
                    set 'partial_columns', 'true'
                    set 'columns', columns
                    set 'strict_mode', "false"
                    file 'concurrency_update.csv'
                    time 10000 // limit inflight 10s
                }
            }
        })
    }

    def t1 = Thread.startDaemon {
        def num = cnt
        def cur_length = v2_length.get()
        while (cur_length < 100 && (num < 0 || num-- > 0)) {
            // for (int i = 1; i <= threads_num; i++) {
                sql """ alter table ${tableName} modify column v2 varchar(${cur_length+1});"""
                wait_for_schema_change()
                logger.info("column v2 change to varchar(${cur_length+1})");
                v2_length.incrementAndGet()
                cur_length = v2_length.get()
                sleep(1000)
            // }  
            // cur_length++
        }
    }
    t1.join()

    for (int i = 0; i < threads_num; i++) {
        threads[i].join()
    }
    
    qt_sql """ select * from ${tableName} order by k1;"""
}
