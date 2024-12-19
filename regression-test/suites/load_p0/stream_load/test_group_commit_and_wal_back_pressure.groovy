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

import org.awaitility.Awaitility
import static java.util.concurrent.TimeUnit.SECONDS

suite("test_group_commit_and_wal_back_pressure") {

    def getRowCount = { table, expectedRowCount ->
        Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until(
            {
                def result = sql "select count(*) from ${table}"
                logger.info("table: ${table}, rowCount: ${result}")
                return result[0][0] == expectedRowCount
            }
        )
    }

    def tableName = "test_group_commit_and_wal_back_pressure"
    for (int j = 1; j <= 3; j++) {
        sql """ DROP TABLE IF EXISTS ${tableName}${j} """
        sql """
            CREATE TABLE ${tableName}${j} (
                k bigint,  
                v string
                )  
                UNIQUE KEY(k)  
                DISTRIBUTED BY HASH (k) BUCKETS 32  
                PROPERTIES(  
                "group_commit_interval_ms" = "100",
                "replication_num" = "1"
            );
        """
    }

    def t1 = []
    for (int i = 0; i < 20; i++) {
        t1.add(Thread.startDaemon {
            streamLoad {
                table "${tableName}1"

                set 'column_separator', ','
                set 'compress_type', 'GZ'
                set 'format', 'csv'
                set 'group_commit', 'async_mode'
                unset 'label'

                file 'test_group_commit_and_wal_back_pressure.csv.gz'
                time 600000 
            }
        })
    }

    def t2 = []
    for (int i = 0; i < 20; i++) {
        t2.add(Thread.startDaemon {
            streamLoad {
                table "${tableName}2"

                set 'column_separator', ','
                set 'compress_type', 'GZ'
                set 'format', 'csv'
                set 'group_commit', 'async_mode'
                unset 'label'

                file 'test_group_commit_and_wal_back_pressure.csv.gz'
                time 600000 
            }
        })
    }

    def t3 = []
    for (int i = 0; i < 20; i++) {
        t3.add(Thread.startDaemon {
            streamLoad {
                table "${tableName}3"

                set 'column_separator', ','
                set 'compress_type', 'GZ'
                set 'format', 'csv'
                set 'group_commit', 'async_mode'
                unset 'label'

                file 'test_group_commit_and_wal_back_pressure.csv.gz'
                time 600000 
            }
        })
    }

    for (Thread th in t1) {
        th.join()
    }

    for (Thread th in t2) {
        th.join()
    }

    for (Thread th in t3) {
        th.join()
    }
    sql "sync"
    getRowCount "${tableName}1", 1
    getRowCount "${tableName}2", 1
    getRowCount "${tableName}3", 1
}