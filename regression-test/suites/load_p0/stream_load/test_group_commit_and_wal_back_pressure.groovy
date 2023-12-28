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

suite("test_group_commit_and_wal_back_pressure", "p2") {

    def tableName = "test_group_commit_and_wal_back_pressure"
    sql """ DROP TABLE IF EXISTS ${tableName}1 """
    sql """
            CREATE TABLE ${tableName}1 (
                k bigint,  
                v string
                )  
                UNIQUE KEY(k)  
                DISTRIBUTED BY HASH (k) BUCKETS 32  
                PROPERTIES(  
                "replication_num" = "1"
            );
    """

    sql """ DROP TABLE IF EXISTS ${tableName}2 """
    sql """
            CREATE TABLE ${tableName}2 (
                k bigint,  
                v string
                )  
                UNIQUE KEY(k)  
                DISTRIBUTED BY HASH (k) BUCKETS 32  
                PROPERTIES(  
                "replication_num" = "1"
            );
    """

    sql """ DROP TABLE IF EXISTS ${tableName}3 """
    sql """
            CREATE TABLE ${tableName}3 (
                k bigint,  
                v string
                )  
                UNIQUE KEY(k)  
                DISTRIBUTED BY HASH (k) BUCKETS 32  
                PROPERTIES(  
                "replication_num" = "1"
            );
    """

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

    qt_1 """ select count(*) from ${tableName}1;"""

    qt_2 """ select count(*) from ${tableName}2;"""

    qt_3 """ select count(*) from ${tableName}3;"""

}