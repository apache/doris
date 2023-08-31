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

suite("test_primary_key_partial_update_parallel", "p0") {

    def tableName = "test_primary_key_partial_update"

    // create table
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE ${tableName} (
                `id` int(11) NOT NULL COMMENT "用户 ID",
                `name` varchar(65533) NOT NULL COMMENT "用户姓名",
                `score` int(11) NOT NULL COMMENT "用户得分",
                `test` int(11) NULL COMMENT "null test",
                `dft` int(11) DEFAULT "4321")
                UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
                PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true")
    """
    // insert 2 lines
    sql """
        insert into ${tableName} values(2, "doris2", 2000, 223, 1)
    """

    sql """
        insert into ${tableName} values(1, "doris", 1000, 123, 1)
    """

    Thread.startDaemon {
        streamLoad {
            table "${tableName}"

            set 'column_separator', ','
            set 'format', 'csv'
            set 'partial_columns', 'true'
            set 'columns', 'id,score'

            file 'basic.csv'
            time 10000 // limit inflight 10s
        }
    }

    Thread.startDaemon {
        streamLoad {
            table "${tableName}"

            set 'column_separator', ','
            set 'format', 'csv'
            set 'partial_columns', 'true'
            set 'columns', 'id,score'

            file 'basic_with_duplicate.csv'
            time 10000 // limit inflight 10s
        }
    }

    Thread.startDaemon {
        streamLoad {
            table "${tableName}"

            set 'column_separator', ','
            set 'format', 'csv'
            set 'partial_columns', 'true'
            set 'columns', 'id,score'

            file 'basic_with_duplicate2.csv'
            time 10000 // limit inflight 10s
        }
    }

    sql "sync"

    sql """
        select * from ${tableName} order by id;
    """
}

