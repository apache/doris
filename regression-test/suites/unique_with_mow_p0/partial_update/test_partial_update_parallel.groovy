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

    // case 1: concurrent partial update
    def tableName = "test_primary_key_partial_update"
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

    sql """insert into ${tableName} values
        (2, "doris2", 2000, 223, 2),
        (1, "doris", 1000, 123, 1),
        (5, "doris5", 5000, 523, 5),
        (4, "doris4", 4000, 423, 4),
        (3, "doris3", 3000, 323, 3);"""

    t1 = Thread.startDaemon {
        streamLoad {
            table "${tableName}"

            set 'column_separator', ','
            set 'format', 'csv'
            set 'partial_columns', 'true'
            set 'columns', 'id,name'

            file 'partial_update_parallel1.csv'
            time 10000 // limit inflight 10s
        }
    }

    t2 = Thread.startDaemon {
        streamLoad {
            table "${tableName}"

            set 'column_separator', ','
            set 'format', 'csv'
            set 'partial_columns', 'true'
            set 'columns', 'id,score,test'

            file 'partial_update_parallel2.csv'
            time 10000 // limit inflight 10s
        }
    }

    t3 = Thread.startDaemon {
        streamLoad {
            table "${tableName}"

            set 'column_separator', ','
            set 'format', 'csv'
            set 'partial_columns', 'true'
            set 'columns', 'id,dft'

            file 'partial_update_parallel3.csv'
            time 10000 // limit inflight 10s
        }
    }

    t1.join()
    t2.join()
    t3.join()

    sql "sync"

    qt_sql """ select * from ${tableName} order by id;"""

    sql """ DROP TABLE IF EXISTS ${tableName}; """


    // case 2: concurrent partial update with row store column
    tableName = "test_primary_key_row_store_partial_update"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE ${tableName} (
                `id` int(11) NOT NULL COMMENT "用户 ID",
                `name` varchar(65533) NOT NULL COMMENT "用户姓名",
                `score` int(11) NOT NULL COMMENT "用户得分",
                `test` int(11) NULL COMMENT "null test",
                `dft` int(11) DEFAULT "4321")
                UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
                PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true", "store_row_column" = "true")
    """

    sql """insert into ${tableName} values
        (2, "doris2", 2000, 223, 2),
        (1, "doris", 1000, 123, 1),
        (5, "doris5", 5000, 523, 5),
        (4, "doris4", 4000, 423, 4),
        (3, "doris3", 3000, 323, 3);"""

    t1 = Thread.startDaemon {
        streamLoad {
            table "${tableName}"

            set 'column_separator', ','
            set 'format', 'csv'
            set 'partial_columns', 'true'
            set 'columns', 'id,name'

            file 'partial_update_parallel1.csv'
            time 10000 // limit inflight 10s
        }
    }

    t2 = Thread.startDaemon {
        streamLoad {
            table "${tableName}"

            set 'column_separator', ','
            set 'format', 'csv'
            set 'partial_columns', 'true'
            set 'columns', 'id,score,test'

            file 'partial_update_parallel2.csv'
            time 10000 // limit inflight 10s
        }
    }

    t3 = Thread.startDaemon {
        streamLoad {
            table "${tableName}"

            set 'column_separator', ','
            set 'format', 'csv'
            set 'partial_columns', 'true'
            set 'columns', 'id,dft'

            file 'partial_update_parallel3.csv'
            time 10000 // limit inflight 10s
        }
    }

    t1.join()
    t2.join()
    t3.join()

    sql "sync"

    qt_sql """ select * from ${tableName} order by id;"""

    sql """ DROP TABLE IF EXISTS ${tableName}; """


    // case 3: concurrent partial update with sequence column
    tableName = "test_primary_key_seq_partial_update"

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
                PROPERTIES(
                    "replication_num" = "1", 
                    "enable_unique_key_merge_on_write" = "true",
                    "function_column.sequence_col" = "dft")
    """

    sql """insert into ${tableName} values
        (2, "deprecated", 99999, 999, 1),
        (2, "doris2", 2000, 223, 2),
        (1, "doris", 1000, 123, 1),
        (3, "deprecated", 99999, 999, 2),
        (5, "doris5", 5000, 523, 5),
        (4, "doris4", 4000, 423, 4),
        (4, "deprecated", 99999, 999, 3),
        (4, "deprecated", 99999, 999, 1),
        (3, "doris3", 3000, 323, 3);"""

    t1 = Thread.startDaemon {
        streamLoad {
            table "${tableName}"

            set 'column_separator', ','
            set 'format', 'csv'
            set 'partial_columns', 'true'
            set 'columns', 'id,name'

            file 'partial_update_parallel1.csv'
            time 10000 // limit inflight 10s
        }
    }

    t2 = Thread.startDaemon {
        streamLoad {
            table "${tableName}"

            set 'column_separator', ','
            set 'format', 'csv'
            set 'partial_columns', 'true'
            set 'columns', 'id,score,test'

            file 'partial_update_parallel2.csv'
            time 10000 // limit inflight 10s
        }
    }

    t3 = Thread.startDaemon {
        streamLoad {
            table "${tableName}"

            set 'column_separator', ','
            set 'format', 'csv'
            set 'partial_columns', 'true'
            set 'columns', 'id,dft'

            file 'partial_update_parallel3.csv'
            time 10000 // limit inflight 10s
        }
    }

    t1.join()
    t2.join()
    t3.join()

    sql "set show_hidden_columns=true;"
    sql "sync"

    qt_sql """ select * from ${tableName} order by id;"""
    sql "set show_hidden_columns=false;"
    sql "sync"
    sql """ DROP TABLE IF EXISTS ${tableName}; """


    // case 4: concurrent partial update with row store column and sequence column
    tableName = "test_primary_key_row_store_seq_partial_update"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE ${tableName} (
                `id` int(11) NOT NULL COMMENT "用户 ID",
                `name` varchar(65533) NOT NULL COMMENT "用户姓名",
                `score` int(11) NOT NULL COMMENT "用户得分",
                `test` int(11) NULL COMMENT "null test",
                `dft` int(11) DEFAULT "4321")
                UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
                PROPERTIES(
                    "replication_num" = "1", 
                    "enable_unique_key_merge_on_write" = "true",
                    "function_column.sequence_col" = "dft",
                    "store_row_column" = "true")
    """

    sql """insert into ${tableName} values
        (2, "deprecated", 99999, 999, 1),
        (2, "doris2", 2000, 223, 2),
        (1, "doris", 1000, 123, 1),
        (3, "deprecated", 99999, 999, 2),
        (5, "doris5", 5000, 523, 5),
        (4, "doris4", 4000, 423, 4),
        (4, "deprecated", 99999, 999, 3),
        (4, "deprecated", 99999, 999, 1),
        (3, "doris3", 3000, 323, 3);"""

    t1 = Thread.startDaemon {
        streamLoad {
            table "${tableName}"

            set 'column_separator', ','
            set 'format', 'csv'
            set 'partial_columns', 'true'
            set 'columns', 'id,name'

            file 'partial_update_parallel1.csv'
            time 10000 // limit inflight 10s
        }
    }

    t2 = Thread.startDaemon {
        streamLoad {
            table "${tableName}"

            set 'column_separator', ','
            set 'format', 'csv'
            set 'partial_columns', 'true'
            set 'columns', 'id,score,test'

            file 'partial_update_parallel2.csv'
            time 10000 // limit inflight 10s
        }
    }

    t3 = Thread.startDaemon {
        streamLoad {
            table "${tableName}"

            set 'column_separator', ','
            set 'format', 'csv'
            set 'partial_columns', 'true'
            set 'columns', 'id,dft'

            file 'partial_update_parallel3.csv'
            time 10000 // limit inflight 10s
        }
    }

    t1.join()
    t2.join()
    t3.join()

    sql "set show_hidden_columns=true;"
    sql "sync"

    qt_sql """ select id,name,score,test,dft,__DORIS_DELETE_SIGN__,__DORIS_VERSION_COL__,__DORIS_SEQUENCE_COL__ from ${tableName} order by id;"""

    sql """ DROP TABLE IF EXISTS ${tableName}; """

    // case 5: partial update with delete sign
    def tableName = "test_primary_key_partial_update_delete_sign"
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

    sql """insert into ${tableName} values
        (2, "doris2", 2000, 223, 2),
        (1, "doris", 1000, 123, 1),
        (5, "doris5", 5000, 523, 5),
        (4, "doris4", 4000, 423, 4),
        (3, "doris3", 3000, 323, 3);"""

    t1 = Thread.startDaemon {
        streamLoad {
            table "${tableName}"

            set 'column_separator', ','
            set 'format', 'csv'
            set 'partial_columns', 'true'
            set 'columns', 'id,name'

            file 'partial_update_parallel1.csv'
            time 10000 // limit inflight 10s
        }
    }

    t2 = Thread.startDaemon {
        streamLoad {
            table "${tableName}"

            set 'column_separator', ','
            set 'format', 'csv'
            set 'partial_columns', 'true'
            set 'columns', 'id,__DORIS_DELETE_SIGN__'

            file 'partial_update_parallel4.csv'
            time 10000 // limit inflight 10s
        }
    }

    t3 = Thread.startDaemon {
        streamLoad {
            table "${tableName}"

            set 'column_separator', ','
            set 'format', 'csv'
            set 'partial_columns', 'true'
            set 'columns', 'id,dft'

            file 'partial_update_parallel3.csv'
            time 10000 // limit inflight 10s
        }
    }

    t1.join()
    t2.join()
    t3.join()

    // Delete again, to make sure the result is right
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'partial_columns', 'true'
        set 'columns', 'id,__DORIS_DELETE_SIGN__'

        file 'partial_update_parallel4.csv'
        time 10000 // limit inflight 10s
    }

    sql "sync"

    qt_sql """ select * from ${tableName} order by id;"""

    sql """ DROP TABLE IF EXISTS ${tableName}; """
}

