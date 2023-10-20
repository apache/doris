
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

suite("test_primary_key_partial_update_seq_col", "p0") {
    def tableName = "test_primary_key_partial_update_seq_col"

    // create table
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE ${tableName} (
                `id` int(11) NOT NULL COMMENT "用户 ID",
                `name` varchar(65533) DEFAULT "unknown" COMMENT "用户姓名",
                `score` int(11) NOT NULL COMMENT "用户得分",
                `test` int(11) NULL COMMENT "null test",
                `dft` int(11) DEFAULT "4321",
                `update_time` date NULL)
            UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES(
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "function_column.sequence_col" = "update_time"
            )
    """
    // insert 2 lines
    sql """
        insert into ${tableName} values
            (2, "doris2", 2000, 223, 1, '2023-01-01'),
            (1, "doris", 1000, 123, 1, '2023-01-01')
    """

    sql "sync"

    qt_select_default """
        select * from ${tableName} order by id;
    """

    // don't set partial update header, it's a row update streamload
    // the input data don't contains sequence mapping column, will load fail
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'id,score'

        file 'basic.csv'
        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertTrue(json.Message.contains('need to specify the sequence column'))
        }
    }


    // set partial update header, should success
    // we don't provide the sequence column in input data, so the updated rows
    // should use there original sequence column values.
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'partial_columns', 'true'
        set 'columns', 'id,score'

        file 'basic.csv'
        time 10000 // limit inflight 10s
    }

    sql "sync"

    qt_partial_update_without_seq """
        select * from ${tableName} order by id;
    """

    // provide the sequence column this time, should update according to the
    // given sequence values
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'partial_columns', 'true'
        set 'columns', 'id,score,update_time'

        file 'basic_with_seq.csv'
        time 10000 // limit inflight 10s
    }

    sql "sync"

    qt_partial_update_with_seq """
        select * from ${tableName} order by id;
    """

    sql "SET show_hidden_columns=true"

    sql "sync"

    qt_partial_update_with_seq_hidden_columns """
        select * from ${tableName} order by id;
    """

    // drop drop
    sql """ DROP TABLE IF EXISTS ${tableName} """


    def tableName2 = "nereids_partial_update_native_insert_seq_col2"
    sql """ DROP TABLE IF EXISTS ${tableName2} """
    sql """
            CREATE TABLE ${tableName2} (
                `id` int(11) NOT NULL COMMENT "用户 ID",
                `score` int(11) NOT NULL COMMENT "用户得分",
                `update_time` DATETIMEV2 NULL DEFAULT CURRENT_TIMESTAMP)
            UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES(
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "function_column.sequence_col" = "update_time"
            )""" 
    
    // don't set partial update header, it's a row update streamload
    // the input data don't contains sequence mapping column but the sequence mapping
    // column's default value is CURRENT_TIMESTAMP, will load successfully
    streamLoad {
        table "${tableName2}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'id,score'

        file 'basic.csv'
        time 10000 // limit inflight 10s
    }
    qt_sql """ select id,score from ${tableName2} order by id;"""
    sql """ DROP TABLE IF EXISTS ${tableName2}; """
}
