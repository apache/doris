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

suite("test_mow_table_ignore_mode") {

    def tableName = "test_mow_table_ignore_mode1"
    sql """ DROP TABLE IF EXISTS ${tableName} FORCE;"""
    sql """
            CREATE TABLE ${tableName} ( 
                `id` int(11) NULL, 
                `name` varchar(10) NULL,
                `age` int(11) NULL DEFAULT "20", 
                `city` varchar(10) NOT NULL DEFAULT "beijing", 
                `balance` decimalv3(9, 0) NULL
            ) ENGINE = OLAP UNIQUE KEY(`id`) 
            COMMENT 'OLAP' DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ( 
                "replication_allocation" = "tag.location.default: 1", 
                "storage_format" = "V2", 
                "enable_unique_key_merge_on_write" = "true", 
                "light_schema_change" = "true", 
                "disable_auto_compaction" = "false", 
                "enable_single_replica_compaction" = "false" 
            );
    """
    sql """insert into ${tableName} values
        (1,"kevin",18,"shenzhen",400),
        (2,"bob",20,"beijing",500),
        (3,"alice",22,"shanghai",600),
        (4,"jack",24,"hangzhou",700),
        (5,"tom",26,"guanzhou",800);"""
    qt_origin_data "select * from ${tableName} order by id;"

    // some rows are with existing keys, some are not
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'id,name,age,city,balance'
        set 'ignore_mode', 'true'

        file 'ignore_mode.csv'
        time 10000 // limit inflight 10s
    }
    sql "sync"

    qt_after_ignore_mode_stream_load "select * from ${tableName} order by id;"
    sql """ DROP TABLE IF EXISTS ${tableName};"""


    // test illegal case
    def tableName2 = "test_mow_table_ignore_mode2"
    sql """ DROP TABLE IF EXISTS ${tableName2} FORCE;"""
    sql """
            CREATE TABLE ${tableName2} ( 
                `id` int(11) NULL, 
                `name` varchar(10) NULL,
                `age` int(11) NULL DEFAULT "20", 
                `city` varchar(10) NOT NULL DEFAULT "beijing", 
                `balance` decimalv3(9, 0) NULL
            ) ENGINE = OLAP UNIQUE KEY(`id`) 
            COMMENT 'OLAP' DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ( 
                "replication_allocation" = "tag.location.default: 1", 
                "storage_format" = "V2", 
                "enable_unique_key_merge_on_write" = "true", 
                "light_schema_change" = "true", 
                "disable_auto_compaction" = "false", 
                "enable_single_replica_compaction" = "false" 
            );"""
    sql """insert into ${tableName2} values
        (1,"kevin",18,"shenzhen",400),
        (2,"bob",20,"beijing",500),
        (3,"alice",22,"shanghai",600),
        (4,"jack",24,"hangzhou",700),
        (5,"tom",26,"guanzhou",800);"""
    // some rows are with existing keys, some are not
    streamLoad {
        table "${tableName2}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'id,balance'
        set 'partial_columns', 'true'
        set 'ignore_mode', 'true'

        file 'ignore_mode.csv'
        time 10000 // limit inflight 10s

        check {result, exception, startTime, endTime ->
            assertTrue(exception == null)
            def json = parseJson(result)
            assertEquals("Fail", json.Status)
            assertTrue(json.Message.contains("ignore mode can't be used in partial update."))
        }
    }
}
