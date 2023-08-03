
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

suite("test_partial_update_strict_mode", "p0") {

    def tableName = "test_partial_update_strict_mode"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE ${tableName} ( 
                `id` int(11) NULL, 
                `name` varchar(10) NULL,
                `age` int(11) NULL DEFAULT "20", 
                `city` varchar(10) NOT NULL DEFAULT "beijing", 
                `balance` decimalv3(9, 0) NULL, 
                `last_access_time` datetime NULL 
            ) ENGINE = OLAP UNIQUE KEY(`id`) 
            COMMENT 'OLAP' DISTRIBUTED BY HASH(`id`) 
            BUCKETS AUTO PROPERTIES ( 
                "replication_allocation" = "tag.location.default: 1", 
                "storage_format" = "V2", 
                "enable_unique_key_merge_on_write" = "true", 
                "light_schema_change" = "true", 
                "disable_auto_compaction" = "false", 
                "enable_single_replica_compaction" = "false" 
            );
    """
    sql """insert into ${tableName} values(1,"kevin",18,"shenzhen",400,"2023-07-01 12:00:00");"""
    qt_sql """select * from ${tableName} order by id;"""
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'partial_columns', 'true'
        set 'columns', 'id,balance,last_access_time'
        set 'strict_mode', 'true'
        set 'max_filter_ratio', '1'

        file 'upsert.csv'
        time 10000 // limit inflight 10s

        check {result, exception, startTime, endTime ->
            assertTrue(exception == null)
            def json = parseJson(result)
            assertEquals("Success", json.Status)
            assertEquals(3, json.NumberTotalRows)
            assertEquals(1, json.NumberLoadedRows)
            assertEquals(2, json.NumberFilteredRows)
        }
    }
    sql "sync"
    qt_sql """select * from ${tableName} order by id;"""
    sql """ DROP TABLE IF EXISTS ${tableName} """


    def tableName2 = "test_partial_update_strict_mode2"
    sql """ DROP TABLE IF EXISTS ${tableName2} """
    sql """
            CREATE TABLE ${tableName2} ( 
                `id` int(11) NULL, 
                `name` varchar(10) NULL,
                `age` int(11) NULL DEFAULT "20", 
                `city` varchar(10) NOT NULL DEFAULT "beijing", 
                `balance` decimalv3(9, 0) NULL, 
                `last_access_time` datetime NULL 
            ) ENGINE = OLAP UNIQUE KEY(`id`) 
            COMMENT 'OLAP' DISTRIBUTED BY HASH(`id`) 
            BUCKETS AUTO PROPERTIES ( 
                "replication_allocation" = "tag.location.default: 1", 
                "storage_format" = "V2", 
                "enable_unique_key_merge_on_write" = "true", 
                "light_schema_change" = "true", 
                "disable_auto_compaction" = "false", 
                "enable_single_replica_compaction" = "false" 
            );
    """
    sql """insert into ${tableName2} values(1,"kevin",18,"shenzhen",400,"2023-07-01 12:00:00");"""
    qt_sql """select * from ${tableName2} order by id;"""
    streamLoad {
        table "${tableName2}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'partial_columns', 'true'
        set 'columns', 'id,balance,last_access_time'
        set 'strict_mode', 'true'
        set 'max_filter_ratio', '0.5'

        file 'upsert.csv'
        time 10000 // limit inflight 10s

        check {result, exception, startTime, endTime ->
            assertTrue(exception == null)
            def json = parseJson(result)
            assertEquals("Fail", json.Status)
            assertEquals("[INTERNAL_ERROR]too many filtered rows", json.Message)
            assertEquals(3, json.NumberTotalRows)
            assertEquals(1, json.NumberLoadedRows)
            assertEquals(2, json.NumberFilteredRows)
        }
    }
    sql "sync"
    qt_sql """select * from ${tableName2} order by id;"""

    def tableName3 = "test_partial_update_strict_mode3";
    sql """ DROP TABLE IF EXISTS ${tableName3} """
    sql """
            CREATE TABLE ${tableName3} ( 
                `id` int(11) NULL, 
                `name` varchar(10) NULL,
                `age` int(11) NULL DEFAULT "20", 
                `city` varchar(10) NOT NULL DEFAULT "beijing", 
                `balance` decimalv3(9, 0) NULL, 
                `last_access_time` datetime NULL 
            ) ENGINE = OLAP UNIQUE KEY(`id`) 
            COMMENT 'OLAP' DISTRIBUTED BY HASH(`id`) 
            BUCKETS AUTO PROPERTIES ( 
                "replication_allocation" = "tag.location.default: 1", 
                "storage_format" = "V2", 
                "enable_unique_key_merge_on_write" = "true", 
                "light_schema_change" = "true", 
                "disable_auto_compaction" = "false", 
                "enable_single_replica_compaction" = "false" 
            );
    """
    sql """insert into ${tableName3} values(1,"kevin",18,"shenzhen",400,"2023-07-01 12:00:00");"""
    qt_sql """select * from ${tableName3} order by id;"""
    streamLoad {
        table "${tableName3}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'partial_columns', 'true'
        set 'columns', 'id,balance,last_access_time'
        set 'strict_mode', 'false'
        set 'max_filter_ratio', '0.5'

        file 'upsert_invalid.csv'
        time 10000 // limit inflight 10s

        check {result, exception, startTime, endTime ->
            assertTrue(exception == null)
            def json = parseJson(result)
            assertEquals("Fail", json.Status)
            assertEquals("[INTERNAL_ERROR]too many filtered rows", json.Message)
            assertEquals(3, json.NumberTotalRows)
            assertEquals(1, json.NumberLoadedRows)
            assertEquals(2, json.NumberFilteredRows)
        }
    }
    sql "sync"
    qt_sql """select * from ${tableName3} order by id;"""

    // all columns valid, partial columns do not exist in file
    def tableName4 = "test_partial_update_strict_mode4"
    sql """ DROP TABLE IF EXISTS ${tableName4} """
    sql """
            CREATE TABLE ${tableName4} ( 
                `id` int(11) NULL, 
                `name` varchar(10) NULL,
                `age` int(11) NULL DEFAULT "20", 
                `city` varchar(10) NOT NULL DEFAULT "beijing", 
                `balance` decimalv3(9, 0) NULL, 
                `last_access_time` datetime NULL 
            ) ENGINE = OLAP UNIQUE KEY(`id`) 
            COMMENT 'OLAP' DISTRIBUTED BY HASH(`id`) 
            BUCKETS AUTO PROPERTIES ( 
                "replication_allocation" = "tag.location.default: 1", 
                "storage_format" = "V2", 
                "enable_unique_key_merge_on_write" = "true", 
                "light_schema_change" = "true", 
                "disable_auto_compaction" = "false", 
                "enable_single_replica_compaction" = "false" 
            );
    """
    sql """insert into ${tableName4} values(1,"kevin",18,"shenzhen",400,"2023-07-01 12:00:00");"""
    sql """insert into ${tableName4} values(3,"steve",23,"beijing",500,"2023-07-03 12:00:02");"""
    qt_sql """select * from ${tableName4} order by id;"""
    streamLoad {
        table tableName4
        set 'column_separator', ','
        set 'format', 'csv'
        set 'partial_columns', 'true'
        set 'columns', 'id,balance,last_access_time'
        set 'strict_mode', 'true'

        file 'upsert_missing_par_cols.csv'
        time 10000 // limit inflight 10s

        check {result, exception, startTime, endTime ->
            assertTrue(exception == null)
            def json = parseJson(result)
            assertEquals("Fail", json.Status)
        }
    }

    sql "sync"
    qt_sql """select * from ${tableName4} order by id;"""
}
