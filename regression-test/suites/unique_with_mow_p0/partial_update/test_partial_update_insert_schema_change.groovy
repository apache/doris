
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
import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility

suite("test_partial_update_insert_schema_change", "p0") {

    // ===== light schema change =====
    // test add value column
    def tableName = "test_partial_update_insert_schema_change_add_column"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE ${tableName} (
                `c0` int NULL,
                `c1` int NULL,
                `c2` int NULL,
                `c3` int NULL,
                `c4` int NULL,
                `c5` int NULL,
                `c6` int NULL,
                `c7` int NULL,
                `c8` int NULL,
                `c9` int NULL)
                UNIQUE KEY(`c0`) DISTRIBUTED BY HASH(`c0`) BUCKETS 1
                PROPERTIES(
                    "replication_num" = "1",
                    "light_schema_change" = "false",
                    "enable_unique_key_merge_on_write" = "true")
    """

    sql "insert into ${tableName} values(1, 0, 0, 0, 0, 0, 0, 0, 0, 0);"
    sql "sync"
    qt_add_value_col_1 " select * from ${tableName} order by c0 "
    
    // schema change
    sql " ALTER table ${tableName} add column c10 INT DEFAULT '0' "
    def try_times=12000
    // if timeout awaitility will raise exception
    Awaitility.await().atMost(try_times, TimeUnit.SECONDS).with().pollDelay(100, TimeUnit.MILLISECONDS).await().until(() -> {
        def res = sql " SHOW ALTER TABLE COLUMN WHERE TableName = '${tableName}' ORDER BY CreateTime DESC LIMIT 1 "
        if(res[0][9].toString() == "FINISHED"){
            return true;
        }
        return false;
    });
    sql "sync"
    
    // test insert data without new column
    sql "set enable_unique_key_partial_update=true;"
    sql "insert into ${tableName}(c0,c1,c2) values(1,1,1);"
    sql "set enable_unique_key_partial_update=false;"
    sql "sync"

    // check data, new column is filled by default value.
    qt_add_value_col_2 " select * from ${tableName} order by c0 "

    // test insert data with new column
    sql "set enable_unique_key_partial_update=true;"
    sql "insert into ${tableName}(c0,c1,c2,c10) values(1,1,1,10);"
    sql "set enable_unique_key_partial_update=false;"
    sql "sync"

    // check data, new column is filled by given value.
    qt_add_value_col_3 " select * from ${tableName} order by c0 "

    sql """ DROP TABLE IF EXISTS ${tableName} """


    // test delete value column
    tableName = "test_partial_update_insert_schema_change_delete_column"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """CREATE TABLE ${tableName} (
                `c0` int NULL,
                `c1` int NULL,
                `c2` int NULL,
                `c3` int NULL,
                `c4` int NULL,
                `c5` int NULL,
                `c6` int NULL,
                `c7` int NULL,
                `c8` int NULL,
                `c9` int NULL)
                UNIQUE KEY(`c0`) DISTRIBUTED BY HASH(`c0`) BUCKETS 1
                PROPERTIES(
                    "replication_num" = "1",
                    "light_schema_change" = "false",
                    "enable_unique_key_merge_on_write" = "true")"""

    sql "insert into ${tableName} values(1, 0, 0, 0, 0, 0, 0, 0, 0, 0);"
    sql "sync"
    qt_delete_value_col_1 " select * from ${tableName} order by c0 "
    
    // schema change
    sql " ALTER table ${tableName} DROP COLUMN c8 "
    // if timeout awaitility will raise exception
    Awaitility.await().atMost(try_times, TimeUnit.SECONDS).with().pollDelay(100, TimeUnit.MILLISECONDS).await().until(() -> {
        def res = sql " SHOW ALTER TABLE COLUMN WHERE TableName = '${tableName}' ORDER BY CreateTime DESC LIMIT 1 "
        if(res[0][9].toString() == "FINISHED"){
            return true;
        }
        return false;
    });
    sql "sync"

    // test insert data without delete column
    sql "set enable_unique_key_partial_update=true;"
    test {
        sql "insert into ${tableName}(c0,c1,c2,c8) values(1,1,1,10);"
        exception "c8"
    }
    sql "insert into ${tableName}(c0,c1,c2) values(1,1,1);"
    sql "set enable_unique_key_partial_update=false;"
    sql "sync"
    qt_delete_value_col_2 " select * from ${tableName} order by c0 "

    sql """ DROP TABLE IF EXISTS ${tableName} """


    // test delete sequence col
    tableName = "test_partial_update_insert_schema_change_delete_seq_col"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """CREATE TABLE ${tableName} (
                `k` int NULL,
                `v1` int NULL,
                `v2` int NULL,
                `c` int NULL)
                UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
                PROPERTIES(
                    "replication_num" = "1",
                    "light_schema_change" = "false",
                    "enable_unique_key_merge_on_write" = "true",
                    "function_column.sequence_col" = "c");"""
    sql "insert into ${tableName} values(1,1,1,1),(2,20,20,20),(1,10,10,10),(2,10,10,10);"
    qt_delete_seq_col_1 "select * from ${tableName} order by k;"

    // schema change
    test {
        sql " ALTER table ${tableName} DROP COLUMN c;"
        exception "Can not drop sequence mapping column[c] in Unique data model table[${tableName}]"
    }

    // test update value column
    tableName = "test_partial_update_insert_schema_change_update_column"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE ${tableName} (
                `c0` int NULL,
                `c1` int NULL,
                `c2` int NULL,
                `c3` int NULL,
                `c4` int NULL,
                `c5` int NULL,
                `c6` int NULL,
                `c7` int NULL,
                `c8` int NULL,
                `c9` int NULL)
                UNIQUE KEY(`c0`) DISTRIBUTED BY HASH(`c0`) BUCKETS 1
                PROPERTIES(
                    "replication_num" = "1",
                    "light_schema_change" = "false",
                    "enable_unique_key_merge_on_write" = "true") """

    sql "insert into ${tableName} values(1, 0, 0, 0, 0, 0, 0, 0, 0, 0);"
    sql "sync"
    qt_update_value_col_1 " select * from ${tableName} order by c0 "
    
    // schema change
    sql " ALTER table ${tableName} MODIFY COLUMN c2 double "
    // if timeout awaitility will raise exception
    Awaitility.await().atMost(try_times, TimeUnit.SECONDS).with().pollDelay(100, TimeUnit.MILLISECONDS).await().until(() -> {
        def res = sql " SHOW ALTER TABLE COLUMN WHERE TableName = '${tableName}' ORDER BY CreateTime DESC LIMIT 1 "
        if(res[0][9].toString() == "FINISHED"){
            return true;
        }
        return false;
    });
    sql "sync"

    // test insert data with update column
    sql "set enable_unique_key_partial_update=true;"
    sql "insert into ${tableName}(c0,c1,c2) values(1,1,1.0);"
    sql "set enable_unique_key_partial_update=false;"
    sql "sync"
    qt_update_value_col_2 " select * from ${tableName} order by c0 "

    sql """ DROP TABLE IF EXISTS ${tableName} """


    // test add key column
    tableName = "test_partial_update_insert_schema_change_add_key_column"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE ${tableName} (
                `c0` int NULL)
                UNIQUE KEY(`c0`) DISTRIBUTED BY HASH(`c0`) BUCKETS 1
                PROPERTIES(
                    "replication_num" = "1",
                    "light_schema_change" = "false",
                    "enable_unique_key_merge_on_write" = "true")"""
    sql "insert into ${tableName} values(1);"
    sql "sync"
    qt_add_key_col_1 " select * from ${tableName} order by c0; "
    
    // schema change
    sql """ ALTER table ${tableName} ADD COLUMN c1 int key default "0"; """
    // if timeout awaitility will raise exception
    Awaitility.await().atMost(try_times, TimeUnit.SECONDS).with().pollDelay(100, TimeUnit.MILLISECONDS).await().until(() -> {
        def res = sql " SHOW ALTER TABLE COLUMN WHERE TableName = '${tableName}' ORDER BY CreateTime DESC LIMIT 1 "
        if(res[0][9].toString() == "FINISHED"){
            return true;
        }
        return false;
    });
    sql "sync"

    sql " ALTER table ${tableName} ADD COLUMN c2 int null "
    // if timeout awaitility will raise exception
    Awaitility.await().atMost(try_times, TimeUnit.SECONDS).with().pollDelay(100, TimeUnit.MILLISECONDS).await().until(() -> {
        def res = sql " SHOW ALTER TABLE COLUMN WHERE TableName = '${tableName}' ORDER BY CreateTime DESC LIMIT 1 "
        if(res[0][9].toString() == "FINISHED"){
            return true;
        }
        return false;
    });
    sql "sync"

    sql " ALTER table ${tableName} ADD COLUMN c3 int null "
    // if timeout awaitility will raise exception
    Awaitility.await().atMost(try_times, TimeUnit.SECONDS).with().pollDelay(100, TimeUnit.MILLISECONDS).await().until(() -> {
        def res = sql " SHOW ALTER TABLE COLUMN WHERE TableName = '${tableName}' ORDER BY CreateTime DESC LIMIT 1 "
        if(res[0][9].toString() == "FINISHED"){
            return true;
        }
        return false;
    });
    sql "sync"

    // test insert data with all key column, should fail because
    // it don't have any value columns
    sql "set enable_unique_key_partial_update=true;"
    test {
        sql "insert into ${tableName}(c0,c1) values(1, 1);"
        exception "INTERNAL_ERROR"
    }
    sql "insert into ${tableName}(c0,c1,c2) values(1,0,10);"
    sql "set enable_unique_key_partial_update=false;"
    sql "sync"
    qt_add_key_col_2 " select * from ${tableName} order by c0; "

    sql """ DROP TABLE IF EXISTS ${tableName} """


    // test create index
    tableName = "test_partial_update_insert_schema_change_create_index"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE ${tableName} (
                `c0` int NULL,
                `c1` int NULL,
                `c2` int NULL,
                `c3` int NULL,
                `c4` int NULL,
                `c5` int NULL,
                `c6` int NULL,
                `c7` int NULL,
                `c8` int NULL,
                `c9` int NULL)
                UNIQUE KEY(`c0`) DISTRIBUTED BY HASH(`c0`) BUCKETS 1
                PROPERTIES(
                    "replication_num" = "1",
                    "light_schema_change" = "false",
                    "enable_unique_key_merge_on_write" = "true")
    """

    sql "insert into ${tableName} values(1, 0, 0, 0, 0, 0, 0, 0, 0, 0);"
    sql "sync"
    qt_create_index_1 " select * from ${tableName} order by c0 "

    
    sql " CREATE INDEX test ON ${tableName} (c1) USING BITMAP "
    // if timeout awaitility will raise exception
    Awaitility.await().atMost(try_times, TimeUnit.SECONDS).with().pollDelay(100, TimeUnit.MILLISECONDS).await().until(() -> {
        def res = sql " SHOW ALTER TABLE COLUMN WHERE TableName = '${tableName}' ORDER BY CreateTime DESC LIMIT 1 "
        if(res[0][9].toString() == "FINISHED"){
            return true;
        }
        return false;
    });
    sql "sync"
    
    //test insert data with create index
    sql "set enable_unique_key_partial_update=true;"
    sql "insert into ${tableName}(c0,c1,c2) values(1,1,1);"
    sql "set enable_unique_key_partial_update=false;"
    sql "sync"
    qt_create_index_2 " select * from ${tableName} order by c0 "
    sql """ DROP TABLE IF EXISTS ${tableName} """
}
