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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite ("test_modify_reorder_column") {

    def getJobState = { tableName ->
         def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
         return jobStateResult[0][9]
    }

    // test reorder column on duplicate table
    def tbl1 = "test_modify_reorder_column_dup"
    sql """ DROP TABLE IF EXISTS ${tbl1}; """
    sql """ create table ${tbl1} (
            k1 TINYINT,
            colnotnull STRUCT<f1: varchar(65533), f2: char(32), f3: int, f4: double> NOT NULL,
            colnull STRUCT<f1: varchar(65533), f2: char(32), f3: int, f4: double> NULL,
            v VARIANT
        ) duplicate key(k1) distributed by hash(k1) buckets 1
        properties( "replication_num" = "1" ); """

    sql """insert into ${tbl1} values
        (1, {"A", "B", 10, 3.14}, {"C", "D", 20, 8.343}, '{"a" : 1, "b" : [1], "c": 1.0}') """
    qt_dup """ select * from ${tbl1} order by k1;"""

    sql "ALTER TABLE ${tbl1} MODIFY COLUMN colnull STRUCT<f1: varchar(65533), f2: char(32), f3: int, f4: double> NULL AFTER v"


    int max_try_time = 100
    while (max_try_time--){
        String result = getJobState("${tbl1}")
        if (result == "FINISHED") {
            sleep(1000)
            break
        } else {
            sleep(1000)
            assertTrue(max_try_time>1)
        }
    }

    sql """insert into ${tbl1} values
        (2, {"E", "F", 30, 484.3234}, '{"a" : 1, "b" : [1], "c": 1.0}', null) """
    qt_dup """ select * from ${tbl1} order by k1;"""
    sql """DROP TABLE IF EXISTS ${tbl1} FORCE; """


    // test reorder column on MOR table
    def tbl2 = "test_modify_reorder_column_mor"
    sql """ DROP TABLE IF EXISTS ${tbl2}; """
    sql """ create table ${tbl2} (
            k1 TINYINT,
            colnotnull STRUCT<f1: varchar(65533), f2: char(32), f3: int, f4: double> NOT NULL,
            colnull STRUCT<f1: varchar(65533), f2: char(32), f3: int, f4: double> NULL,
            v VARIANT
        ) unique key(k1) distributed by hash(k1) buckets 1
        properties( "replication_num" = "1", "enable_unique_key_merge_on_write" = "false" ); """

    sql """insert into ${tbl2} values
        (1, {"A", "B", 10, 3.14}, {"C", "D", 20, 8.343}, '{"a" : 1, "b" : [1], "c": 1.0}') """
    qt_mor """ select * from ${tbl2} order by k1;"""

    sql "ALTER TABLE ${tbl2} MODIFY COLUMN colnull STRUCT<f1: varchar(65533), f2: char(32), f3: int, f4: double> NULL AFTER v"

    max_try_time = 100
    while (max_try_time--){
        String result = getJobState("${tbl2}")
        if (result == "FINISHED") {
            sleep(1000)
            break
        } else {
            sleep(1000)
            assertTrue(max_try_time>1)
        }
    }

    sql """insert into ${tbl2} values
        (2, {"E", "F", 30, 484.3234}, '{"a" : 1, "b" : [1], "c": 1.0}', null) """
    qt_mor """ select * from ${tbl2} order by k1;"""
    sql """DROP TABLE IF EXISTS ${tbl2} FORCE; """


    // test reorder column on MOW table
    def tbl3 = "test_modify_reorder_column_mow"
    sql """ DROP TABLE IF EXISTS ${tbl3}; """
    sql """ create table ${tbl3} (
            k1 TINYINT,
            colnotnull STRUCT<f1: varchar(65533), f2: char(32), f3: int, f4: double> NOT NULL,
            colnull STRUCT<f1: varchar(65533), f2: char(32), f3: int, f4: double> NULL,
            v VARIANT
        ) unique key(k1) distributed by hash(k1) buckets 1
        properties( "replication_num" = "1", "enable_unique_key_merge_on_write" = "false" ); """

    sql """insert into ${tbl3} values
        (1, {"A", "B", 10, 3.14}, {"C", "D", 20, 8.343}, '{"a" : 1, "b" : [1], "c": 1.0}') """
    qt_mow """ select * from ${tbl3} order by k1;"""

    sql "ALTER TABLE ${tbl3} MODIFY COLUMN colnull STRUCT<f1: varchar(65533), f2: char(32), f3: int, f4: double> NULL AFTER v"

    max_try_time = 100
    while (max_try_time--){
        String result = getJobState("${tbl3}")
        if (result == "FINISHED") {
            sleep(1000)
            break
        } else {
            sleep(1000)
            assertTrue(max_try_time>1)
        }
    }

    sql """insert into ${tbl3} values
        (2, {"E", "F", 30, 484.3234}, '{"a" : 1, "b" : [1], "c": 1.0}', null) """
    qt_mow """ select * from ${tbl3} order by k1;"""
    sql """DROP TABLE IF EXISTS ${tbl3} FORCE; """
}
