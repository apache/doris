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

suite("test_current_date") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    def tableName = "test_current_date"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName}
        (
            id TINYINT,
            name CHAR(10) NOT NULL DEFAULT "zs",
            dt_0 DATE,
            dt_2 DATEV2,
            dt_1 DATE DEFAULT current_date,
            dt_3 DATEV2 DEFAULT current_date,
 
        )
        COMMENT "test current_date table"
        DISTRIBUTED BY HASH(id)
        PROPERTIES("replication_num" = "1");
    """

    // test insert into.
    sql " insert into ${tableName} (id,name,dt_0,dt_2) values (1,'aa',current_date(),current_date()); "
    sql " insert into ${tableName} (id,name,dt_0,dt_2) values (2,'bb',current_date(),current_date()); "
    sql " insert into ${tableName} (id,name,dt_0,dt_2) values (3,'cc',current_date(),current_date()); "
    sql " insert into ${tableName} (id,name,dt_0,dt_2) values (4,'dd',current_date(),current_date()); "
    sql "sync"
    qt_insert_into1 """ select count(*) from ${tableName} where dt_0 = dt_1; """
    qt_insert_into2 """ select count(*) from ${tableName} where dt_2 = dt_3; """

    sql """select now()"""

    // test csv stream load.
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'id, name, dt_0 = current_date(), dt_2 = current_date()'

        file 'test_current_timestamp_streamload.csv'

        time 10000 // limit inflight 10s
    }

    sql "sync"

    qt_stream_load_csv1 """ select count(*) from ${tableName} where id > 4 and dt_0 = dt_1; """
    qt_stream_load_csv2 """ select count(*) from ${tableName} where id > 4 and dt_2 = dt_3; """

    // test varchar with default current_date
    sql "DROP TABLE IF EXISTS test_varchar_default"
    test {
        sql """create table test_varchar_default(a int, b varchar(100) default current_date)
        distributed by hash(a) properties('replication_num'="1");"""
        exception "Types other than DATE and DATEV2 cannot use current_date as the default value"
    }

    // test int with default current_date
    sql "DROP TABLE IF EXISTS test_int_default"
    test {
        sql """create table test_int_default(a int, b int default current_date)
        distributed by hash(a) properties('replication_num'="1");"""
        exception "Types other than DATE and DATEV2 cannot use current_date as the default value"
    }

    // test double with default current_date
    sql "DROP TABLE IF EXISTS test_double_default"
    test {
        sql """create table test_int_default(a int, b double default current_date)
        distributed by hash(a) properties('replication_num'="1");"""
        exception "Types other than DATE and DATEV2 cannot use current_date as the default value"
    }

}