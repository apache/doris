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

suite("test_current_timestamp") {
    def tableName = "test_current_timestamp"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName}
        (
            id TINYINT,
            name CHAR(10) NOT NULL DEFAULT "zs",
            dt_0 DATETIME,
            dt_2 DATETIMEV2,
            dt_4 DATETIMEV2(3),
            dt_6 DATETIMEV2(6),
            dt_1 DATETIME DEFAULT CURRENT_TIMESTAMP,
            dt_3 DATETIMEV2 DEFAULT CURRENT_TIMESTAMP,
            dt_5 DATETIMEV2(3) DEFAULT CURRENT_TIMESTAMP,
            dt_7 DATETIMEV2(6) DEFAULT CURRENT_TIMESTAMP
        )
        COMMENT "test current_timestamp table"
        DISTRIBUTED BY HASH(id)
        PROPERTIES("replication_num" = "1");
    """

    def tableName2 = "test_current_timestamp2"

    sql """ DROP TABLE IF EXISTS ${tableName2} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName2}
        (
            id TINYINT,
            name CHAR(10) NOT NULL DEFAULT "zs",
            dt_1 DATETIME DEFAULT CURRENT_TIMESTAMP,
            dt_2 DATETIMEV2 DEFAULT CURRENT_TIMESTAMP
        )
        COMMENT "test current_timestamp table2"
        DISTRIBUTED BY HASH(id)
        PROPERTIES("replication_num" = "1");
    """
    
    // test insert into.
    sql " insert into ${tableName} (id,name,dt_0,dt_2,dt_4,dt_6) values (1,'aa',current_timestamp(),current_timestamp(),current_timestamp(),current_timestamp()); "
    sql " insert into ${tableName} (id,name,dt_0,dt_2,dt_4,dt_6) values (2,'bb',current_timestamp(),current_timestamp(),current_timestamp(),current_timestamp()); "
    sql " insert into ${tableName} (id,name,dt_0,dt_2,dt_4,dt_6) values (3,'cc',current_timestamp(),current_timestamp(),current_timestamp(),current_timestamp()); "
    sql " insert into ${tableName} (id,name,dt_0,dt_2,dt_4,dt_6) values (4,'dd',current_timestamp(),current_timestamp(),current_timestamp(),current_timestamp()); "

    qt_insert_into1 """ select count(*) from ${tableName} where to_date(dt_0) = to_date(dt_1); """
    qt_insert_into2 """ select count(*) from ${tableName} where to_date(dt_2) = to_date(dt_3); """
    qt_insert_into3 """ select count(*) from ${tableName} where to_date(dt_4) = to_date(dt_5); """
    qt_insert_into4 """ select count(*) from ${tableName} where to_date(dt_6) = to_date(dt_7); """

    sql """select now()"""

    // test csv stream load.
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'id, name, dt_0 = current_timestamp(), dt_2 = current_timestamp(), dt_4 = current_timestamp(), dt_6 = current_timestamp()'

        file 'test_current_timestamp_streamload.csv'

        time 10000 // limit inflight 10s
    }
    qt_stream_load_csv1 """ select count(*) from ${tableName} where id > 4 and to_date(dt_0) = to_date(dt_1); """
    qt_stream_load_csv2 """ select count(*) from ${tableName} where id > 4 and to_date(dt_2) = to_date(dt_3); """
    qt_stream_load_csv3 """ select count(*) from ${tableName} where id > 4 and to_date(dt_4) = to_date(dt_5); """
    qt_stream_load_csv4 """ select count(*) from ${tableName} where id > 4 and to_date(dt_6) = to_date(dt_7); """

    sql """select now()"""

    // test json stream load
    streamLoad {
        table "${tableName}"

        set 'columns', 'id, name, dt_0 = current_timestamp(), dt_2 = current_timestamp(), dt_4 = current_timestamp(), dt_6 = current_timestamp()'
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'strip_outer_array', 'false'

        file 'test_current_timestamp_streamload.json'

        time 10000 // limit inflight 10s
    }
    qt_stream_load_json1 """ select count(*) from ${tableName} where id > 8 and to_date(dt_0) = to_date(dt_1); """
    qt_stream_load_json2 """ select count(*) from ${tableName} where id > 8 and to_date(dt_2) = to_date(dt_3); """
    qt_stream_load_json3 """ select count(*) from ${tableName} where id > 8 and to_date(dt_4) = to_date(dt_5); """
    qt_stream_load_json4 """ select count(*) from ${tableName} where id > 8 and to_date(dt_6) = to_date(dt_7); """

    // test json stream load 2
    // if we set column param but the column do not exist in json file
    // stream load json file will set it to NULL, just like dt_1
    streamLoad {
        table "${tableName2}"

        set 'columns', 'id, name, dt_1'
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'strip_outer_array', 'false'

        file 'test_current_timestamp_streamload.json'
        time 10000 // limit inflight 10s
    }
    qt_stream_load_json5 """ select id, name, dt_1  from ${tableName2} order by id; """
    qt_stream_load_json6 """ select count(*) from ${tableName2} where dt_2 is not NULL; """
 }
