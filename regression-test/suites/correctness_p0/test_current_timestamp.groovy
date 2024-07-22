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

    sql "sync"

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

    sql "sync"

    qt_stream_load_json1 """ select count(*) from ${tableName} where id > 8 and to_date(dt_0) = to_date(dt_1); """
    qt_stream_load_json2 """ select count(*) from ${tableName} where id > 8 and to_date(dt_2) = to_date(dt_3); """
    qt_stream_load_json3 """ select count(*) from ${tableName} where id > 8 and to_date(dt_4) = to_date(dt_5); """
    qt_stream_load_json4 """ select count(*) from ${tableName} where id > 8 and to_date(dt_6) = to_date(dt_7); """

    // test json stream load 2
    // if we set column param but the column do not exist in json file
    // stream load json file will set it to default value, just like dt_1
    streamLoad {
        table "${tableName2}"

        set 'columns', 'id, name, dt_1'
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'strip_outer_array', 'false'

        file 'test_current_timestamp_streamload.json'
        time 10000 // limit inflight 10s
    }

    sql "sync"

    qt_stream_load_json5 """ select id, name from ${tableName2} where dt_1 is not NULL order by id; """
    qt_stream_load_json6 """ select count(*) from ${tableName2} where dt_2 is not NULL; """

    def tableName3 = "test_current_timestamp_ms"

    sql """ DROP TABLE IF EXISTS ${tableName3} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName3}
        (
            id TINYINT,
            name CHAR(10) NOT NULL DEFAULT "zs",
            dt_2 DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3)
        )
        COMMENT "test current_timestamp table3"
        DISTRIBUTED BY HASH(id)
        PROPERTIES("replication_num" = "1");
    """

    // test insert into.
    qt_insert_sql " insert into ${tableName3} (id,name,dt_2) values (1,'aa',current_timestamp(3)); "
    qt_insert_sql " insert into ${tableName3} (id,name) values (2,'bb'); "

    qt_select_sql """ select count(*) from ${tableName3} where to_date(dt_2) = to_date(current_timestamp(3)); """

    // test csv stream load.
    streamLoad {
        table "${tableName3}"

        set 'column_separator', ','
        set 'columns', 'id, name, dt_2 = current_timestamp(3)'

        file 'test_current_timestamp_streamload.csv'

        time 10000 // limit inflight 10s
    }

    sql "sync"

    // test json stream load
    streamLoad {
        table "${tableName3}"

        set 'columns', 'id, name, dt_2 = current_timestamp(3)'
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'strip_outer_array', 'false'

        file 'test_current_timestamp_streamload.json'

        time 10000 // limit inflight 10s
    }

    sql "sync"

    // test create
    def tableName4 = "test_current_timestamp_ms2"
    test {
        sql """ DROP TABLE IF EXISTS ${tableName4} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName4}
            (
                id TINYINT,
                name CHAR(10) NOT NULL DEFAULT "zs",
                dt_2 DATETIME(1) DEFAULT CURRENT_TIMESTAMP(3)
            )
            COMMENT "test current_timestamp table4"
            DISTRIBUTED BY HASH(id)
            PROPERTIES("replication_num" = "1");
        """
        exception "errCode = 2, detailMessage = default value precision: CURRENT_TIMESTAMP(3) can not be greater than type precision: datetimev2(1)"
    }

    // test create
    def tableName5 = "test_current_timestamp_ms3"
    test {
        sql """ DROP TABLE IF EXISTS ${tableName5} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName5}
            (
                id TINYINT,
                name CHAR(10) NOT NULL DEFAULT "zs",
                dt_2 DATETIME(6) DEFAULT CURRENT_TIMESTAMP(7)
            )
            COMMENT "test current_timestamp table5"
            DISTRIBUTED BY HASH(id)
            PROPERTIES("replication_num" = "1");
        """
        exception "between 0 and 6"
    }

    // user case
    def tableName6 = "test_current_timestamp_4"
    sql """ DROP TABLE IF EXISTS ${tableName6} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName6}
        (
            `id` int,
            `date_time` datetime(3),
            `data_entry_time` datetime(3) NULL DEFAULT CURRENT_TIMESTAMP(3)
        ) DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(id)
        PROPERTIES
        (
            "replication_num" = "1"
        );
    """

    streamLoad {
        table "${tableName6}"
        set 'columns', 'id,date_time'
        set 'format', 'csv'
        set 'column_separator', ','

        file "test_current_timestamp_dft.csv"

        time 10000
    }

    sql "sync"
    qt_select """ select count(*) from ${tableName6} """

    def tableName7 = "test_current_timestamp_5"
    sql """ DROP TABLE IF EXISTS ${tableName7} """
    sql """
    CREATE TABLE ${tableName7} (
       id int,
       `name` varchar(50) NULL,
       input_time datetime default current_timestamp
    ) ENGINE=OLAP
    DUPLICATE KEY(`id`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`id`) BUCKETS 10
    PROPERTIES (
    "in_memory" = "false",
    "storage_format" = "V2",
    "replication_num" = "1"
    );
    """

    streamLoad {
        table "${tableName7}"
        set 'columns', 'id,name'
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'strip_outer_array', 'false'

        file "test_current_timestamp_dft.json"

        time 10000
    }

    sql "sync"
    qt_select """ select count(*) from ${tableName7} """

}
