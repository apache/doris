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

suite("test_default_pi") {
    def tableName = "test_default_pi"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName}
        (
            k TINYINT,
            v1 DOUBLE NOT NULL DEFAULT PI,
            v2 INT
        )
        UNIQUE KEY(K)
        DISTRIBUTED BY HASH(k)
        PROPERTIES("replication_num" = "1");
    """

    // test insert into.
    sql " insert into ${tableName} (k, v2) values (1, 1); "
    sql " insert into ${tableName} (k, v2) values (2, 2); "
    sql " insert into ${tableName} (k, v2) values (3, 3); "
    sql " insert into ${tableName} (k, v2) values (4, 4); "
    sql "sync"
    qt_insert_into1 """ select * from ${tableName} order by k; """

    // test csv stream load.
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'k, v1=pi(), v2'

        file 'test_default_pi_streamload.csv'

        time 10000 // limit inflight 10s
    }

    sql "sync"

    qt_stream_load_csv1 """ select * from ${tableName} order by k; """

    // test partial update
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName}
        (
            k TINYINT,
            v1 DOUBLE NOT NULL DEFAULT PI,
            v2 INT
        )
        UNIQUE KEY(K)
        DISTRIBUTED BY HASH(k)
        PROPERTIES("replication_num" = "1");
    """

    sql "set enable_unique_key_partial_update=true;"
    sql "set enable_insert_strict=false;"

    sql " insert into ${tableName} (k, v2) values (1, 1); "
    sql " insert into ${tableName} (k, v2) values (2, 2); "
    sql " insert into ${tableName} (k, v2) values (3, 3); "
    sql " insert into ${tableName} (k, v2) values (4, 4); "
    sql "sync"

    qt_select_1 "select * from ${tableName} order by k;"

    streamLoad {
        table "${tableName}"

        set 'partial_columns', 'true'
        set 'column_separator', ','
        set 'columns', 'k, v1=pi(), v2'

        file 'test_default_pi_streamload.csv'

        time 10000 // limit inflight 10s
    }

    sql "sync"

    qt_stream_load_csv1 """ select * from ${tableName} order by k; """

    // test varchar with default pi
    sql "DROP TABLE IF EXISTS test_varchar_default"
    test {
        sql """create table test_varchar_default(a int, b varchar(100) default pi)
        distributed by hash(a) properties('replication_num'="1");"""
        exception "Types other than DOUBLE cannot use pi as the default value"
    }

    // test int with default pi
    sql "DROP TABLE IF EXISTS test_int_default"
    test {
        sql """create table test_int_default(a int, b int default pi)
        distributed by hash(a) properties('replication_num'="1");"""
        exception "Types other than DOUBLE cannot use pi as the default value"
    }

    // test float with default pi
    sql "DROP TABLE IF EXISTS test_double_default"
    test {
        sql """create table test_int_default(a int, b float default pi)
        distributed by hash(a) properties('replication_num'="1");"""
        exception "Types other than DOUBLE cannot use pi as the default value"
    }

}
