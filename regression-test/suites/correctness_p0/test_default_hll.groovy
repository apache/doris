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

suite("test_default_hll") {
    def tableName = "test_default_hll"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName}
        (
            k TINYINT,
            v1 DECIMAL(10, 2) DEFAULT "0",
            h1 hll NOT NULL COMMENT "hll column"
        )
        UNIQUE KEY(K)
        DISTRIBUTED BY HASH(k)
        PROPERTIES("replication_num" = "1");
    """

    // test insert into.
    sql " insert into ${tableName} (k, v1, h1) values (1, 1, hll_empty()); "
    sql " insert into ${tableName} (k, v1, h1) values (2, 2, hll_empty()); "
    sql " insert into ${tableName} (k, v1, h1) values (3, 3, hll_empty()); "
    sql " insert into ${tableName} (k, v1, h1) values (4, 4, hll_empty()); "
    sql "sync"
    qt_insert_into1 """ select HLL_CARDINALITY(h1) from ${tableName} order by k; """

    // test csv stream load.
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'k, v1, h1=hll_hash(k)'

        file 'test_default_hll_streamload.csv'

        time 10000 // limit inflight 10s
    }

    sql "sync"

    qt_stream_load_csv1 """ select HLL_CARDINALITY(h1) from ${tableName} order by k; """

    // test partial update
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName}
        (
            k TINYINT,
            v1 DECIMAL(10, 2) DEFAULT "0",
            h1 hll NOT NULL COMMENT "hll column"
        )
        UNIQUE KEY(K)
        DISTRIBUTED BY HASH(k)
        PROPERTIES("replication_num" = "1");
    """

    sql "set enable_unique_key_partial_update=true;" 
    sql "set enable_insert_strict=false;"

    sql " insert into ${tableName} (k, v1) values (1, 1); "
    sql " insert into ${tableName} (k, v1) values (2, 2); "
    sql " insert into ${tableName} (k, v1) values (3, 3); "
    sql " insert into ${tableName} (k, v1) values (4, 4); "
    sql "sync"

    qt_select_1 "select HLL_CARDINALITY(h1) from ${tableName} order by k;"

    streamLoad {
        table "${tableName}"

        set 'partial_columns', 'true'
        set 'column_separator', ','
        set 'columns', 'k, v1'

        file 'test_default_hll_streamload.csv'

        time 10000 // limit inflight 10s
    }

    sql "sync"

    qt_stream_load_csv1 """ select HLL_CARDINALITY(h1) from ${tableName} order by k; """

} 