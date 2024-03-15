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

suite("test_decimalv2_load", "nonConcurrent") {
    def dbName = "test_decimalv2_load_db"
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "USE $dbName"

    sql """
        admin set frontend config("enable_decimal_conversion" = "false");
    """
    sql "set check_overflow_for_decimal=false;"

    def tableName = "test_decimalv2_load_tbl"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
    CREATE TABLE IF NOT EXISTS ${tableName} (
          `a` decimalv2(8, 5)
        ) ENGINE=OLAP
        DUPLICATE KEY(`a`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`a`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
    );
    """

    streamLoad {
        db dbName
        table tableName
        file "${context.file.parent}/test_data/test.csv"
        time 30000

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals(6, json.NumberTotalRows)
            assertEquals(6, json.NumberLoadedRows)
        }
    }

    sql """ sync; """

    qt_query1 """
        select * from ${tableName} order by 1;
    """

    def tableName2 = "test_decimalv2_load_tbl2"
    sql """ DROP TABLE IF EXISTS ${tableName2} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName2} (
              `a` decimalv2(8, 5)
            ) ENGINE=OLAP
            DUPLICATE KEY(`a`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`a`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        insert into ${tableName2} select * from ${tableName};
    """

    qt_query2 """
        select * from ${tableName2} order by 1;
    """

    sql """
        drop table if exists test_decimalv2_insert;
    """
    sql """
        CREATE TABLE `test_decimalv2_insert` (
            `k1` decimalv2(27, 9) null,
            `k2` decimalv2(27, 9) null
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    sql "set enable_insert_strict=true;"
    // overflow, max is inserted
    sql """
        insert into test_decimalv2_insert values("999999999999999999999999999999",1);
    """
    // underflow, min is inserted
    sql """
        insert into test_decimalv2_insert values("-999999999999999999999999999999",2);
    """
    sql """
        insert into test_decimalv2_insert values("999999999999999999.9999999991",3);
    """
    sql """
        insert into test_decimalv2_insert values("-999999999999999999.9999999991",4);
    """
    sql """
        insert into test_decimalv2_insert values("999999999999999999.9999999995",5);
    """
    sql """
        insert into test_decimalv2_insert values("-999999999999999999.9999999995",6);
    """
    qt_decimalv2_insert "select * from test_decimalv2_insert order by 2; "

    sql """
        admin set frontend config("enable_decimal_conversion" = "true");
    """
}
