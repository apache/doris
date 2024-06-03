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

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

suite("test_load") {
    def tableName = "test_decimal_load"
    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
              `a` decimalv3(38,18)
            ) ENGINE=OLAP
            DUPLICATE KEY(`a`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`a`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
        """

        streamLoad {
            table "${tableName}"

            file 'test.csv'
            time 10000 // limit inflight 10s

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(3, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        sql """sync"""
        qt_select_default """ SELECT * FROM ${tableName} t ORDER BY a; """
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }

    sql """
        drop table if exists test_decimalv3_insert;
    """
    sql """
        CREATE TABLE `test_decimalv3_insert` (
            `k1` decimalv3(38, 6) null,
            `k2` decimalv3(38, 6) null
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    sql "set enable_insert_strict=true;"
    // overflow, max is inserted
    sql """
        insert into test_decimalv3_insert values("9999999999999999999999999999999999999999",1);
    """
    // underflow, min is inserted
    sql """
        insert into test_decimalv3_insert values("-9999999999999999999999999999999999999999",2);
    """
    sql """
        insert into test_decimalv3_insert values("99999999999999999999999999999999.9999991",3);
    """
    sql """
        insert into test_decimalv3_insert values("-99999999999999999999999999999999.9999991",4);
    """

    test {
        sql """
        insert into test_decimalv3_insert values("99999999999999999999999999999999.9999999",5);
        """
        exception "error"
    }
    test {
        sql """
        insert into test_decimalv3_insert values("-99999999999999999999999999999999.9999999",6);
        """
        exception "error"
    }
    qt_decimalv3_insert "select * from test_decimalv3_insert order by 1, 2;"

    // fix
    sql """ set enable_nereids_dml=true; """
    sql """
        drop TABLE if exists test_sys_update_basic_test_update_decimal_tb;
    """
    sql """
        CREATE TABLE test_sys_update_basic_test_update_decimal_tb (
          k1 DECIMAL(10, 5) NULL, 
          v1 DECIMAL(10, 5) NULL
        ) UNIQUE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 5 PROPERTIES (
          "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """
        insert into test_sys_update_basic_test_update_decimal_tb values
            (1.001, 2.002), (1.002, 0.00000002), (1.003, 0.100000001), (1.004, 0.100044001), (1.005, 0.100045001);
    """
    qt_decimalv3_underflow1 """
        select * from test_sys_update_basic_test_update_decimal_tb order by 1, 2;
    """
    sql """
        UPDATE test_sys_update_basic_test_update_decimal_tb SET v1="0.00000001" WHERE k1 = 1.001;
    """
    qt_decimalv3_underflow2 """
        select * from test_sys_update_basic_test_update_decimal_tb order by 1, 2;
    """

    sql """ set enable_nereids_dml=false; """
    sql """
        drop TABLE if exists test_sys_update_basic_test_update_decimal_tb;
    """
    sql """
        CREATE TABLE test_sys_update_basic_test_update_decimal_tb (
          k1 DECIMAL(10, 5) NULL, 
          v1 DECIMAL(10, 5) NULL
        ) UNIQUE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 5 PROPERTIES (
          "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """
        insert into test_sys_update_basic_test_update_decimal_tb values
            (1.001, 2.002), (1.002, 0.00000002), (1.003, 0.100000001), (1.004, 0.100044001), (1.005, 0.100045001);
    """
    qt_decimalv3_underflow3 """
        select * from test_sys_update_basic_test_update_decimal_tb order by 1, 2;
    """
    // need to use "1.001"(quoted) because non-nereids dml seems treat 1.001 as double,
    // which will cause wrong result of decimal comparision
    sql """
        UPDATE test_sys_update_basic_test_update_decimal_tb SET v1="0.00000001" WHERE k1 = "1.001";
    """
    qt_decimalv3_underflow4 """
        select * from test_sys_update_basic_test_update_decimal_tb order by 1, 2;
    """
}
