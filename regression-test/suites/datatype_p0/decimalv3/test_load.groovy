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
    def dbName = "test_load"
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "USE $dbName"

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

        StringBuilder commandBuilder = new StringBuilder()
        commandBuilder.append("""curl --max-time 5 --location-trusted -u ${context.config.feHttpUser}:${context.config.feHttpPassword}""")
        commandBuilder.append(""" -H format:csv -T ${context.file.parent}/test_data/test.csv http://${context.config.feHttpAddress}/api/""" + dbName + "/" + tableName + "/_stream_load")
        command = commandBuilder.toString()
        process = command.execute()
        code = process.waitFor()
        err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())))
        out = process.getText()
        logger.info("Run command: command=" + command + ",code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)

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
}
