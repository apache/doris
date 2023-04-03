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

suite("test_load_overflow") {
    def dbName = "test_load"
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "USE $dbName"

    def tableName = "test_decimal_load_overflow"
    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
              `a` decimalv3(16,2)
            ) ENGINE=OLAP
            DUPLICATE KEY(`a`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`a`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
        """

        StringBuilder commandBuilder = new StringBuilder()
        commandBuilder.append("""curl --location-trusted -u ${context.config.feHttpUser}:${context.config.feHttpPassword}""")
        commandBuilder.append(""" -H format:csv -T ${context.file.parent}/test_data/test_overflow.csv http://${context.config.feHttpAddress}/api/""" + dbName + "/" + tableName + "/_stream_load")
        command = commandBuilder.toString()
        process = command.execute()
        code = process.waitFor()
        err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())))
        out = process.getText()
        logger.info("Run command: command=" + command + ",code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        qt_select_default """ SELECT * FROM ${tableName} t ORDER BY a; """

        test {
            sql """ insert into ${tableName} select "3957417026770075.00"; """
            check{result, exception, startTime, endTime ->
                assertTrue(exception != null)
                logger.info(exception.message)
            }
        }
        qt_select_default """ SELECT * FROM ${tableName} t ORDER BY a; """
        sql """ insert into ${tableName} select "39574170267700.75"; """
        qt_select_default """ SELECT * FROM ${tableName} t ORDER BY a; """
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
