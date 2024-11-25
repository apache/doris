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

import org.junit.jupiter.api.Assertions;

suite("docs/data-operate/delete/batch-delete-manual.md") {
    def writeToFile = {String path, String data ->
        OutputStreamWriter w = null
        try {
            w = new OutputStreamWriter(new FileOutputStream(path))
            w.write(data)
            w.flush()
            w.close()
        } finally {
            if (w != null) w.close()
        }
    }
    try {
        multi_sql """
            CREATE DATABASE IF NOT EXISTS test;
            USE test;
            DROP TABLE IF EXISTS table1;
            CREATE TABLE IF NOT EXISTS table1 (
                siteid INT,
                citycode INT,
                username VARCHAR(64),
                pv BIGINT
            ) UNIQUE KEY (siteid, citycode, username)
            DISTRIBUTED BY HASH(siteid) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            );
        """
        multi_sql """
            SET show_hidden_columns=true;
            DESC table1;
            SET show_hidden_columns=false;
        """

        writeToFile("${context.file.parent}/table1_data", """\
3,2,tom,2
4,3,bush,3
5,3,helen,3
""")
        println (cmd """curl --location-trusted -u ${context.config.jdbcUser}:${context.config.jdbcPassword} -H "column_separator:," -H "columns: siteid, citycode, username, pv" -H "merge_type: APPEND"  -T ${context.file.parent}/table1_data http://${context.config.feHttpAddress}/api/test/table1/_stream_load""")
        println (cmd """curl --location-trusted -u ${context.config.jdbcUser}:${context.config.jdbcPassword} -H "column_separator:," -H "columns: siteid, citycode, username, pv" -T ${context.file.parent}/table1_data http://${context.config.feHttpAddress}/api/test/table1/_stream_load""")
        order_qt_sql "SELECT * FROM test.table1"

        writeToFile("${context.file.parent}/table1_data", """\
3,2,tom,0
""")
        println (cmd """curl --location-trusted -u ${context.config.jdbcUser}:${context.config.jdbcPassword} -H "column_separator:," -H "columns: siteid, citycode, username, pv" -H "merge_type: DELETE"  -T ${context.file.parent}/table1_data http://${context.config.feHttpAddress}/api/test/table1/_stream_load""")
        order_qt_sql "SELECT * FROM test.table1"

        multi_sql """
            TRUNCATE TABLE table1;
            INSERT INTO table1(siteid, citycode, username, pv) VALUES
                              (     4,        3, 'bush'  ,  3),
                              (     5,        3, 'helen' ,  3),
                              (     1,        1, 'jim'   ,  2);
        """
        writeToFile("${context.file.parent}/table1_data", """\
2,1,grace,2
3,2,tom,2
1,1,jim,2
""")
        println (cmd """curl --location-trusted -u ${context.config.jdbcUser}:${context.config.jdbcPassword} -H "column_separator:," -H "columns: siteid, citycode, username, pv" -H "merge_type: MERGE" -H "delete: siteid=1"  -T ${context.file.parent}/table1_data http://${context.config.feHttpAddress}/api/test/table1/_stream_load""")
        order_qt_sql "SELECT * FROM test.table1"

        multi_sql """
            DROP TABLE IF EXISTS table1;
            CREATE TABLE IF NOT EXISTS table1 (
                name VARCHAR(100) NOT NULL,
                gender VARCHAR(10),
                age INT,
                pv BIGINT
            ) UNIQUE KEY (name)
            DISTRIBUTED BY HASH(name) BUCKETS 1
            PROPERTIES (
                "function_column.sequence_col" = "age",
                "replication_num" = "1"
            );
            INSERT INTO table1(name, gender, age) VALUES
                              ('li', 'male', 10),
                              ('wang', 'male', 14),
                              ('zhang', 'male', 12);
            SET show_hidden_columns=true;
            DESC table1;
            SET show_hidden_columns=false;
        """
        writeToFile("${context.file.parent}/table1_data", """\
li,male,10
""")
        println (cmd """curl --location-trusted -u ${context.config.jdbcUser}:${context.config.jdbcPassword} -H "column_separator:," -H "columns: name, gender, age" -H "function_column.sequence_col: age" -H "merge_type: DELETE"  -T ${context.file.parent}/table1_data http://${context.config.feHttpAddress}/api/test/table1/_stream_load""")
        order_qt_sql "SELECT * FROM test.table1"

        sql """INSERT INTO table1(name, gender, age) VALUES ('li', 'male', 10);"""
        writeToFile("${context.file.parent}/table1_data", """\
li,male,9
""")
        println (cmd """curl --location-trusted -u ${context.config.jdbcUser}:${context.config.jdbcPassword} -H "column_separator:," -H "columns: name, gender, age" -H "function_column.sequence_col: age" -H "merge_type: DELETE"  -T ${context.file.parent}/table1_data http://${context.config.feHttpAddress}/api/test/table1/_stream_load""")
        order_qt_sql "SELECT * FROM test.table1"
    } catch (Throwable t) {
        Assertions.fail("examples in docs/data-operate/delete/batch-delete-manual.md failed to exec, please fix it", t)
    }
}
