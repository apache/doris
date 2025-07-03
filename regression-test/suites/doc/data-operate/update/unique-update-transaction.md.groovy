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

suite("docs/data-operate/update/unique-update-transaction.md") {
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
            CREATE DATABASE IF NOT EXISTS test; USE test;
            DROP TABLE IF EXISTS test.test_table;
            CREATE TABLE test.test_table
            (
                user_id bigint,
                date date,
                group_id bigint,
                modify_date date,
                keyword VARCHAR(128)
            )
            UNIQUE KEY(user_id, date, group_id)
            DISTRIBUTED BY HASH (user_id) BUCKETS 32
            PROPERTIES(
                "function_column.sequence_col" = 'modify_date',
                "replication_num" = "1",
                "in_memory" = "false"
            );
        """
        sql "desc test_table;"
        writeToFile("${context.file.parent}/testData", """\
1	2020-02-22	1	2020-02-21	a
1	2020-02-22	1	2020-02-22	b
1	2020-02-22	1	2020-03-05	c
1	2020-02-22	1	2020-02-26	d
1	2020-02-22	1	2020-02-23	e
1	2020-02-22	1	2020-02-24	b
""")
        cmd """curl --location-trusted -u ${context.config.jdbcUser}:${context.config.jdbcPassword} -T ${context.file.parent}/testData http://${context.config.feHttpAddress}/api/test/test_table/_stream_load"""
        qt_sql "select * from test_table;"

        writeToFile("${context.file.parent}/testData", """\
1	2020-02-22	1	2020-02-22	a
1	2020-02-22	1	2020-02-23	b
""")
        cmd """curl --location-trusted -u ${context.config.jdbcUser}:${context.config.jdbcPassword} -T ${context.file.parent}/testData http://${context.config.feHttpAddress}/api/test/test_table/_stream_load"""
        qt_sql "select * from test_table;"

        writeToFile("${context.file.parent}/testData", """\
1	2020-02-22	1	2020-02-22	a
1	2020-02-22	1	2020-03-23	w
""")
        cmd """curl --location-trusted -u ${context.config.jdbcUser}:${context.config.jdbcPassword} -T ${context.file.parent}/testData http://${context.config.feHttpAddress}/api/test/test_table/_stream_load"""
        qt_sql "select * from test_table;"

    } catch (Throwable t) {
        Assertions.fail("examples in docs/data-operate/update/unique-update-transaction.md failed to exec, please fix it", t)
    }
}
