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

suite("test_alter_table_column_nullable") {
    def tbName = "alter_table_column_nullable"

    def getJobState = { tableName ->
        def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
        return jobStateResult[0][9]
    }

    sql "DROP TABLE IF EXISTS ${tbName}"
    // char not null to null
    sql """
            CREATE TABLE ${tbName} (
                k1 INT NOT NULL,
                value1 varchar(16) NOT NULL
            )
            DUPLICATE KEY (k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1 properties("replication_num" = "1");
        """
    StringBuilder insertCommand = new StringBuilder();
    insertCommand.append("INSERT INTO ${tbName} VALUES ");
    // insert row count: 4096 * 2 + 1 = 8193, 3 blocks
    int insert_row_count = 8193;
    while (insert_row_count-- > 1) {
        insertCommand.append("(1, '11'),");
    }
    insertCommand.append("(1, '11')");
    sql insertCommand.toString()

    // not nullable to nullable
    sql """alter table ${tbName} modify column value1 varchar(128) NULL""";

    int max_try_secs = 60
    while (max_try_secs--) {
        String res = getJobState(tbName)
        if (res == "FINISHED") {
            break
        } else {
            Thread.sleep(2000)
            if (max_try_secs < 1) {
                println "test timeout," + "state:" + res
                assertEquals("FINISHED",res)
            }
        }
    }

    qt_select_char """select * from ${tbName} limit 1"""

    // int not null to null, linked schema change
    sql "DROP TABLE ${tbName}"
    sql """
            CREATE TABLE ${tbName} (
                k1 INT NOT NULL,
                value1 INT NOT NULL
            )
            DUPLICATE KEY (k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1 properties("replication_num" = "1");
        """
    insertCommand = new StringBuilder();
    insertCommand.append("INSERT INTO ${tbName} VALUES ");
    while (insert_row_count-- > 1) {
        insertCommand.append("(1, 11),");
    }
    insertCommand.append("(1, 11)");
    sql insertCommand.toString()

    sql """alter table ${tbName} modify column value1 INT NULL""";

    max_try_secs = 60
    while (max_try_secs--) {
        String res = getJobState(tbName)
        if (res == "FINISHED") {
            break
        } else {
            Thread.sleep(2000)
            if (max_try_secs < 1) {
                println "test timeout," + "state:" + res
                assertEquals("FINISHED",res)
            }
        }
    }

    qt_select_int """select * from ${tbName} limit 1"""

    // char not null to int not null, data loss
    sql "DROP TABLE ${tbName}"
    sql """
            CREATE TABLE ${tbName} (
                k1 INT NOT NULL,
                value1 varchar(16) NOT NULL
            )
            DUPLICATE KEY (k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1 properties("replication_num" = "1");
        """
    sql "INSERT INTO ${tbName} VALUES (1, 'zz')";

    // the cast expr of schema change is `CastExpr(CAST String to Nullable(Int32))`,
    sql """alter table ${tbName} modify column value1 INT NOT NULL"""

    max_try_secs = 60
    while (max_try_secs--) {
        String res = getJobState(tbName)
        if (res == "CANCELLED") {
            break
        } else {
            Thread.sleep(2000)
            if (max_try_secs < 1) {
                println "test timeout," + "state:" + res
                assertEquals("CANCELLED",res)
            }
        }
    }
    qt_select_char_to_int_fail """select * from ${tbName}"""

    // char not null to int not null OK
    sql "DROP TABLE ${tbName}"
    sql """
            CREATE TABLE ${tbName} (
                k1 INT NOT NULL,
                value1 varchar(16) NOT NULL
            )
            DUPLICATE KEY (k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1 properties("replication_num" = "1");
        """
    insertCommand = new StringBuilder();
    insertCommand.append("INSERT INTO ${tbName} VALUES ");
    while (insert_row_count-- > 1) {
        insertCommand.append("(1, '11'),");
    }
    insertCommand.append("(1, '11')");
    sql insertCommand.toString()

    // the cast expr of schema change is `CastExpr(CAST String to Nullable(Int32))`,
    sql """alter table ${tbName} modify column value1 INT NOT NULL""";

    max_try_secs = 60
    while (max_try_secs--) {
        String res = getJobState(tbName)
        if (res == "FINISHED") {
            break
        } else {
            Thread.sleep(2000)
            if (max_try_secs < 1) {
                println "test timeout," + "state:" + res
                assertEquals("FINISHED",res)
            }
        }
    }

    qt_select_char_to_int """select * from ${tbName} limit 1"""
}
