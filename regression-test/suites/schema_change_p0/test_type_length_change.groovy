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

suite("test_type_length_change", "p0") {
    def tableName = "test_type_length_change";
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE ${tableName}
        (
            k INT,
            c0 CHAR(5),
            c1 VARCHAR(5)
        ) DUPLICATE KEY (k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ( "replication_allocation" = "tag.location.default: 1");
    """

    def getTableStatusSql = " SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1  "

    sql """ INSERT INTO ${tableName} VALUES(1, "abc", "abc") """

    test {
        sql """ ALTER TABLE ${tableName} MODIFY COLUMN c0 CHAR(3) """
        exception "Shorten type length is prohibited"
    }

    sql """ ALTER TABLE ${tableName} MODIFY COLUMN c0 CHAR(6) """
    waitForSchemaChangeDone {
        sql getTableStatusSql
        time 600
    }

    sql """  INSERT INTO ${tableName} VALUES(2, "abcde", "abc") """
    qt_master_sql """ SELECT * FROM ${tableName} ORDER BY k"""

    test {
        sql """ ALTER TABLE ${tableName} MODIFY COLUMN c1 VARCHAR(3) """
        exception "Shorten type length is prohibited"
    }

    sql """ ALTER TABLE ${tableName} MODIFY COLUMN c1 VARCHAR(6) """
    waitForSchemaChangeDone {
        sql getTableStatusSql
        time 600
    }

    sql """  INSERT INTO ${tableName} VALUES(3, "abcde", "abcde") """
    qt_master_sql """ SELECT * FROM ${tableName} ORDER BY k"""

    test {
        sql """ ALTER TABLE ${tableName} MODIFY COLUMN c0 VARCHAR(3) """
        exception "Shorten type length is prohibited"
    }

    sql """ ALTER TABLE ${tableName} MODIFY COLUMN c0 VARCHAR(6) """
    waitForSchemaChangeDone {
        sql getTableStatusSql
        time 600
    }

    sql """  INSERT INTO ${tableName} VALUES(4, "abcde", "abcde") """
    qt_master_sql """ SELECT * FROM ${tableName} ORDER BY k"""
    qt_master_sql """ DESC ${tableName} """

    test {
        sql """ ALTER TABLE ${tableName} MODIFY COLUMN c1 CHAR(10) """
        exception "Can not change VARCHAR to CHAR"
    }
}
