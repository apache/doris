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

suite("test_alter_db") {
    String dbName = "test_alter_db_qwer"
    String newDbName = "new_alter_db_qwer"
    String tableName = "t1_qwer"

    sql """DROP DATABASE IF EXISTS ${dbName} FORCE;"""
    sql """DROP DATABASE IF EXISTS ${newDbName} FORCE;"""

    sql """CREATE DATABASE ${dbName};"""

    sql """ALTER DATABASE ${dbName} SET DATA QUOTA 102400;"""
    sql """ALTER DATABASE ${dbName} SET REPLICA QUOTA 1024;"""
    sql """ALTER DATABASE ${dbName} SET TRANSACTION QUOTA 1024;"""

    def dbResults = sql """SHOW PROC '/dbs';"""
    def found = false
    def result = null
    for (def dbResult : dbResults) {
        logger.debug("dbResult:${dbResult}")
        if (dbResult[1].contains(dbName)) {
            found = true
            result = dbResult
            break;
        }
    }
    assertTrue(found)
    assertEquals("100.000 KB", result[4])
    assertEquals("1024", result[7])
    assertEquals("1024", result[9])

    sql """use ${dbName};"""
    sql """
        CREATE TABLE `${tableName}`
        (
            `siteid` INT DEFAULT '10',
            `citycode` SMALLINT,
            `username` VARCHAR(32) DEFAULT 'test',
            `pv` BIGINT SUM DEFAULT '0'
        )
        AGGREGATE KEY(`siteid`, `citycode`, `username`)
        DISTRIBUTED BY HASH(siteid) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    sql """INSERT INTO ${dbName}.${tableName}(siteid, citycode, username, pv) VALUES (1, 1, "xxx", 1);"""
    sql """SYNC;"""
    order_qt_sql """SELECT * FROM ${dbName}.${tableName};"""

    sql """ ALTER DATABASE ${dbName} RENAME ${newDbName};"""
    order_qt_sql """SELECT * FROM ${newDbName}.${tableName};"""

    dbResults = sql """SHOW PROC '/dbs';"""
    found = false
    for (def dbResult : dbResults) {
        logger.debug("dbResult:${dbResult}")
        if (dbResult[1].contains(dbName)) {
            found = true
            break;
        }
    }
    assertFalse(found)

    sql """DROP DATABASE IF EXISTS ${dbName} FORCE;"""
    sql """DROP DATABASE IF EXISTS ${newDbName} FORCE;"""
}
