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

suite("test_nereids_authentication", "query") {
    def create_table = { tableName ->
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE ${tableName} (
                `key` INT,
                value INT
            ) DUPLICATE KEY (`key`) DISTRIBUTED BY HASH (`key`) BUCKETS 1
            PROPERTIES ('replication_num' = '1')
        """
    }

    sql "set enable_nereids_planner = true"

    def dbName = "nereids_authentication"
    sql "DROP DATABASE IF EXISTS ${dbName}"
    sql "CREATE DATABASE ${dbName}"
    sql "USE ${dbName}"

    def tableName1 = "accessible_table";
    def tableName2 = "inaccessible_table";
    create_table.call(tableName1);
    create_table.call(tableName2);

    def user='nereids_user'
    try_sql "DROP USER ${user}"
    sql "CREATE USER ${user} IDENTIFIED BY '123456'"
    sql "GRANT SELECT_PRIV ON internal.${dbName}.${tableName1} TO ${user}"

    def tokens = context.config.jdbcUrl.split('/')
    def url=tokens[0] + "//" + tokens[2] + "/" + dbName + "?"
    def result = connect(user=user, password='123456', url=url) {
        sql "SELECT * FROM ${tableName1}"
    }
    assertEquals(result.size(), 0)

    connect(user=user, password='123456', url=url) {
        try {
            sql "SELECT * FROM ${tableName2}"
            fail()
        } catch (Exception e) {
            log.info(e.getMessage())
            assertTrue(e.getMessage().contains('SELECT command denied to user'))
        }
    }

    connect(user=user, password='123456', url=url) {
        try {
            sql "SELECT * FROM ${tableName1}, ${tableName2} WHERE ${tableName1}.`key` = ${tableName2}.`key`"
            fail()
        } catch (Exception e) {
            log.info(e.getMessage())
            assertTrue(e.getMessage().contains('SELECT command denied to user'))
        }
    }

    sql "GRANT SELECT_PRIV ON internal.${dbName}.${tableName2} TO ${user}"
    connect(user=user, password='123456', url=url) {
        sql "SELECT * FROM ${tableName2}"
    }
    assertEquals(result.size(), 0)
    connect(user=user, password='123456', url=url) {
        sql "SELECT * FROM ${tableName1}, ${tableName2} WHERE ${tableName1}.`key` = ${tableName2}.`key`"
    }
    assertEquals(result.size(), 0)
}
