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

import org.junit.Assert;

suite("test_show_transaction_auth", "p0,auth") {
    String user = 'test_show_transaction_auth_user'
    String pwd = 'C123_567p'
    String dbName = 'test_show_transaction_db'
    String tableName = 'test_show_transaction_table'
    String label = 'test_show_transaction_label'
    try_sql("DROP USER ${user}")
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    sql """drop database if exists ${dbName}"""
    sql """create database ${dbName}"""
    sql """
            CREATE TABLE IF NOT EXISTS ${dbName}.`${tableName}` (
            user_id            BIGINT       NOT NULL COMMENT "user id",
            name               VARCHAR(20)           COMMENT "name",
            age                INT                   COMMENT "age"
            )
            DUPLICATE KEY(user_id)
            DISTRIBUTED BY HASH(user_id) BUCKETS 10;
            """
    sql """grant select_priv,load_priv on ${dbName}.* to ${user}"""
    connect(user, "${pwd}", context.config.jdbcUrl) {
        sql """ 
                INSERT INTO ${dbName}.${tableName} with label ${label} (user_id, name, age)
                VALUES (1, "Emily", 25),
                       (2, "Benjamin", 35),
                       (3, "Olivia", 28),
                       (4, "Alexander", 60),
                       (5, "Ava", 17);
            """
        sql """SHOW TRANSACTION FROM ${dbName} WHERE label='${dbName}';"""

    }
    sql """drop database if exists ${dbName}"""
    try_sql("DROP USER ${user}")
}
