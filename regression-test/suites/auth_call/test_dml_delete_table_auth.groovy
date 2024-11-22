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
import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_dml_delete_table_auth","p0,auth_call") {

    String user = 'test_dml_delete_table_auth_user'
    String pwd = 'C123_567p'
    String dbName = 'test_dml_delete_table_auth_db'
    String tableName = 'test_dml_delete_table_auth_tb'

    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO ${user}""";
    }

    try_sql("DROP USER ${user}")
    try_sql """drop database if exists ${dbName}"""
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    sql """grant select_priv on regression_test to ${user}"""
    sql """create database ${dbName}"""

    sql """create table ${dbName}.${tableName} (
                id BIGINT,
                username VARCHAR(20)
            )
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1"
            );"""
    sql """
        insert into ${dbName}.`${tableName}` values 
        (1, "111"),
        (2, "222"),
        (3, "333");
        """

    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """DELETE FROM ${dbName}.${tableName} WHERE id = 3;"""
            exception "denied"
        }
        checkNereidsExecute("show DELETE from ${dbName}")
        def del_res = sql """show DELETE from ${dbName}"""
        assertTrue(del_res.size() == 0)
    }
    sql """grant load_priv on ${dbName}.${tableName} to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """DELETE FROM ${dbName}.${tableName} WHERE id = 3;"""
            exception "denied"
        }
        def del_res = sql """show DELETE from ${dbName}"""
        assertTrue(del_res.size() == 0)
    }
    sql """grant select_priv on ${dbName}.${tableName} to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """DELETE FROM ${dbName}.${tableName} WHERE id = 3;"""
        def del_res = sql """show DELETE from ${dbName}"""
        logger.info("del_res: " + del_res)
        assertTrue(del_res.size() == 1)
    }

    def res = sql """select count(*) from ${dbName}.${tableName};"""
    assertTrue(res[0][0] == 2)

    String tableName1 = 'test_dml_delete_table_auth_tb1'
    String tableName2 = 'test_dml_delete_table_auth_tb2'
    String tableName3 = 'test_dml_delete_table_auth_tb3'
    sql """CREATE TABLE ${dbName}.${tableName1}
        (id INT, c1 BIGINT, c2 STRING, c3 DOUBLE, c4 DATE)
        UNIQUE KEY (id)
        DISTRIBUTED BY HASH (id)
        PROPERTIES('replication_num'='1', "function_column.sequence_col" = "c4");"""
    sql """CREATE TABLE ${dbName}.${tableName2}
        (id INT, c1 BIGINT, c2 STRING, c3 DOUBLE, c4 DATE)
        DISTRIBUTED BY HASH (id)
        PROPERTIES('replication_num'='1');"""
    sql """CREATE TABLE ${dbName}.${tableName3}
        (id INT)
        DISTRIBUTED BY HASH (id)
        PROPERTIES('replication_num'='1');"""
    sql """INSERT INTO ${dbName}.${tableName1} VALUES
        (1, 1, '1', 1.0, '2000-01-01'),
        (2, 2, '2', 2.0, '2000-01-02'),
        (3, 3, '3', 3.0, '2000-01-03');"""
    sql """INSERT INTO ${dbName}.${tableName2} VALUES
        (1, 10, '10', 10.0, '2000-01-10'),
        (2, 20, '20', 20.0, '2000-01-20'),
        (3, 30, '30', 30.0, '2000-01-30'),
        (4, 4, '4', 4.0, '2000-01-04'),
        (5, 5, '5', 5.0, '2000-01-05');"""
    sql """INSERT INTO ${dbName}.${tableName3} VALUES
        (1),
        (4),
        (5);"""

    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """DELETE FROM ${dbName}.${tableName1} 
                USING ${dbName}.${tableName2} INNER JOIN ${dbName}.${tableName3} 
                ON ${dbName}.${tableName2}.id = ${dbName}.${tableName3}.id
                WHERE ${dbName}.${tableName1}.id = ${dbName}.${tableName2}.id;"""
            exception "denied"
        }
    }
    sql """grant load_priv on ${dbName}.${tableName1} to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """DELETE FROM ${dbName}.${tableName1} 
                USING ${dbName}.${tableName2} INNER JOIN ${dbName}.${tableName3} 
                ON ${dbName}.${tableName2}.id = ${dbName}.${tableName3}.id
                WHERE ${dbName}.${tableName1}.id = ${dbName}.${tableName2}.id;"""
            exception "denied"
        }
    }
    sql """grant select_priv on ${dbName}.${tableName1} to ${user}"""
    sql """grant select_priv on ${dbName}.${tableName2} to ${user}"""
    sql """grant select_priv on ${dbName}.${tableName3} to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """DELETE FROM ${dbName}.${tableName1} 
            USING ${dbName}.${tableName2} INNER JOIN ${dbName}.${tableName3} 
            ON ${dbName}.${tableName2}.id = ${dbName}.${tableName3}.id
            WHERE ${dbName}.${tableName1}.id = ${dbName}.${tableName2}.id;"""
    }
    res = sql """select count(*) from ${dbName}.${tableName1};"""
    assertTrue(res[0][0] == 2)


    sql """drop database if exists ${dbName}"""
    try_sql("DROP USER ${user}")
}
