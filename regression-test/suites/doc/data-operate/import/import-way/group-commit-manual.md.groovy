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


import org.junit.jupiter.api.Assertions

import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.Statement

suite("docs/data-operate/import/import-way/group-commit-manual.md", "p0,nonConcurrent") {
    try {
        sql "CREATE DATABASE IF NOT EXISTS `db`; use `db`;"
        sql "DROP TABLE IF EXISTS `dt`;"
        sql """
            CREATE TABLE `dt` (
                `id` int(11) NOT NULL,
                `name` varchar(50) NULL,
                `score` int(11) NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            );
        """

        Demo.HOST = getMasterIp()
        Demo.PORT = getMasterPort("mysql")
        Demo.USER = context.config.jdbcUser
        Demo.PASSWD = context.config.jdbcPassword

        Demo.groupCommitInsert()
        Demo.groupCommitInsertBatch()


        sql "truncate table dt;"
        multi_sql """
            set group_commit = async_mode;
            insert into dt values(1, 'Bob', 90), (2, 'Alice', 99);
            insert into dt(id, name) values(3, 'John');
            select * from dt;
            select * from dt;
        """

        multi_sql """
            set group_commit = sync_mode;
            insert into dt values(4, 'Bob', 90), (5, 'Alice', 99);
            select * from dt;
        """

        sql "set group_commit = off_mode;"
        cmd """curl --location-trusted -u ${context.config.jdbcUser}:${context.config.jdbcPassword} -T ${context.file.parent}/data.csv -H "group_commit:async_mode"  -H "column_separator:,"  http://${context.config.feHttpAddress}/api/db/dt/_stream_load"""
        cmd """curl --location-trusted -u ${context.config.jdbcUser}:${context.config.jdbcPassword} -T ${context.file.parent}/data.csv -H "group_commit:sync_mode"  -H "column_separator:,"  http://${context.config.feHttpAddress}/api/db/dt/_stream_load"""
        cmd """curl --location-trusted -u ${context.config.jdbcUser}:${context.config.jdbcPassword} -T ${context.file.parent}/data.csv  -H "group_commit:async_mode" -H "sql:insert into db.dt select * from http_stream('column_separator'=',', 'format' = 'CSV')"  http://${context.config.feHttpAddress}/api/_http_stream"""
        cmd """curl --location-trusted -u ${context.config.jdbcUser}:${context.config.jdbcPassword} -T ${context.file.parent}/data.csv  -H "group_commit:sync_mode" -H "sql:insert into db.dt select * from http_stream('column_separator'=',', 'format' = 'CSV')"  http://${context.config.feHttpAddress}/api/_http_stream"""

        sql """ALTER TABLE dt SET ("group_commit_interval_ms" = "2000");"""
        sql """ALTER TABLE dt SET ("group_commit_data_bytes" = "134217728");"""
    } catch (Throwable t) {
        Assertions.fail("examples in docs/data-operate/import/import-way/group-commit-manual.md failed to exec, please fix it", t)
    }
}

class Demo {
    static String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static String URL_PATTERN = "jdbc:mysql://%s:%d/%s?useServerPrepStmts=true";
    static String HOST = "127.0.0.1";
    static int PORT = 9030;
    static String DB = "db";
    static String TBL = "dt";
    static String USER = "root";
    static String PASSWD = "";
    static int INSERT_BATCH_SIZE = 10;

    static final void groupCommitInsert() throws Exception {
        Class.forName(JDBC_DRIVER);
        Connection conn = DriverManager.getConnection(String.format(URL_PATTERN, HOST, PORT, DB), USER, PASSWD)
        try {
            // set session variable 'group_commit'
            Statement statement = conn.createStatement()
            try {
                statement.execute("SET group_commit = async_mode;");
            } finally {
                statement.close()
            }

            String query = "insert into " + TBL + " values(?, ?, ?)";
            PreparedStatement stmt = conn.prepareStatement(query)
            try {
                for (int i = 0; i < INSERT_BATCH_SIZE; i++) {
                    stmt.setInt(1, i);
                    stmt.setString(2, "name" + i);
                    stmt.setInt(3, i + 10);
                    int result = stmt.executeUpdate();
                    System.out.println("rows: " + result);
                }
            } finally {
                stmt.close()
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e
        } finally {
            conn.close()
        }
    }

    static final void groupCommitInsertBatch() throws Exception {
        Class.forName(JDBC_DRIVER);
        // add rewriteBatchedStatements=true and cachePrepStmts=true in JDBC url
        // set session variables by sessionVariables=group_commit=async_mode in JDBC url
        Connection conn = DriverManager.getConnection(
                String.format(URL_PATTERN + "&rewriteBatchedStatements=true&cachePrepStmts=true&sessionVariables=group_commit=async_mode", HOST, PORT, DB), USER, PASSWD)
        try {
            String query = "insert into " + TBL + " values(?, ?, ?)";
            PreparedStatement stmt = conn.prepareStatement(query)
            try {
                for (int j = 0; j < 5; j++) {
                    // 10 rows per insert
                    for (int i = 0; i < INSERT_BATCH_SIZE; i++) {
                        stmt.setInt(1, i);
                        stmt.setString(2, "name" + i);
                        stmt.setInt(3, i + 10);
                        stmt.addBatch();
                    }
                    int[] result = stmt.executeBatch();
                }
            } finally {
                stmt.close()
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e
        } finally {
            conn.close()
        }
    }
}
