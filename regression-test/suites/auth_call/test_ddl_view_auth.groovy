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

suite("test_ddl_view_auth","p0,auth_call") {
    String user = 'test_ddl_view_auth_user'
    String pwd = 'C123_567p'
    String dbName = 'test_ddl_view_auth_db'
    String tableName = 'test_ddl_view_auth_tb'
    String viewName = 'test_ddl_view_auth_view'

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
        INSERT INTO ${dbName}.${tableName} (id, username)
        VALUES (1, "111"),
               (2, "222"),
               (3, "333")
        """

    // ddl create
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """CREATE VIEW ${dbName}.${viewName} (k1, v1)
                AS
                SELECT id as k1, SUM(id) FROM ${dbName}.${tableName}
                WHERE id = 1 GROUP BY k1;"""
            exception "denied"
        }
        test {
            sql """SHOW VIEW from ${tableName} from ${dbName}"""
            exception 'denied'
        }
    }
    sql """grant select_priv(id) on ${dbName}.${tableName} to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """CREATE VIEW ${dbName}.${viewName} (k1, v1)
                AS
                SELECT id as k1, SUM(id) FROM ${dbName}.${tableName}
                WHERE id = 1 GROUP BY k1;"""
            exception 'denied'
        }
        def res = sql """SHOW VIEW from ${tableName} from ${dbName}"""
        assertTrue(res.size() == 0)
    }
    sql """CREATE VIEW ${dbName}.${viewName} (k1, v1)
            AS
            SELECT id as k1, SUM(id) FROM ${dbName}.${tableName}
            WHERE id = 1 GROUP BY k1;"""
    sql """grant Create_priv on ${dbName}.${viewName} to ${user}"""
    sql """drop view ${dbName}.${viewName}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """CREATE VIEW ${dbName}.${viewName} (k1, v1)
            AS
            SELECT id as k1, SUM(id) FROM ${dbName}.${tableName}
            WHERE id = 1 GROUP BY k1;"""

        def res = sql """SHOW VIEW from ${tableName} from ${dbName}"""
        assertTrue(res.size() == 1)
    }
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """CREATE VIEW ${dbName}.${viewName} (k1, v1)
                AS
                SELECT username as k1, SUM(id) FROM ${dbName}.${tableName}
                WHERE id = 1 GROUP BY k1;"""
            exception 'denied'
        }
    }

    // ddl alter
    // user alter
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """alter VIEW ${dbName}.${viewName} (k1, v1)
                AS
                SELECT id as k1, SUM(id) FROM ${dbName}.${tableName}
                WHERE id = 1 GROUP BY k1;"""
            exception 'denied'
        }
    }
    sql """grant ALTER_PRIV on ${dbName}.${viewName} to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """alter VIEW ${dbName}.${viewName} (k1, v1)
                AS
                SELECT id as k1, SUM(id) FROM ${dbName}.${tableName}
                WHERE id = 1 GROUP BY k1;"""
    }

    // dml show
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """select * from ${dbName}.${viewName};"""
            exception "denied"
        }
    }
    sql """grant select_PRIV on ${dbName}.${viewName} to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """select * from ${dbName}.${viewName};"""
    }

    // ddl drop
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """drop table ${dbName}.${viewName};"""
            exception 'denied'
        }
    }
    sql """grant DROP_PRIV on ${dbName}.${viewName} to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """use ${dbName}"""
        sql """drop view ${viewName};"""
        def ctl_res = sql """show tables;"""
        assertTrue(ctl_res.size() == 1)
    }

    sql """drop database if exists ${dbName}"""
    try_sql("DROP USER ${user}")
}
