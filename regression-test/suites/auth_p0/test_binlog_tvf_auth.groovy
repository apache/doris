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

suite("test_binlog_tvf_auth", "p0,auth") {
    String user = "test_binlog_tvf_auth_user"
    String password = "C123_567p"

    sql "DROP USER IF EXISTS '${user}'"
    sql "DROP DATABASE IF EXISTS test_binlog_tvf_auth_db"
    sql "CREATE DATABASE test_binlog_tvf_auth_db"
    sql """
        CREATE TABLE test_binlog_tvf_auth_db.test_binlog_tvf_auth_table (
            k BIGINT,
            v INT
        ) ENGINE=OLAP
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        )
    """
    sql "INSERT INTO test_binlog_tvf_auth_db.test_binlog_tvf_auth_table VALUES (1, 100)"
    sql "SYNC"
    sql "CREATE USER '${user}' IDENTIFIED BY '${password}'"

    if (isCloudMode()) {
        def clusters = sql "SHOW CLUSTERS"
        assertFalse(clusters.isEmpty())
        sql "GRANT USAGE_PRIV ON CLUSTER `${clusters[0][0]}` TO ${user}"
    }
    // The configured JDBC URL connects to regression_test before executing the query.
    // Grant access to that default database while keeping the target table unauthorized.
    sql "GRANT SELECT_PRIV ON regression_test TO ${user}"

    connect(user, password, context.config.jdbcUrl) {
        test {
            sql "SELECT * FROM test_binlog_tvf_auth_db.test_binlog_tvf_auth_table"
            exception "denied"
        }
        test {
            sql """
                SELECT k, v
                FROM binlog(
                    "db" = "test_binlog_tvf_auth_db",
                    "table" = "test_binlog_tvf_auth_table"
                )
                LIMIT 1
            """
            exception "denied"
        }
    }

    sql "GRANT SELECT_PRIV ON test_binlog_tvf_auth_db.test_binlog_tvf_auth_table TO ${user}"
    connect(user, password, context.config.jdbcUrl) {
        sql """
            SELECT k, v
            FROM binlog(
                "db" = "test_binlog_tvf_auth_db",
                "table" = "test_binlog_tvf_auth_table"
            )
            LIMIT 1
        """
    }
}
