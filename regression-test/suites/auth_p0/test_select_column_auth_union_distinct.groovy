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

suite("test_select_column_auth_union_distinct", "p0,auth") {
    def user = "test_column_union_distinct_user"
    def pwd = "C123_567p"
    def dbName = "test_column_union_distinct_db"
    def tableName = "t"

    try_sql("drop user ${user}")
    sql """drop database if exists ${dbName}"""
    sql """create user '${user}' identified by '${pwd}'"""

    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO ${user}""";
    }

    sql """create database ${dbName}"""
    sql """
        create table ${dbName}.${tableName} (
            id int,
            name varchar(32),
            secret bigint
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties('replication_num' = '1')
        """
    sql """insert into ${dbName}.${tableName} values (1, 'alice', 111111111)"""
    sql """grant select_priv(id) on ${dbName}.${tableName} to ${user}"""

    connect(user, "${pwd}", context.config.jdbcUrl) {
        // Reading the unauthorized name/secret columns through a DISTINCT union must be denied.
        test {
            sql """
                select name, id from ${dbName}.${tableName}
                union
                select name, id from ${dbName}.${tableName}
                """
            exception "Permission denied"
        }
        test {
            sql """
                select secret, id from ${dbName}.${tableName}
                union
                select secret, id from ${dbName}.${tableName}
                """
            exception "Permission denied"
        }
        test {
            sql """
                select id, name, secret from ${dbName}.${tableName}
                union
                select id, name, secret from ${dbName}.${tableName}
                """
            exception "Permission denied"
        }
        test {
            sql """
                select * from (
                    select name, id from ${dbName}.${tableName}
                    union
                    select name, id from ${dbName}.${tableName}
                ) q
                """
            exception "Permission denied"
        }
        test {
            sql """
                select name, id from ${dbName}.${tableName}
                union all
                select name, id from ${dbName}.${tableName}
                """
            exception "Permission denied"
        }

        // Queries over the granted column must keep working.
        sql """select id from ${dbName}.${tableName} union select id from ${dbName}.${tableName}"""
        sql """
            select count(*) from (
                select id from ${dbName}.${tableName}
                union
                select id from ${dbName}.${tableName}
            ) q
            """
    }
}
