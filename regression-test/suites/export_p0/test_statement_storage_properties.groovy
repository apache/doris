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

suite("test_statement_storage_properties") {
    sql "DROP TABLE IF EXISTS statement_storage_fixture"
    sql """CREATE TABLE statement_storage_fixture (id INT) DUPLICATE KEY(id)
           DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num"="1")"""
    def db = (sql "SELECT DATABASE()")[0][0]
    def user = "statement_storage_test_user"
    def password = "C123_567p"
    try_sql("DROP USER ${user}")
    sql """CREATE USER '${user}' IDENTIFIED BY '${password}'"""
    try {
        sql """GRANT SELECT_PRIV ON ${db}.* TO ${user}"""
        if (isCloudMode()) {
            def cluster = (sql "SHOW CLUSTERS")[0][0]
            sql """GRANT USAGE_PRIV ON CLUSTER `${cluster}` TO ${user}"""
        }
        connect(user, password, context.config.jdbcUrl) {
            sql """USE ${db}"""
            sql """EXPLAIN SELECT 1 INTO OUTFILE 'hdfs://nn/data'
                   PROPERTIES ("hadoop.username"="doris", "dfs.replication"="2")"""
            test {
                sql """EXPLAIN SELECT 1 INTO OUTFILE 'hdfs://nn/data'
                       PROPERTIES ("fs.defaultFS"="relative")"""
                exception "Default filesystem must have a remote scheme and authority"
            }
            test {
                sql """EXPLAIN SELECT * FROM hdfs("uri"="hdfs://nn/data", "format"="csv",
                                                  "fs.defaultFS"="relative")"""
                exception "Default filesystem must have a remote scheme and authority"
            }
            test {
                sql """EXPORT TABLE statement_storage_fixture TO 'hdfs://nn/data'
                       WITH HDFS ("fs.defaultFS"="relative")"""
                exception "Default filesystem must have a remote scheme and authority"
            }
        }
    } finally {
        sql """DROP USER ${user}"""
    }
}
