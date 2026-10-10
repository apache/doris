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

suite("test_ddl_dictionary_auth", "p0,auth_call") {
    String user = 'test_ddl_dictionary_auth_user'
    String pwd = 'C123_567p'
    String dbName = 'test_ddl_dictionary_auth_db'
    String tableName = 'test_ddl_dictionary_auth_tb'
    String dictName = 'test_ddl_dictionary_auth_dict'

    try_sql("DROP USER ${user}")
    try_sql """drop database if exists ${dbName}"""
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    sql """grant select_priv on regression_test to ${user}"""
    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO ${user}""";
    }

    sql """create database ${dbName}"""
    sql """
        CREATE TABLE IF NOT EXISTS ${dbName}.${tableName} (
            id BIGINT,
            username VARCHAR(30)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES ("replication_num" = "1");
        """
    sql """insert into ${dbName}.${tableName} values(1, 'doris')"""

    def createDictSql = """
        CREATE DICTIONARY ${dbName}.${dictName} USING ${dbName}.${tableName}
        (
            id KEY,
            username VALUE
        )
        LAYOUT(HASH_MAP)
        PROPERTIES('data_lifetime'='600');
        """

    // no privilege at all on the database
    connect(user, "${pwd}", context.config.jdbcUrl) {
        test {
            sql createDictSql
            exception "denied"
        }
        test {
            sql """DROP DICTIONARY ${dbName}.${dictName};"""
            exception "denied"
        }
    }

    // CREATE on the database is not enough: the dictionary load task runs as ADMIN, so the
    // creator must also be able to read the source table itself.
    sql """grant CREATE_PRIV on ${dbName}.* to ${user}"""
    connect(user, "${pwd}", context.config.jdbcUrl) {
        test {
            sql createDictSql
            exception "denied"
        }
    }

    sql """grant SELECT_PRIV on ${dbName}.${tableName} to ${user}"""
    connect(user, "${pwd}", context.config.jdbcUrl) {
        sql createDictSql
    }
    sql """use ${dbName}"""
    def dictRes = sql """SHOW DICTIONARIES"""
    assertTrue(dictRes.size() == 1)

    // dropping needs DROP on the database
    connect(user, "${pwd}", context.config.jdbcUrl) {
        test {
            sql """DROP DICTIONARY ${dbName}.${dictName};"""
            exception "denied"
        }
    }

    sql """grant DROP_PRIV on ${dbName}.* to ${user}"""
    connect(user, "${pwd}", context.config.jdbcUrl) {
        sql """DROP DICTIONARY ${dbName}.${dictName};"""
    }
    sql """use ${dbName}"""
    dictRes = sql """SHOW DICTIONARIES"""
    assertTrue(dictRes.size() == 0)

    sql """drop database if exists ${dbName}"""
    try_sql("DROP USER ${user}")
}
