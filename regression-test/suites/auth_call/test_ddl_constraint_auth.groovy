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

suite("test_ddl_constraint_auth", "p0,auth_call") {
    String user = 'test_ddl_constraint_auth_user'
    String pwd = 'C123_567p'
    String dbName = 'test_ddl_constraint_auth_db'
    String tableName = 'test_ddl_constraint_auth_tb'
    String constraintName = 'test_ddl_constraint_auth_uk'

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

    // SELECT alone must not be enough to change constraints
    sql """grant SELECT_PRIV on ${dbName}.${tableName} to ${user}"""
    connect(user, "${pwd}", context.config.jdbcUrl) {
        sql """use ${dbName}"""
        test {
            sql """ALTER TABLE ${tableName} ADD CONSTRAINT ${constraintName} UNIQUE (id)"""
            exception "denied"
        }
    }

    sql """use ${dbName}"""
    sql """ALTER TABLE ${tableName} ADD CONSTRAINT ${constraintName} UNIQUE (id)"""

    // dropping a constraint is refused as well. Note this goes through the normal resolution path:
    // the name-based fallback in DropConstraintCommand only triggers when resolution throws (e.g. an
    // external table removed out of band), which is not reproducible from a suite, so the check on
    // that branch is covered by inspection only.
    connect(user, "${pwd}", context.config.jdbcUrl) {
        sql """use ${dbName}"""
        test {
            sql """ALTER TABLE ${tableName} DROP CONSTRAINT ${constraintName}"""
            exception "denied"
        }
    }
    def constraints = sql """SHOW CONSTRAINTS FROM ${dbName}.${tableName}"""
    assertTrue(constraints.size() == 1)

    sql """grant ALTER_PRIV on ${dbName}.${tableName} to ${user}"""
    connect(user, "${pwd}", context.config.jdbcUrl) {
        sql """use ${dbName}"""
        sql """ALTER TABLE ${tableName} DROP CONSTRAINT ${constraintName}"""
        sql """ALTER TABLE ${tableName} ADD CONSTRAINT ${constraintName} UNIQUE (id)"""
    }
    constraints = sql """SHOW CONSTRAINTS FROM ${dbName}.${tableName}"""
    assertTrue(constraints.size() == 1)

    // dropping a primary key cascades into the foreign keys of every referencing table, so ALTER on
    // the referencing tables is required as well
    String pkTable = 'test_ddl_constraint_auth_pk_tb'
    String fkTable = 'test_ddl_constraint_auth_fk_tb'
    String pkName = 'test_ddl_constraint_auth_pk'
    String fkName = 'test_ddl_constraint_auth_fk'
    sql """
        CREATE TABLE IF NOT EXISTS ${dbName}.${pkTable} (
            id BIGINT NOT NULL,
            username VARCHAR(30)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES ("replication_num" = "1");
        """
    sql """
        CREATE TABLE IF NOT EXISTS ${dbName}.${fkTable} (
            id BIGINT NOT NULL,
            pk_id BIGINT NOT NULL
        )
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES ("replication_num" = "1");
        """
    sql """ALTER TABLE ${dbName}.${pkTable} ADD CONSTRAINT ${pkName} PRIMARY KEY (id)"""
    sql """ALTER TABLE ${dbName}.${fkTable} ADD CONSTRAINT ${fkName} FOREIGN KEY (pk_id) REFERENCES ${pkTable}(id)"""

    sql """grant ALTER_PRIV on ${dbName}.${pkTable} to ${user}"""
    connect(user, "${pwd}", context.config.jdbcUrl) {
        sql """use ${dbName}"""
        test {
            sql """ALTER TABLE ${pkTable} DROP CONSTRAINT ${pkName}"""
            exception "denied"
        }
    }
    def fkConstraints = sql """SHOW CONSTRAINTS FROM ${dbName}.${fkTable}"""
    assertTrue(fkConstraints.size() == 1)

    sql """grant ALTER_PRIV on ${dbName}.${fkTable} to ${user}"""
    connect(user, "${pwd}", context.config.jdbcUrl) {
        sql """use ${dbName}"""
        sql """ALTER TABLE ${pkTable} DROP CONSTRAINT ${pkName}"""
    }
    fkConstraints = sql """SHOW CONSTRAINTS FROM ${dbName}.${fkTable}"""
    assertTrue(fkConstraints.isEmpty())

    sql """drop database if exists ${dbName}"""
    try_sql("DROP USER ${user}")
}
