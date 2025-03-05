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

suite("test_ddl_catalog_auth","p0,auth_call") {
    String user = 'test_ddl_catalog_auth_user'
    String pwd = 'C123_567p'
    String catalogName = 'test_ddl_catalog_auth_catalog'
    String catalogNameNew = 'test_ddl_catalog_auth_catalog_new'
    String catalogNameOther = 'test_ddl_catalog_auth_catalog_other'

    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO ${user}""";
    }

    sql """create catalog if not exists ${catalogNameOther} properties (
            'type'='hms'
        );"""

    try_sql("DROP USER ${user}")
    try_sql """drop catalog if exists ${catalogName}"""
    try_sql """drop catalog if exists ${catalogNameNew}"""
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    sql """grant select_priv on regression_test to ${user}"""

    // ddl create
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """create catalog if not exists ${catalogName} properties (
                    'type'='hms'
                );"""
            exception "denied"
        }
        def ctl_res = sql """show catalogs;"""
        assertTrue(ctl_res.size() == 1)
    }
    sql """create catalog if not exists ${catalogName} properties (
            'type'='hms'
        );"""
    sql """grant Create_priv on ${catalogName}.*.* to ${user}"""
    sql """drop catalog ${catalogName}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """create catalog if not exists ${catalogName} properties (
            'type'='hms'
        );"""
        sql """show create catalog ${catalogName}"""
        def ctl_res = sql """show catalogs;"""
        assertTrue(ctl_res.size() == 2)
    }

    // ddl alter
    // user alter
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """ALTER CATALOG ${catalogName} RENAME ${catalogNameNew};"""
            exception "denied"
        }
    }
    sql """grant ALTER_PRIV on ${catalogName}.*.* to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """ALTER CATALOG ${catalogName} RENAME ${catalogNameNew};"""
        test {
            sql """show create catalog ${catalogNameNew}"""
            exception "denied"
        }
        def ctl_res = sql """show catalogs;"""
        assertTrue(ctl_res.size() == 1)
    }
    // root alter
    sql """ALTER CATALOG ${catalogNameNew} RENAME ${catalogName};"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """show create catalog ${catalogName}"""
        def ctl_res = sql """show catalogs;"""
        assertTrue(ctl_res.size() == 2)
    }

    // ddl drop
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """drop CATALOG ${catalogName};"""
            exception "denied"
        }
    }
    sql """grant DROP_PRIV on ${catalogName}.*.* to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """drop CATALOG ${catalogName};"""
        def ctl_res = sql """show catalogs;"""
        assertTrue(ctl_res.size() == 1)
    }

    sql """drop catalog if exists ${catalogName}"""
    sql """drop catalog if exists ${catalogNameOther}"""
    try_sql("DROP USER ${user}")
}
