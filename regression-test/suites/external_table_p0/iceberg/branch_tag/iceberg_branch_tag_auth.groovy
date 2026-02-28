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

suite("iceberg_branch_tag_auth", "p0,external_docker,external,branch_tag") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "iceberg_branch_tag_auth"
    String db_name = "test_parallel_auth"
    String table_name = "test_branch_tag_auth"

    sql """drop catalog if exists ${catalog_name}"""
    sql """
    CREATE CATALOG ${catalog_name} PROPERTIES (
        'type'='iceberg',
        'iceberg.catalog.type'='rest',
        'uri' = 'http://${externalEnvIp}:${rest_port}',
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
        "s3.region" = "us-east-1"
    );"""

    sql """drop database if exists ${catalog_name}.${db_name} force"""
    sql """create database ${catalog_name}.${db_name}"""
    sql """ use ${catalog_name}.${db_name} """
    sql """drop table if exists ${table_name}"""
    sql """create table ${table_name} (id int, name string)"""
    sql """insert into ${table_name} values (1, 'name_1')"""
    sql """alter table ${table_name} create branch branch_0 """
    sql """alter table ${table_name} create tag tag_0 """
    qt_select_branch_0 """select * from ${table_name}@branch(branch_0)"""
    qt_select_tag_0 """select * from ${table_name}@tag(tag_0)"""

    // Create test user without table permission
    String user = "test_iceberg_branch_tag_auth_user"
    String pwd = 'C123_567p'
    try_sql("DROP USER '${user}'@'%'")
    sql """CREATE USER '${user}'@'%' IDENTIFIED BY '${pwd}'"""
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO '${user}'@'%'""";
    }
    sql """create database if not exists internal.regression_test"""
    sql """GRANT SELECT_PRIV ON internal.regression_test.* TO '${user}'@'%'"""
    connect(user, "${pwd}", context.config.jdbcUrl) {
        test {
            sql """
                alter table  ${catalog_name}.${db_name}.${table_name} create branch branch_1;
              """
            exception "denied"
        }
        test {
            sql """
                select * from ${catalog_name}.${db_name}.${table_name}@branch(branch_0);
              """
            exception "denied"
        }
        test {
            sql """
                alter table  ${catalog_name}.${db_name}.${table_name} drop branch branch_0;
                """
            exception "denied"
        }
        test {
            sql """
                alter table  ${catalog_name}.${db_name}.${table_name} create tag tag_1;
                """
            exception "denied"
        }
        test {
            sql """
                select * from ${catalog_name}.${db_name}.${table_name}@tag(tag_0);
                """
            exception "denied"
        }
        test {
            sql """
                alter table  ${catalog_name}.${db_name}.${table_name} drop tag tag_0;
                """
            exception "denied"
        }
    }
    // Grant permission and verify user can query branch and tag
    sql """GRANT SELECT_PRIV ON ${catalog_name}.${db_name}.${table_name} TO '${user}'@'%'"""

    connect(user, "${pwd}", context.config.jdbcUrl) {
        qt_select_branch_0_auth """select * from ${catalog_name}.${db_name}.${table_name}@branch(branch_0)"""
        qt_select_tag_0_auth """select * from ${catalog_name}.${db_name}.${table_name}@tag(tag_0)"""

        test {
            sql """
                alter table  ${catalog_name}.${db_name}.${table_name} create branch branch_1;
              """
            exception "denied"
        }
        test {
            sql """
                alter table  ${catalog_name}.${db_name}.${table_name} drop branch branch_0;
                """
            exception "denied"
        }
        test {
            sql """
                alter table  ${catalog_name}.${db_name}.${table_name} create tag tag_1;
                """
            exception "denied"
        }
        test {
            sql """
                alter table  ${catalog_name}.${db_name}.${table_name} drop tag tag_0;
                """
            exception "denied"
        }
    }

    // Grant permission and verify user can create/drop branch and tag
    sql """GRANT ALTER_PRIV ON ${catalog_name}.${db_name}.${table_name} TO '${user}'@'%'"""
    connect(user, "${pwd}", context.config.jdbcUrl) {
        sql """
            alter table  ${catalog_name}.${db_name}.${table_name} create branch branch_1;
          """
        qt_select_branch_0_auth_2 """
            select * from ${catalog_name}.${db_name}.${table_name}@branch(branch_0);
          """
        sql """
            alter table  ${catalog_name}.${db_name}.${table_name} drop branch branch_0;
            """
        sql """
            alter table  ${catalog_name}.${db_name}.${table_name} create tag tag_1;
            """
        qt_select_tag_0_auth_2 """
            select * from ${catalog_name}.${db_name}.${table_name}@tag(tag_0);
            """
        sql """
            alter table  ${catalog_name}.${db_name}.${table_name} drop tag tag_0;
            """
    }
}

