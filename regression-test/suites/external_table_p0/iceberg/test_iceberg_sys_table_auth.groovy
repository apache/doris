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

suite("test_iceberg_sys_table_auth", "p0,external,doris,external_docker,external_docker_doris,system_table") {

    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String catalog_name = "test_iceberg_systable_auth_ctl"
    String db_name = "test_db"
    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
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

    sql """switch ${catalog_name}"""
    sql """use ${db_name}"""

    // Create test table
    sql """drop table if exists test_iceberg_systable_auth_tbl1;"""
    sql """create table test_iceberg_systable_auth_tbl1 (id int);"""
    sql """insert into test_iceberg_systable_auth_tbl1 values(1);"""
    sql """insert into test_iceberg_systable_auth_tbl1 values(2);"""
    sql """insert into test_iceberg_systable_auth_tbl1 values(3);"""
    sql """insert into test_iceberg_systable_auth_tbl1 values(4);"""
    sql """insert into test_iceberg_systable_auth_tbl1 values(5);"""

    // Create test user without table permission
    String user = "test_iceberg_systable_auth_user"
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

    // Test that user without table permission cannot query system tables
    connect(user, "${pwd}", context.config.jdbcUrl) {
        // Test snapshots system table via iceberg_meta function
        test {
              sql """
                 select committed_at, snapshot_id, parent_id, operation from iceberg_meta(
                                             "table" = "${catalog_name}.${db_name}.test_iceberg_systable_auth_tbl1",
                                             "query_type" = "snapshots");
              """
              exception "denied"
        }
        // Test snapshots system table via direct access
        test {
              sql """
                 select committed_at, snapshot_id, parent_id, operation from ${catalog_name}.${db_name}.test_iceberg_systable_auth_tbl1\$snapshots
              """
              exception "denied"
        }
        // Test files system table via iceberg_meta function
        test {
              sql """
                 select * from iceberg_meta(
                                             "table" = "${catalog_name}.${db_name}.test_iceberg_systable_auth_tbl1",
                                             "query_type" = "files");
              """
              exception "denied"
        }
        // Test files system table via direct access
        test {
              sql """
                 select * from ${catalog_name}.${db_name}.test_iceberg_systable_auth_tbl1\$files
              """
              exception "denied"
        }
        // Test entries system table via iceberg_meta function
        test {
              sql """
                 select * from iceberg_meta(
                                             "table" = "${catalog_name}.${db_name}.test_iceberg_systable_auth_tbl1",
                                             "query_type" = "entries");
              """
              exception "denied"
        }
        // Test entries system table via direct access
        test {
              sql """
                 select * from ${catalog_name}.${db_name}.test_iceberg_systable_auth_tbl1\$entries
              """
              exception "denied"
        }
        // Test history system table via iceberg_meta function
        test {
              sql """
                 select * from iceberg_meta(
                                             "table" = "${catalog_name}.${db_name}.test_iceberg_systable_auth_tbl1",
                                             "query_type" = "history");
              """
              exception "denied"
        }
        // Test history system table via direct access
        test {
              sql """
                 select * from ${catalog_name}.${db_name}.test_iceberg_systable_auth_tbl1\$history
              """
              exception "denied"
        }
    }

    // Grant permission and verify user can query system tables
    sql """GRANT SELECT_PRIV ON ${catalog_name}.${db_name}.test_iceberg_systable_auth_tbl1 TO '${user}'@'%'"""
    connect(user, "${pwd}", context.config.jdbcUrl) {
        // Test snapshots system table with permission
        sql """
           select committed_at, snapshot_id, parent_id, operation from iceberg_meta(
                                       "table" = "${catalog_name}.${db_name}.test_iceberg_systable_auth_tbl1",
                                       "query_type" = "snapshots");
        """
        sql """select committed_at, snapshot_id, parent_id, operation from ${catalog_name}.${db_name}.test_iceberg_systable_auth_tbl1\$snapshots"""
        
        // Test files system table with permission
        sql """
           select * from iceberg_meta(
                                       "table" = "${catalog_name}.${db_name}.test_iceberg_systable_auth_tbl1",
                                       "query_type" = "files");
        """
        sql """select * from ${catalog_name}.${db_name}.test_iceberg_systable_auth_tbl1\$files"""
        
        // Test entries system table with permission
        sql """
           select * from iceberg_meta(
                                       "table" = "${catalog_name}.${db_name}.test_iceberg_systable_auth_tbl1",
                                       "query_type" = "entries");
        """
        sql """select * from ${catalog_name}.${db_name}.test_iceberg_systable_auth_tbl1\$entries"""
        
        // Test history system table with permission
        sql """
           select * from iceberg_meta(
                                       "table" = "${catalog_name}.${db_name}.test_iceberg_systable_auth_tbl1",
                                       "query_type" = "history");
        """
        sql """select * from ${catalog_name}.${db_name}.test_iceberg_systable_auth_tbl1\$history"""
    }
    try_sql("DROP USER '${user}'@'%'")
}

