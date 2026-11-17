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

suite("test_iceberg_table_handle_reuse", "p0,external,iceberg") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableIcebergTest"))) {
        return
    }
    String catalog = context.config.otherConfigs.get("icebergSplitCatalog")
    if (!catalog) {
        catalog = "iceberg_table_handle_reuse"
        String host = context.config.otherConfigs.get("externalEnvIp")
        String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
        String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
        sql "drop catalog if exists ${catalog}"
        sql """create catalog ${catalog} properties (
            'type'='iceberg', 'iceberg.catalog.type'='rest', 'uri'='http://${host}:${restPort}',
            's3.access_key'='admin', 's3.secret_key'='password',
            's3.endpoint'='http://${host}:${minioPort}', 's3.region'='us-east-1',
            'meta.cache.iceberg.manifest.enable'='true')"""
    }
    String sqlCache = sql("show variables like 'enable_sql_cache'")[0][1]
    try {
        sql "set enable_sql_cache=false"
        sql "create database if not exists ${catalog}.iceberg_table_handle_reuse"
        sql "use ${catalog}.iceberg_table_handle_reuse"
        sql "drop table if exists handle_versions"
        test {
            sql "select * from handle_versions"
            exception "does not exist"
        }
        sql "create table handle_versions (id int) engine=iceberg"
        sql "create table if not exists handle_versions (id int) engine=iceberg"
        sql "insert into handle_versions values (1), (2)"
        sql "alter table handle_versions create branch audit"
        sql "alter table handle_versions create tag before_change"
        order_qt_cold "select id from handle_versions"
        order_qt_warm "select id from handle_versions"
        sql "alter table handle_versions modify column id bigint"
        sql "insert into handle_versions values (3000000000)"
        // Independent handles within one statement must retain their own snapshot/ref.
        order_qt_multiple_versions """select 'main', count(*), sum(id) from handle_versions
            union all select 'branch', count(*), sum(id) from handle_versions@branch(audit)
            union all select 'tag', count(*), sum(id) from handle_versions@tag(before_change)"""
        sql "alter table handle_versions create or replace branch audit"
        order_qt_moved_branch "select id from handle_versions@branch(audit)"
        order_qt_original_tag "select id from handle_versions@tag(before_change)"
        for (String level : ["table", "database", "catalog"]) {
            if (level == "table") {
                sql "refresh table handle_versions"
            } else if (level == "database") {
                sql "refresh database ${catalog}.iceberg_table_handle_reuse"
            } else {
                sql "refresh catalog ${catalog}"
            }
            quickTest("${level}_cold", "select count(*), sum(id) from handle_versions", true)
            quickTest("${level}_warm", "select count(*), sum(id) from handle_versions", true)
        }
        // DROP removes positive proof and a previous miss must not mask recreation.
        sql "drop table handle_versions"
        test {
            sql "select * from handle_versions"
            exception "does not exist"
        }
        sql "create table handle_versions (id bigint) engine=iceberg"
        sql "insert into handle_versions values (42)"
        order_qt_recreated "select id from handle_versions"
    } finally {
        sql "set enable_sql_cache=${sqlCache}"
    }
}
