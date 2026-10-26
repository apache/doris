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

suite("test_hive_external_lookup_refresh", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Hive test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String hmsPort = context.config.otherConfigs.get("hive3HmsPort")
    String hdfsPort = context.config.otherConfigs.get("hive3HdfsPort")
    for (String catalog : ["hive_lookup_source", "hive_lookup_target"]) {
        sql "DROP CATALOG IF EXISTS ${catalog}"
        sql """CREATE CATALOG ${catalog} PROPERTIES (
            'type' = 'hms',
            'hadoop.username' = 'hadoop',
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
            'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfsPort}'
        )"""
    }

    // Two independent catalogs model a change made through another engine's cache domain.
    sql "DROP DATABASE IF EXISTS hive_lookup_source.fe_lookup_refresh_db FORCE"
    sql "CREATE DATABASE hive_lookup_source.fe_lookup_refresh_db"
    sql "CREATE TABLE hive_lookup_source.fe_lookup_refresh_db.owner_target (before_col INT)"
    sql "CREATE TABLE hive_lookup_source.fe_lookup_refresh_db.owner_sibling (sibling_col INT)"
    order_qt_names_before "SHOW TABLES FROM hive_lookup_target.fe_lookup_refresh_db"
    qt_schema_before "DESC hive_lookup_target.fe_lookup_refresh_db.owner_target"
    qt_sibling_before "DESC hive_lookup_target.fe_lookup_refresh_db.owner_sibling"
    // Validate the stable projection used by case-mapping suites without requiring BE data execution.
    sql """EXPLAIN SELECT * REPLACE(CAST(NULL AS DATETIME) AS UPDATE_TIME)
        FROM hive_lookup_target.information_schema.tables WHERE TABLE_SCHEMA = 'fe_lookup_refresh_db'"""

    sql "DROP TABLE hive_lookup_source.fe_lookup_refresh_db.owner_target"
    sql "REFRESH TABLE hive_lookup_target.fe_lookup_refresh_db.owner_target"
    // Listing first proves refresh itself invalidated membership, without a failing SELECT repairing it.
    order_qt_names_after_drop "SHOW TABLES FROM hive_lookup_target.fe_lookup_refresh_db"
    test {
        sql "SELECT * FROM hive_lookup_target.fe_lookup_refresh_db.owner_target"
        exception "does not exist"
    }
    test {
        sql "EXPLAIN SELECT * FROM hive_lookup_target.fe_lookup_refresh_db.owner_target"
        exception "does not exist"
    }
    qt_sibling_after "DESC hive_lookup_target.fe_lookup_refresh_db.owner_sibling"

    sql "CREATE TABLE hive_lookup_source.fe_lookup_refresh_db.owner_target (after_col STRING)"
    sql "REFRESH DATABASE hive_lookup_target.fe_lookup_refresh_db"
    order_qt_names_after_database_refresh "SHOW TABLES FROM hive_lookup_target.fe_lookup_refresh_db"
    qt_schema_after_database_refresh "DESC hive_lookup_target.fe_lookup_refresh_db.owner_target"

    sql "DROP TABLE hive_lookup_source.fe_lookup_refresh_db.owner_target"
    sql "CREATE TABLE hive_lookup_source.fe_lookup_refresh_db.owner_target (recreated_col BIGINT)"
    sql "REFRESH TABLE hive_lookup_target.fe_lookup_refresh_db.owner_target"
    qt_schema_after_recreation "DESC hive_lookup_target.fe_lookup_refresh_db.owner_target"

    sql "DROP CATALOG IF EXISTS hive_lookup_observed"
    sql """CREATE CATALOG hive_lookup_observed PROPERTIES (
        'type' = 'hms',
        'hadoop.username' = 'hadoop',
        'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
        'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfsPort}',
        'schema.cache.ttl-second' = '0'
    )"""
    sql "CREATE TABLE hive_lookup_source.fe_lookup_refresh_db.owner_observed (observed_col INT)"
    // Schema/HMS table reads are live, while the FE table-name and object caches stay enabled.
    order_qt_observed_names_before """SHOW TABLES FROM hive_lookup_observed.fe_lookup_refresh_db
        LIKE 'owner_observed'"""
    qt_observed_schema_before "DESC hive_lookup_observed.fe_lookup_refresh_db.owner_observed"
    sql "DROP TABLE hive_lookup_source.fe_lookup_refresh_db.owner_observed"
    test {
        sql "SELECT * FROM hive_lookup_observed.fe_lookup_refresh_db.owner_observed"
        // The legacy HMS schema read confirms absence and retires the matching lookup state.
        exception "failed to"
    }
    order_qt_observed_names_after """SHOW TABLES FROM hive_lookup_observed.fe_lookup_refresh_db
        LIKE 'owner_observed'"""
    test {
        sql "SELECT * FROM hive_lookup_observed.fe_lookup_refresh_db.owner_observed"
        exception "does not exist"
    }

}
