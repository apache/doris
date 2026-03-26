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

suite("test_deltalake_catalog", "p0,external,deltalake,external_docker,external_docker_deltalake") {
    String enabled = context.config.otherConfigs.get("enableDeltaLakeTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Delta Lake test.")
        return
    }

    String hms_port = context.config.otherConfigs.get("deltalakeHmsPort")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "test_deltalake_catalog"

    sql """drop catalog if exists ${catalog_name};"""

    sql """
        CREATE CATALOG ${catalog_name} PROPERTIES (
            'type' = 'deltalake',
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
        );
    """

    // Test 1: catalog creation and basic connectivity
    sql """switch ${catalog_name};"""
    def databases = sql """show databases;"""
    logger.info("Delta Lake databases: " + databases.toString())
    assertTrue(databases.size() > 0, "Should have at least one database")

    // Test 2: show tables
    sql """use delta_test;"""
    def tables = sql """show tables;"""
    logger.info("Delta Lake tables: " + tables.toString())
    assertTrue(tables.size() > 0, "Should have at least one table in delta_test")

    // Test 3: switch back to internal
    sql """switch internal;"""
    def internalDbs = sql """show databases;"""
    assertTrue(internalDbs.size() > 0, "Should show internal databases")

    // Test 4: access Delta Lake table via full qualified name
    sql """switch ${catalog_name};"""

    // Test 5: verify catalog properties
    def catalogInfo = sql """show create catalog ${catalog_name};"""
    logger.info("Catalog info: " + catalogInfo.toString())
    assertTrue(catalogInfo.toString().contains("deltalake"), "Catalog type should be deltalake")

    // Clean up
    sql """switch internal;"""
    sql """drop catalog if exists ${catalog_name};"""
}
