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

suite("test_external_catalog_create_table_cache_visibility", "p0,external") {

    // ========== Iceberg (HMS catalog type) ==========
    def test_iceberg = { String hivePrefix ->
        String enabled = context.config.otherConfigs.get("enableIcebergTest")
        if (enabled == null || !enabled.equalsIgnoreCase("true")) {
            logger.info("skip iceberg test.")
            return
        }

        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String hmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
        String default_fs = "hdfs://${externalEnvIp}:${hdfs_port}"
        String warehouse = "${default_fs}/warehouse"
        String catalog_name = "test_iceberg_create_cache_vis_${hivePrefix}"

        sql """drop catalog if exists ${catalog_name}"""
        sql """
        create catalog ${catalog_name} properties (
            'type'='iceberg',
            'iceberg.catalog.type'='hms',
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
            'fs.defaultFS' = '${default_fs}',
            'warehouse' = '${warehouse}'
        );
        """

        sql """switch ${catalog_name}"""
        String db_name = "test_iceberg_cache_vis_db"
        sql """drop database if exists ${db_name} force"""
        sql """create database ${db_name}"""
        sql """use ${db_name}"""

        // CREATE TABLE then immediately verify visibility
        String tbl1 = "iceberg_cache_vis_tbl1"
        sql """drop table if exists ${db_name}.${tbl1}"""
        sql """CREATE TABLE ${db_name}.${tbl1} (id int, name string) engine=iceberg"""

        // The new table should be immediately visible without REFRESH CATALOG
        def show_result = sql """SHOW TABLES"""
        assertTrue(show_result.toString().contains(tbl1), "Table ${tbl1} should appear in SHOW TABLES after CREATE TABLE")

        def create_result = sql """SHOW CREATE TABLE ${db_name}.${tbl1}"""
        assertTrue(create_result.size() > 0, "SHOW CREATE TABLE should succeed for newly created table")
        assertTrue(create_result[0][1].toString().toLowerCase().contains("iceberg"), "SHOW CREATE TABLE result should contain iceberg engine")

        // INSERT + SELECT to verify end-to-end
        sql """INSERT INTO ${db_name}.${tbl1} VALUES (1, 'a'), (2, 'b')"""
        order_qt_iceberg_select """SELECT * FROM ${db_name}.${tbl1} ORDER BY id"""

        // Create a second table to verify multiple tables
        String tbl2 = "iceberg_cache_vis_tbl2"
        sql """drop table if exists ${db_name}.${tbl2}"""
        sql """CREATE TABLE ${db_name}.${tbl2} (id int, value double) engine=iceberg"""
        def create_result2 = sql """SHOW CREATE TABLE ${db_name}.${tbl2}"""
        assertTrue(create_result2.size() > 0, "Second table should also be immediately visible")

        sql """drop table if exists ${db_name}.${tbl1}"""
        sql """drop table if exists ${db_name}.${tbl2}"""
        sql """drop database if exists ${db_name} force"""
        sql """drop catalog if exists ${catalog_name}"""
    }

    // ========== Hive/HMS ==========
    def test_hive = { String hivePrefix ->
        String enabled = context.config.otherConfigs.get("enableHiveTest")
        if (enabled == null || !enabled.equalsIgnoreCase("true")) {
            logger.info("skip hive test.")
            return
        }

        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String hmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
        String catalog_name = "test_hive_create_cache_vis_${hivePrefix}"

        sql """drop catalog if exists ${catalog_name}"""
        sql """
        create catalog if not exists ${catalog_name} properties (
            'type'='hms',
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
            'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}'
        );
        """

        sql """switch ${catalog_name}"""
        String db_name = "test_hive_cache_vis_db"
        sql """drop database if exists ${db_name} force"""
        sql """create database ${db_name}"""
        sql """use ${db_name}"""

        // CREATE TABLE then immediately verify visibility
        String tbl1 = "hive_cache_vis_tbl1"
        sql """drop table if exists ${db_name}.${tbl1}"""
        sql """
        CREATE TABLE ${db_name}.${tbl1} (
            id INT,
            name STRING
        ) ENGINE=hive
        PROPERTIES (
            'file_format'='orc'
        )
        """

        // The new table should be immediately visible without REFRESH CATALOG
        def show_result = sql """SHOW TABLES"""
        assertTrue(show_result.toString().contains(tbl1), "Table ${tbl1} should appear in SHOW TABLES after CREATE TABLE")

        def create_result = sql """SHOW CREATE TABLE ${db_name}.${tbl1}"""
        assertTrue(create_result.size() > 0, "SHOW CREATE TABLE should succeed for newly created table")
        assertTrue(create_result[0][1].toString().toLowerCase().contains("hive"), "SHOW CREATE TABLE result should contain hive engine")

        // INSERT + SELECT to verify end-to-end
        sql """INSERT INTO ${db_name}.${tbl1} VALUES (1, 'a'), (2, 'b')"""
        order_qt_hive_select """SELECT * FROM ${db_name}.${tbl1} ORDER BY id"""

        // Create a second table
        String tbl2 = "hive_cache_vis_tbl2"
        sql """drop table if exists ${db_name}.${tbl2}"""
        sql """
        CREATE TABLE ${db_name}.${tbl2} (
            id INT,
            value DOUBLE
        ) ENGINE=hive
        PROPERTIES (
            'file_format'='parquet'
        )
        """
        def create_result2 = sql """SHOW CREATE TABLE ${db_name}.${tbl2}"""
        assertTrue(create_result2.size() > 0, "Second table should also be immediately visible")

        sql """drop table if exists ${db_name}.${tbl1}"""
        sql """drop table if exists ${db_name}.${tbl2}"""
        sql """drop database if exists ${db_name} force"""
        sql """drop catalog if exists ${catalog_name}"""
    }

    // ========== Paimon ==========
    def test_paimon = { String hivePrefix ->
        String enabled = context.config.otherConfigs.get("enablePaimonTest")
        if (enabled == null || !enabled.equalsIgnoreCase("true")) {
            logger.info("skip paimon test.")
            return
        }

        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String hmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
        String default_fs = "hdfs://${externalEnvIp}:${hdfs_port}"
        String warehouse = "${default_fs}/warehouse"
        String catalog_name = "test_paimon_create_cache_vis_${hivePrefix}"

        sql """drop catalog if exists ${catalog_name}"""
        sql """
        create catalog ${catalog_name} properties (
            'type'='paimon',
            'paimon.catalog.type'='hms',
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
            'warehouse' = '${warehouse}'
        );
        """

        sql """switch ${catalog_name}"""
        String db_name = "test_paimon_cache_vis_db"
        sql """drop database if exists ${db_name} force"""
        sql """create database ${db_name}"""
        sql """use ${db_name}"""

        // CREATE TABLE then immediately verify visibility
        String tbl1 = "paimon_cache_vis_tbl1"
        sql """drop table if exists ${db_name}.${tbl1}"""
        sql """
        CREATE TABLE ${db_name}.${tbl1} (
            id INT,
            name STRING
        ) engine=paimon
        properties("primary-key"="id")
        """

        // The new table should be immediately visible without REFRESH CATALOG
        def show_result = sql """SHOW TABLES"""
        assertTrue(show_result.toString().contains(tbl1), "Table ${tbl1} should appear in SHOW TABLES after CREATE TABLE")

        def create_result = sql """SHOW CREATE TABLE ${db_name}.${tbl1}"""
        assertTrue(create_result.size() > 0, "SHOW CREATE TABLE should succeed for newly created table")
        assertTrue(create_result[0][1].toString().toLowerCase().contains("paimon"), "SHOW CREATE TABLE result should contain paimon engine")

        // INSERT + SELECT to verify end-to-end
        sql """INSERT INTO ${db_name}.${tbl1} VALUES (1, 'a'), (2, 'b')"""
        order_qt_paimon_select """SELECT * FROM ${db_name}.${tbl1} ORDER BY id"""

        // Create a second table
        String tbl2 = "paimon_cache_vis_tbl2"
        sql """drop table if exists ${db_name}.${tbl2}"""
        sql """
        CREATE TABLE ${db_name}.${tbl2} (
            id INT,
            value DOUBLE
        ) engine=paimon
        properties("primary-key"="id")
        """
        def create_result2 = sql """SHOW CREATE TABLE ${db_name}.${tbl2}"""
        assertTrue(create_result2.size() > 0, "Second table should also be immediately visible")

        sql """drop table if exists ${db_name}.${tbl1}"""
        sql """drop table if exists ${db_name}.${tbl2}"""
        sql """drop database if exists ${db_name} force"""
        sql """drop catalog if exists ${catalog_name}"""
    }

    // ========== Iceberg REST catalog type ==========
    def test_iceberg_rest = {
        String enabled = context.config.otherConfigs.get("enableIcebergTest")
        if (enabled == null || !enabled.equalsIgnoreCase("true")) {
            logger.info("skip iceberg rest test.")
            return
        }

        String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
        String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String catalog_name = "test_iceberg_rest_cache_vis"

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
        String db_name = "test_iceberg_rest_cache_vis_db"
        sql """drop database if exists ${db_name} force"""
        sql """create database ${db_name}"""
        sql """use ${db_name}"""

        String tbl1 = "iceberg_rest_cache_vis_tbl1"
        sql """drop table if exists ${db_name}.${tbl1}"""
        sql """CREATE TABLE ${db_name}.${tbl1} (id int, name string)"""

        // The new table should be immediately visible
        def show_result = sql """SHOW TABLES"""
        assertTrue(show_result.toString().contains(tbl1), "Table ${tbl1} should appear in SHOW TABLES after CREATE TABLE")

        def create_result = sql """SHOW CREATE TABLE ${db_name}.${tbl1}"""
        assertTrue(create_result.size() > 0, "SHOW CREATE TABLE should succeed for newly created table")

        sql """INSERT INTO ${db_name}.${tbl1} VALUES (1, 'a'), (2, 'b')"""
        order_qt_iceberg_rest_select """SELECT * FROM ${db_name}.${tbl1} ORDER BY id"""

        sql """drop table if exists ${db_name}.${tbl1}"""
        sql """drop database if exists ${db_name} force"""
        sql """drop catalog if exists ${catalog_name}"""
    }

    // Run tests for each hive prefix
    for (String hivePrefix : ["hive2", "hive3"]) {
        test_iceberg(hivePrefix)
        test_hive(hivePrefix)
        test_paimon(hivePrefix)
    }
    test_iceberg_rest()
}
