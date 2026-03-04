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

suite("test_jfs_hms_catalog_read", "p0,external") {
    String enableJfs = context.config.otherConfigs.get("enableJfsTest")
    if (enableJfs == null || !enableJfs.equalsIgnoreCase("true")) {
        logger.info("disable JFS test.")
        return
    }

    String enableHive = context.config.otherConfigs.get("enableHiveTest")
    if (enableHive == null || !enableHive.equalsIgnoreCase("true")) {
        logger.info("disable Hive test.")
        return
    }

    String jfsFs = context.config.otherConfigs.get("jfsFs")
    if (jfsFs == null || jfsFs.trim().isEmpty()) {
        logger.info("skip JFS test because jfsFs is empty.")
        return
    }

    String jfsImpl = context.config.otherConfigs.get("jfsImpl")
    if (jfsImpl == null || jfsImpl.trim().isEmpty()) {
        jfsImpl = "io.juicefs.JuiceFileSystem"
    }
    String jfsMeta = context.config.otherConfigs.get("jfsMeta")
    String jfsCluster = jfsFs.replaceFirst("^jfs://", "")
    int slashPos = jfsCluster.indexOf("/")
    if (slashPos > 0) {
        jfsCluster = jfsCluster.substring(0, slashPos)
    }
    String jfsMetaProperty = ""
    if (jfsMeta != null && !jfsMeta.trim().isEmpty()) {
        jfsMetaProperty = ",\n                'juicefs.${jfsCluster}.meta' = '${jfsMeta}'"
    }

    String hdfsUser = context.config.otherConfigs.get("hdfsUser")
    if (hdfsUser == null || hdfsUser.trim().isEmpty()) {
        hdfsUser = "hive"
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String hmsPort = context.config.otherConfigs.get("hive2HmsPort")
    String hmsUris = context.config.otherConfigs.get("jfsHiveMetastoreUris")
    if (hmsUris == null || hmsUris.trim().isEmpty()) {
        hmsUris = "thrift://${externalEnvIp}:${hmsPort}"
    }
    String catalogName = "test_jfs_hms_catalog_read"
    String dbName = "test_jfs_hms_catalog_read_db"
    String tableName = "test_jfs_hms_catalog_read_tbl"
    String dbLocation = "${jfsFs}/tmp/${dbName}"

    try {
        sql """drop catalog if exists ${catalogName}"""

        sql """
            CREATE CATALOG ${catalogName} PROPERTIES (
                'type' = 'hms',
                'hive.metastore.uris' = '${hmsUris}',
                'fs.defaultFS' = '${jfsFs}',
                'fs.jfs.impl' = '${jfsImpl}',
                'hadoop.username' = '${hdfsUser}'
                ${jfsMetaProperty}
            );
        """

        sql """switch ${catalogName}"""
        def dbs = sql """show databases"""
        assertTrue(dbs.size() > 0)

        sql """
            create database if not exists `${dbName}`
            properties('location'='${dbLocation}')
        """
        sql """use `${dbName}`"""
        sql """drop table if exists `${tableName}`"""
        sql """
            CREATE TABLE `${tableName}` (
              `id` INT,
              `name` STRING
            ) ENGINE=hive
            PROPERTIES (
              'file_format'='parquet'
            )
        """
        if (jfsMeta == null || jfsMeta.trim().isEmpty()) {
            logger.info("skip JFS data io because jfsMeta is empty. only validate catalog/db/table DDL.")
        } else {
            sql """insert into `${tableName}` values (1, 'jfs_1'), (2, 'jfs_2')"""

            def cnt = sql """select count(*) from `${tableName}`"""
            assertEquals("2", cnt[0][0].toString())

            def rows = sql """select * from `${tableName}` order by id"""
            assertTrue(rows.size() == 2)
            assertEquals("1", rows[0][0].toString())
            assertEquals("jfs_1", rows[0][1].toString())
            assertEquals("2", rows[1][0].toString())
            assertEquals("jfs_2", rows[1][1].toString())
        }
    } finally {
        try {
            sql """switch ${catalogName}"""
            sql """drop table if exists `${dbName}`.`${tableName}`"""
            sql """drop database if exists `${dbName}`"""
        } catch (Exception e) {
            logger.info("cleanup jfs hive objects failed: ${e.getMessage()}")
        }
        sql """switch internal"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
