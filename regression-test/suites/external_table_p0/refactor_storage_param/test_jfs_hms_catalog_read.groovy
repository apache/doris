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
    if (jfsMeta == null || jfsMeta.trim().isEmpty()) {
        throw new IllegalStateException("jfsMeta must be configured for JFS data IO regression")
    }
    String jfsCluster = jfsFs.replaceFirst("^jfs://", "")
    int slashPos = jfsCluster.indexOf("/")
    if (slashPos > 0) {
        jfsCluster = jfsCluster.substring(0, slashPos)
    }
    String jfsMetaProperty = ",\n                'juicefs.${jfsCluster}.meta' = '${jfsMeta}'"

    String hdfsUser = context.config.otherConfigs.get("jfsHadoopUser")
    if (hdfsUser == null || hdfsUser.trim().isEmpty()) {
        hdfsUser = context.config.otherConfigs.get("hdfsUser")
    }
    if (hdfsUser == null || hdfsUser.trim().isEmpty()) {
        hdfsUser = "root"
    }

    String hmsUris = context.config.otherConfigs.get("jfsHiveMetastoreUris")
    if (hmsUris == null || hmsUris.trim().isEmpty()) {
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String hmsPort = context.config.otherConfigs.get("hive3HmsPort")
        if (hmsPort == null || hmsPort.trim().isEmpty()) {
            hmsPort = context.config.otherConfigs.get("hive2HmsPort")
        }
        if (externalEnvIp == null || externalEnvIp.trim().isEmpty()
                || hmsPort == null || hmsPort.trim().isEmpty()) {
            logger.info("skip JFS test because jfsHiveMetastoreUris is empty and fallback externalEnvIp/hmsPort is invalid.")
            return
        }
        hmsUris = "thrift://${externalEnvIp}:${hmsPort}"
    }
    String catalogName = "test_jfs_hms_catalog_read"
    String dbName = "test_jfs_hms_catalog_read_db"
    String tableName = "test_jfs_hms_catalog_read_tbl"
    String jfsDbBasePath = context.config.otherConfigs.get("jfsDbBasePath")
    if (jfsDbBasePath == null || jfsDbBasePath.trim().isEmpty()) {
        jfsDbBasePath = "${jfsFs}/doris_jfs/${hdfsUser}"
    }
    jfsDbBasePath = jfsDbBasePath.replaceAll('/+$', '')
    String jfsStagingDir = context.config.otherConfigs.get("jfsStagingDir")
    if (jfsStagingDir == null || jfsStagingDir.trim().isEmpty()) {
        jfsStagingDir = "${jfsDbBasePath}/.doris_staging"
    }
    jfsStagingDir = jfsStagingDir.replaceAll('/+$', '')
    String dbLocation = "${jfsDbBasePath}/${dbName}"

    sql """drop catalog if exists ${catalogName}"""

    try {
        sql """
            CREATE CATALOG ${catalogName} PROPERTIES (
                'type' = 'hms',
                'hive.metastore.uris' = '${hmsUris}',
                'fs.defaultFS' = '${jfsFs}',
                'fs.jfs.impl' = '${jfsImpl}',
                'hadoop.username' = '${hdfsUser}',
                'hive.staging_dir' = '${jfsStagingDir}'
                ${jfsMetaProperty}
            );
        """

        sql """switch ${catalogName}"""
        def dbs = sql """show databases"""
        assertTrue(dbs.size() > 0)

        def hasDb = sql """show databases like '${dbName}'"""
        if (hasDb.size() > 0) {
            sql """drop table if exists `${dbName}`.`${tableName}`"""
            sql """drop database if exists `${dbName}`"""
        }
        sql """
            create database `${dbName}`
            properties('location'='${dbLocation}')
        """
        sql """use `${dbName}`"""
        sql """
            CREATE TABLE `${tableName}` (
              `id` INT,
              `name` STRING
            ) ENGINE=hive
            PROPERTIES (
              'file_format'='parquet'
            )
        """
        sql """insert into `${tableName}` values (1, 'jfs_1'), (2, 'jfs_2')"""

        def cnt = sql """select count(*) from `${tableName}`"""
        assertEquals("2", cnt[0][0].toString())

        def rows = sql """select * from `${tableName}` order by id"""
        assertTrue(rows.size() == 2)
        assertEquals("1", rows[0][0].toString())
        assertEquals("jfs_1", rows[0][1].toString())
        assertEquals("2", rows[1][0].toString())
        assertEquals("jfs_2", rows[1][1].toString())
    } finally {
        sql """switch internal"""
    }
}
