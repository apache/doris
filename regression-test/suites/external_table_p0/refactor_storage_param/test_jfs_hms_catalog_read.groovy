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

    try {
        sql """drop catalog if exists ${catalogName}"""

        sql """
            CREATE CATALOG ${catalogName} PROPERTIES (
                'type' = 'hms',
                'hive.metastore.uris' = '${hmsUris}',
                'fs.defaultFS' = '${jfsFs}',
                'fs.jfs.impl' = '${jfsImpl}',
                'hadoop.username' = '${hdfsUser}'
            );
        """

        sql """switch ${catalogName}"""
        def dbs = sql """show databases"""
        assertTrue(dbs.size() > 0)

        // Smoke read against docker hive sample table.
        def rows = sql """select * from ${catalogName}.`default`.parquet_partition_table
                          order by l_orderkey, l_partkey limit 1"""
        assertTrue(rows.size() == 1)
    } finally {
        sql """switch internal"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
