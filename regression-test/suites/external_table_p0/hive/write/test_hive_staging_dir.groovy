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

import org.apache.doris.regression.util.Hdfs
import org.apache.hadoop.fs.Path

import java.net.URI

suite("test_hive_staging_dir", "p0,external,hive,external_docker,external_docker_hive") {
    def getHiveTableLocation = { String db, String tbl ->
        def rows = hive_docker """describe formatted `${db}`.`${tbl}`"""
        for (def row : rows) {
            if (row == null || row.size() < 2) {
                continue
            }
            String key = row[0] == null ? "" : row[0].toString().trim()
            if (key.equalsIgnoreCase("Location") || key.equalsIgnoreCase("Location:")) {
                def value = row[1]
                if (value != null && !value.toString().trim().isEmpty()) {
                    return value.toString().trim()
                }
            }
        }
        throw new RuntimeException("Failed to find location for ${db}.${tbl}")
    }

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Hive test.")
        return
    }

    for (String hivePrefix : ["hive3"]) {
        setHivePrefix(hivePrefix)
        String catalogName = "test_${hivePrefix}_staging_dir"
        String dbName = "write_test"
        String tableRel = "staging_dir_rel_${hivePrefix}"
        String tableAbs = "staging_dir_abs_${hivePrefix}"
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String hmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String hdfsPort = context.config.otherConfigs.get(hivePrefix + "HdfsPort")

        String currentUser = (sql "select current_user()")[0][0].toString()
        if (currentUser.contains("@")) {
            currentUser = currentUser.substring(0, currentUser.indexOf("@"))
        }
        currentUser = currentUser.replaceAll("[^A-Za-z0-9_-]", "")
        String suffix = System.currentTimeMillis().toString()

        String hdfsUser = context.config.otherConfigs.get("hdfsUser")
        if (hdfsUser == null || hdfsUser.isEmpty()) {
            hdfsUser = "root"
        }

        String stagingRelBase = null
        String stagingAbsBase = null
        String stagingDefaultBase = null
        String stagingRelWithUser = null
        String stagingAbsWithUser = null
        String stagingDefaultWithUser = null
        String stagingRelPath = null
        def fs = null
        try {
            hive_docker """create database if not exists `${dbName}`"""
            hive_docker """drop table if exists `${dbName}`.`${tableRel}`"""
            hive_docker """drop table if exists `${dbName}`.`${tableAbs}`"""
            hive_docker """create table `${dbName}`.`${tableRel}` (k1 int) stored as orc"""
            hive_docker """create table `${dbName}`.`${tableAbs}` (k1 int) stored as orc"""

            String relLocation = getHiveTableLocation(dbName, tableRel)
            URI tableUri = new URI(relLocation)
            String hdfsUri = "hdfs://${externalEnvIp}:${hdfsPort}"
            if (tableUri.getScheme() != null && tableUri.getAuthority() != null) {
                hdfsUri = tableUri.getScheme() + "://" + tableUri.getAuthority()
            }
            stagingRelBase = "doris_staging_rel_${suffix}"
            stagingAbsBase = "${hdfsUri}/tmp/doris_staging_abs_${suffix}"
            stagingDefaultBase = "/tmp/.doris_staging"
            stagingRelWithUser = new Path(stagingRelBase, currentUser).toString()
            stagingAbsWithUser = new Path(stagingAbsBase, currentUser).toString()
            stagingDefaultWithUser = new Path(stagingDefaultBase, currentUser).toString()
            stagingRelPath = new Path(relLocation, stagingRelWithUser).toString()

            Hdfs hdfs = new Hdfs(hdfsUri, hdfsUser, context.config.dataPath + "/")
            fs = hdfs.fs

            fs.delete(new Path(stagingRelPath), true)
            fs.delete(new Path(stagingAbsWithUser), true)

            sql """drop catalog if exists ${catalogName}"""
            sql """create catalog if not exists ${catalogName} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
                'fs.defaultFS' = '${hdfsUri}'
            );"""
            sql """refresh catalog ${catalogName}"""
            sql """use `${catalogName}`.`${dbName}`"""
            sql """set enable_fallback_to_original_planner=false;"""

            sql """insert into `${tableRel}` values (1)"""
            order_qt_q01 """ select * from `${tableRel}`"""
            assertTrue(fs.exists(new Path(stagingDefaultWithUser)),
                    "default staging dir not created: ${stagingDefaultWithUser}")

            sql """alter catalog ${catalogName} set properties ('hive.staging_dir' = '${stagingRelBase}')"""
            sql """refresh catalog ${catalogName}"""
            sql """insert into `${tableRel}` values (2)"""
            order_qt_q02 """ select * from `${tableRel}`"""
            assertTrue(fs.exists(new Path(stagingRelPath)),
                    "relative staging dir not created: ${stagingRelPath}")
            fs.delete(new Path(stagingRelPath), true)

            sql """alter catalog ${catalogName} set properties ('hive.staging_dir' = '${stagingAbsBase}')"""
            sql """refresh catalog ${catalogName}"""
            sql """insert into `${tableAbs}` values (1)"""
            order_qt_q03 """ select * from `${tableAbs}`"""
            assertTrue(fs.exists(new Path(stagingAbsWithUser)),
                    "absolute staging dir not created: ${stagingAbsWithUser}")
            fs.delete(new Path(stagingAbsWithUser), true)
        } finally {
            try_hive_docker """drop table if exists `${dbName}`.`${tableRel}`"""
            try_hive_docker """drop table if exists `${dbName}`.`${tableAbs}`"""
            try_sql """drop catalog if exists ${catalogName}"""
            if (fs != null) {
                if (stagingRelPath != null) {
                    fs.delete(new Path(stagingRelPath), true)
                }
                if (stagingAbsWithUser != null) {
                    fs.delete(new Path(stagingAbsWithUser), true)
                }
            }
        }
    }
}
