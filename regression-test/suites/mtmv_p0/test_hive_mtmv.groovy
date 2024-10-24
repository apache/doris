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

suite("test_hive_mtmv", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        try {
            String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String catalog_name = "${hivePrefix}_test_mtmv"
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

            sql """drop catalog if exists ${catalog_name}"""
            sql """create catalog if not exists ${catalog_name} properties (
                "type"="hms",
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
            );"""
            // sql """use `${catalog_name}`.`default`"""
            def mvName = "test_hive_mtmv"
            def dbName = "regression_test_mtmv_p0"
            sql """drop materialized view if exists ${mvName};"""

             test {
                sql """CREATE MATERIALIZED VIEW ${catalog_name}.`default`.${mvName}
                           BUILD DEFERRED REFRESH AUTO ON MANUAL
                           DISTRIBUTED BY RANDOM BUCKETS 2
                           PROPERTIES ('replication_num' = '1')
                           AS
                           SELECT * FROM ${catalog_name}.`default`.mtmv_base1;"""
                exception "internal"
            }

            sql """
                CREATE MATERIALIZED VIEW ${mvName}
                    BUILD DEFERRED REFRESH AUTO ON MANUAL
                    partition by(`part_col`)
                    DISTRIBUTED BY RANDOM BUCKETS 2
                    PROPERTIES ('replication_num' = '1')
                    AS
                    SELECT * FROM ${catalog_name}.`default`.mtmv_base1;
                """
            def showPartitionsResult = sql """show partitions from ${mvName}"""
            logger.info("showPartitionsResult: " + showPartitionsResult.toString())
            assertTrue(showPartitionsResult.toString().contains("p_20230101"))
            assertTrue(showPartitionsResult.toString().contains("p_20230102"))

            // refresh one partitions
            sql """
                    REFRESH MATERIALIZED VIEW ${mvName} partitions(p_20230101);
                """
            def jobName = getJobName(dbName, mvName);
            log.info(jobName)
            waitingMTMVTaskFinished(jobName)
            order_qt_refresh_one_partition "SELECT * FROM ${mvName} order by id"

            //refresh complete
            sql """
                    REFRESH MATERIALIZED VIEW ${mvName} complete
                """
            waitingMTMVTaskFinished(jobName)
            order_qt_refresh_complete "SELECT * FROM ${mvName} order by id"

            order_qt_is_sync_before_rebuild "select SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${mvName}'"
           // rebuild catalog, should not Affects MTMV
            sql """drop catalog if exists ${catalog_name}"""
            sql """create catalog if not exists ${catalog_name} properties (
                "type"="hms",
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
            );"""

            order_qt_is_sync_after_rebuild "select SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${mvName}'"

            // should refresh normal after catalog rebuild
            sql """
                    REFRESH MATERIALIZED VIEW ${mvName} complete
                """
            waitingMTMVTaskFinished(jobName)
            order_qt_refresh_complete_rebuild "SELECT * FROM ${mvName} order by id"

            sql """drop materialized view if exists ${mvName};"""
            sql """drop catalog if exists ${catalog_name}"""
        } finally {
        }
    }
}

