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

suite("test_hive_rewrite_mtmv", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }
    String suiteName = "test_hive_rewrite_mtmv"
    String catalogName = "${suiteName}_catalog"
    String mvName = "${suiteName}_mv"
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    sql """set materialized_view_rewrite_enable_contain_external_table=true;"""
    String mvSql = "SELECT part_col,count(*) as num FROM ${catalogName}.`default`.mtmv_base1 group by part_col;";
    for (String hivePrefix : ["hive2", "hive3"]) {
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        sql """drop catalog if exists ${catalogName}"""
        sql """create catalog if not exists ${catalogName} properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
        );"""
        sql """analyze table ${catalogName}.`default`.mtmv_base1 with sync"""
        sql """alter table ${catalogName}.`default`.mtmv_base1 modify column part_col set stats ('row_count'='6');"""

        sql """drop materialized view if exists ${mvName};"""
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
                BUILD DEFERRED REFRESH AUTO ON MANUAL
                partition by(`part_col`)
                DISTRIBUTED BY RANDOM BUCKETS 2
                PROPERTIES ('replication_num' = '1')
                AS
                ${mvSql}
            """
        def showPartitionsResult = sql """show partitions from ${mvName}"""
        logger.info("showPartitionsResult: " + showPartitionsResult.toString())
        assertTrue(showPartitionsResult.toString().contains("p_20230101"))
        assertTrue(showPartitionsResult.toString().contains("p_20230102"))

        // refresh one partitions
        sql """
                REFRESH MATERIALIZED VIEW ${mvName} partitions(p_20230101);
            """
        waitingMTMVTaskFinishedByMvName(mvName)
        order_qt_refresh_one_partition "SELECT * FROM ${mvName}"

        def explainOnePartition = sql """ explain  ${mvSql} """
        logger.info("explainOnePartition: " + explainOnePartition.toString())
        assertTrue(explainOnePartition.toString().contains("VUNION"))
        assertTrue(explainOnePartition.toString().contains("part_col[#4] = 20230102"))
        order_qt_refresh_one_partition_rewrite "${mvSql}"

        mv_rewrite_success("${mvSql}", "${mvName}")

        //refresh complete
        sql """
                REFRESH MATERIALIZED VIEW ${mvName} complete
            """
        waitingMTMVTaskFinishedByMvName(mvName)
        order_qt_refresh_complete "SELECT * FROM ${mvName}"

        def explainAllPartition = sql """ explain  ${mvSql}; """
        logger.info("explainAllPartition: " + explainAllPartition.toString())
        assertTrue(explainAllPartition.toString().contains("VOlapScanNode"))
        assertTrue(explainAllPartition.toString().contains("partitions=2/2"))
        order_qt_refresh_all_partition_rewrite "${mvSql}"

        mv_rewrite_success("${mvSql}", "${mvName}")

        sql """drop materialized view if exists ${mvName};"""
        sql """drop catalog if exists ${catalogName}"""
    }
}

