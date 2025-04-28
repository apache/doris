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

suite("test_hudi_olap_rewrite_mtmv", "p2,external,hudi,external_remote,external_remote_hudi") {
    String enabled = context.config.otherConfigs.get("enableExternalHudiTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disabled hudi test")
        return
    }
    String suiteName = "test_hudi_olap_rewrite_mtmv"
    String catalogName = "${suiteName}_catalog"
    String mvName = "${suiteName}_mv"
    String dbName = context.config.getDbNameByFile(context.file)
    String tableName = "${suiteName}_table"
    sql """drop table if exists ${tableName}"""
    sql """
        CREATE TABLE ${tableName} (
          `user_id` INT,
          `num` INT
        ) ENGINE=OLAP
        DUPLICATE KEY(`user_id`)
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
       """
    sql """
        insert into ${tableName} values(1,2);
        """

    sql """analyze table internal.`${dbName}`. ${tableName} with sync"""
    sql """alter table internal.`${dbName}`. ${tableName} modify column user_id set stats ('row_count'='1');"""

    String props = context.config.otherConfigs.get("hudiEmrCatalog")

    sql """set materialized_view_rewrite_enable_contain_external_table=true;"""
    String mvSql = "SELECT * FROM ${catalogName}.`hudi_mtmv_regression_test`.hudi_table_1 a left join ${tableName} b on a.id=b.user_id;";

    sql """drop catalog if exists ${catalogName}"""
    sql """CREATE CATALOG if not exists ${catalogName} PROPERTIES (
            ${props}
        );"""

    sql """analyze table ${catalogName}.`hudi_mtmv_regression_test`.hudi_table_1 with sync"""
    sql """alter table ${catalogName}.`hudi_mtmv_regression_test`.hudi_table_1 modify column par set stats ('row_count'='10');"""

    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by(`par`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
             ${mvSql}
        """
    def showPartitionsResult = sql """show partitions from ${mvName}"""
    logger.info("showPartitionsResult: " + showPartitionsResult.toString())
    assertTrue(showPartitionsResult.toString().contains("p_a"))
    assertTrue(showPartitionsResult.toString().contains("p_b"))

    // refresh one partitions
    sql """
            REFRESH MATERIALIZED VIEW ${mvName} partitions(p_a);
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_refresh_one_partition "SELECT * FROM ${mvName} "

    def explainOnePartition = sql """ explain  ${mvSql} """
    logger.info("explainOnePartition: " + explainOnePartition.toString())
    assertTrue(explainOnePartition.toString().contains("VUNION"))
    order_qt_refresh_one_partition_rewrite "${mvSql}"

    mv_rewrite_success("${mvSql}", "${mvName}")

    // select p_b should not rewrite
    mv_rewrite_fail("SELECT * FROM ${catalogName}.`hudi_mtmv_regression_test`.hudi_table_1 a left join ${tableName} b on a.id=b.user_id where a.par='b';", "${mvName}")

    //refresh auto
    sql """
            REFRESH MATERIALIZED VIEW ${mvName} auto
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_refresh_auto "SELECT * FROM ${mvName} "

    def explainAllPartition = sql """ explain  ${mvSql}; """
    logger.info("explainAllPartition: " + explainAllPartition.toString())
    assertTrue(explainAllPartition.toString().contains("VOlapScanNode"))
    order_qt_refresh_all_partition_rewrite "${mvSql}"

    mv_rewrite_success("${mvSql}", "${mvName}")

    sql """drop materialized view if exists ${mvName};"""
    sql """drop catalog if exists ${catalogName}"""
}
