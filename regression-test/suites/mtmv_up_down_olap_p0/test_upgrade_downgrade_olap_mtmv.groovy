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

suite("test_upgrade_downgrade_olap_mtmv","p0,mtmv,restart_fe") {
    String suiteName = "mtmv_up_down_olap"
    String dbName = context.config.getDbNameByFile(context.file)
    String mvName = "${suiteName}_mtmv"
    String tableName = "${suiteName}_table"

    // test data is normal
    order_qt_refresh_init "SELECT * FROM ${mvName}"
    // test is sync
    order_qt_mtmv_sync "select SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${mvName}'"
    sql """
            REFRESH MATERIALIZED VIEW ${mvName} complete
        """
    // test can refresh success
    waitingMTMVTaskFinishedByMvName(mvName)

    String dropTableName1 = """${suiteName}_DropTableName1"""
    String dropTableName2 = """${suiteName}_DropTableName2"""
    String dropTableName4 = """${suiteName}_DropTableName4"""
    String dropMtmvName1 = """${suiteName}_dropMtmvName1"""
    String dropMtmvName2 = """${suiteName}_dropMtmvName2"""
    String dropMtmvName3 = """${suiteName}_dropMtmvName3"""

    // drop table
    sql """drop table if exists ${dropTableName1}"""
    def state_mtmv1 = sql """select State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name = '${dropMtmvName1}';"""
    assertTrue(state_mtmv1[0][0] == "SCHEMA_CHANGE")
    assertTrue(state_mtmv1[0][1] == "SUCCESS" || state_mtmv1[0][1] == "INIT")
    assertTrue(state_mtmv1[0][2] == false)

    // drop partition
    def parts_res = sql """show partitions from ${dropTableName2}"""
    sql """ALTER TABLE ${dropTableName2} DROP PARTITION ${parts_res[0][1]};"""
    def state_mtmv2 = sql """select State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name = '${dropMtmvName2}';"""
    assertTrue(state_mtmv2[0][0] == "NORMAL")
    assertTrue(state_mtmv2[0][1] == "SUCCESS")
    def mtmv_part_res = sql """show partitions from ${dropMtmvName2}"""
    logger.info("mtmv_part_res[0][18]: " + mtmv_part_res[0][18])
    logger.info("mtmv_part_res[0][19]: " + mtmv_part_res[0][19])
    logger.info("mtmv_part_res:" + mtmv_part_res)
    def part_1 = mtmv_part_res.size()
    def diff_part = 0
    for (int i = 0; i < mtmv_part_res.size(); i++) {
        if (mtmv_part_res[i][18] == "false" && mtmv_part_res[i][19] as String == "[${dropTableName2}]") {
            diff_part = diff_part + 1
        }
    }

    def sql2 = "SELECT a.* FROM ${dropTableName2} a inner join ${dropTableName4} b on a.user_id=b.user_id;"
    mv_rewrite_success(sql2, dropMtmvName2)

    // An error occurred when refreshing the partition individually, and the partition was not deleted after the refresh.
    try {
        sql """refresh MATERIALIZED VIEW ${dropMtmvName2} partition(${mtmv_part_res[0][1]})"""
    } catch (Exception e) {
        logger.info(e.getMessage())
    }

    // When refreshing the entire MTMV, the partition will be deleted.
    sql """refresh MATERIALIZED VIEW ${dropMtmvName2} auto"""
    waitingMTMVTaskFinishedByMvName(dropMtmvName2)
    mtmv_part_res = sql """show partitions from ${dropMtmvName2}"""
    logger.info("mtmv_part_res:" + mtmv_part_res)
    def part_2 = mtmv_part_res.size()
    assertTrue(part_1 == part_2 + diff_part)

    state_mtmv2 = sql """select State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name = '${dropMtmvName2}';"""
    logger.info("state_mtmv2:" + state_mtmv2)
    assertTrue(state_mtmv2[0][0] == "NORMAL")
    assertTrue(state_mtmv2[0][1] == "SUCCESS")
    assertTrue(state_mtmv2[0][2] == true)
    mv_rewrite_success(sql2, dropMtmvName2)
    def follower_ip = get_follower_ip()
    if (follower_ip != "null") {
        mv_rewrite_success(sql2, dropMtmvName2,
                enable_sync_mv_cost_based_rewrite(), is_partition_statistics_ready(dbName, [dropTableName2, dropTableName4]), 2)
    }


    // After deleting the table, you can create a new MTMV
    def cur_dropMtmvName3 = dropMtmvName3 + UUID.randomUUID().toString().replaceAll("-", "")
    sql """
        CREATE MATERIALIZED VIEW ${cur_dropMtmvName3}
            REFRESH AUTO ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT user_id, age FROM ${dropTableName4};
        """
    waitingMTMVTaskFinishedByMvName(cur_dropMtmvName3)
    mv_rewrite_success_without_check_chosen("""SELECT user_id FROM ${dropTableName4}""", cur_dropMtmvName3)
    if (follower_ip != "null") {
        mv_rewrite_success_without_check_chosen("""SELECT user_id FROM ${dropTableName4}""", cur_dropMtmvName3,
                enable_sync_mv_cost_based_rewrite(), 2)
    }

}
