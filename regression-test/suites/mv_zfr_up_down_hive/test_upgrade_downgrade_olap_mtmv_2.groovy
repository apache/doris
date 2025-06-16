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

suite("test_upgrade_downgrade_olap_mtmv_zfr_hive_2","p0,mtmv,restart_fe") {

    def waitingMTMVTaskByMvName = { mvName, dbName ->
        Thread.sleep(2000);
        String showTasks = "select TaskId,JobId,JobName,MvId,Status,MvName,MvDatabaseName,ErrorMsg from tasks('type'='mv') where MvDatabaseName = '${dbName}' and MvName = '${mvName}' order by CreateTime ASC"
        String status = "NULL"
        List<List<Object>> result
        long startTime = System.currentTimeMillis()
        long timeoutTimestamp = startTime + 5 * 60 * 1000 // 5 min
        List<String> toCheckTaskRow = new ArrayList<>();
        while (timeoutTimestamp > System.currentTimeMillis() && (status == 'PENDING' || status == 'RUNNING' || status == 'NULL')) {
            result = sql(showTasks)
            logger.info("current db is " + dbName + ", showTasks is " + result.toString())
            if (result.isEmpty()) {
                logger.info("waitingMTMVTaskFinishedByMvName toCheckTaskRow is empty")
                Thread.sleep(1000);
                continue;
            }
            toCheckTaskRow = result.last();
            status = toCheckTaskRow.get(4)
            logger.info("The state of ${showTasks} is ${status}")
            Thread.sleep(1000);
        }
    }

    String suiteName = "mtmv_up_down_olap_hive"
    String ctlName = "${suiteName}_ctl"
    String dbName = context.config.getDbNameByFile(context.file)

    String tableName1 = """${suiteName}_tb1"""
    String tableName2 = """${suiteName}_tb2"""
    String tableName3 = """${suiteName}_tb3"""
    String tableName4 = """${suiteName}_tb4"""
    String tableName4_rn = """${suiteName}_tb4_rn"""
    String tableName5 = """${suiteName}_tb5"""
    String tableName6 = """${suiteName}_tb6"""
    String tableName7 = """${suiteName}_tb7"""
    String tableName8 = """${suiteName}_tb8"""
    String tableName9 = """${suiteName}_tb9"""
    String tableName10 = """${suiteName}_tb10"""
    String mtmvName1 = """${suiteName}_mtmv1"""
    String mtmvName2 = """${suiteName}_mtmv2"""
    String mtmvName3 = """${suiteName}_mtmv3"""
    String mtmvName4 = """${suiteName}_mtmv4"""
    String mtmvName5 = """${suiteName}_mtmv5"""
    String mtmvName6 = """${suiteName}_mtmv6"""

    def get_follower_ip = {
        def result = sql """show frontends;"""
        logger.info("result:" + result)
        for (int i = 0; i < result.size(); i++) {
            if (result[i][7] == "FOLLOWER" && result[i][8] == "false" && result[i][11] == "true") {
                return result[i][1]
            }
        }
        return "null"
    }
    def get_master_ip = {
        def result = sql """show frontends;"""
        logger.info("result:" + result)
        for (int i = 0; i < result.size(); i++) {
            if (result[i][7] == "FOLLOWER" && result[i][8] == "true" && result[i][11] == "true") {
                return result[i][1]
            }
        }
        return "null"
    }
    def compare_res = { def stmt ->
        sql "SET enable_materialized_view_rewrite=false"
        def origin_res = sql stmt
        logger.info("origin_res: " + origin_res)
        sql "SET enable_materialized_view_rewrite=true"
        def mv_origin_res = sql stmt
        logger.info("mv_origin_res: " + mv_origin_res)
        assertTrue((mv_origin_res == [] && origin_res == []) || (mv_origin_res.size() == origin_res.size()))
        for (int row = 0; row < mv_origin_res.size(); row++) {
            assertTrue(mv_origin_res[row].size() == origin_res[row].size())
            for (int col = 0; col < mv_origin_res[row].size(); col++) {
                assertTrue(mv_origin_res[row][col] == origin_res[row][col])
            }
        }
    }
    def follower_ip = get_follower_ip()
    def master_ip = get_master_ip()

    def tokens = context.config.jdbcUrl.split('/')
    def url_tmp1 = tokens[0] + "//" + tokens[2] + "/" + "information_schema" + "?useLocalSessionState=true&allowLoadLocalInfile=true&zeroDateTimeBehavior=round"
    def follower_jdbc_url = url_tmp1.replaceAll(/\/\/[0-9.]+:/, "//${follower_ip}:")
    logger.info("follower_jdbc_url: " + follower_jdbc_url)

    def master_jdbc_url = url_tmp1.replaceAll(/\/\/[0-9.]+:/, "//${master_ip}:")
    logger.info("master_jdbc_url: " + master_jdbc_url)


    // 通过检查fe角色的版本号来判断当前是什么阶段
    // eg：从 c8570fd6c0 ——> e52040eb9c
    // 1. 所有fe和be角色都是old
    // 2. 升级所有be。    fe都为old， be为new
    // 3. 升级follower。 master为old，而其他fe为new
    // 4. 升级master。   所有fe都为new
    // 通过判断所有节点版本，来控制在什么步骤进行重建表的操作

    def old_version = "c8570fd6c0"
    def new_version = "e52040eb9c"
    def be_res = sql """show backends;"""
    def fe_res = sql """show frontends"""
    def be_status = 0
    def fe_old = 0
    def fe_new = 0
    for (int i = 0; i < be_res.size(); i++) {
        if (be_res[i][21].toString().indexOf(new_version) != -1) {
            be_status = 1
            break
        }
    }
    for (int i = 0; i < fe_res.size(); i++) {
        if (fe_res[i][21].toString().indexOf(old_version) != -1) {
            fe_old ++
        } else {
            fe_new++
        }
    }
    def step = 0
    if (be_status == 0) {
        step = 1
    } else if (be_status == 1 && fe_old == 3 && fe_new == 0) {
        step = 2
    } else if (be_status == 1 && fe_old == 1 && fe_new == 2) {
        step = 3
    }  else if (be_status == 1 && fe_old == 0 && fe_new == 3) {
        step = 4
    }
    assertTrue(step != 0)

    String hivePrefix = "hive3"
    setHivePrefix(hivePrefix)

    hive_docker """ set hive.stats.column.autogather = false; """

    sql """switch internal;"""
    // mtmv2: add partition
    sql """insert into ${ctlName}.${dbName}.${tableName2} values(13,13,"2018-01-15");"""
    hive_docker """insert into ${dbName}.${tableName1} PARTITION(dt='2018-01-15') values (13,13)"""
    def state_mtmv2 = sql """select State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name = '${mtmvName2}';"""
    def sql2 = "SELECT a.* FROM ${ctlName}.${dbName}.${tableName2} a inner join ${ctlName}.${dbName}.${tableName10} b on a.user_id=b.user_id"

    if (step == 1 || step == 2 || step == 3) {
        assertTrue(state_mtmv2[0][0] == "NORMAL")
        assertTrue(state_mtmv2[0][2] == true)

        connect('root', context.config.jdbcPassword, follower_jdbc_url) {
            sql """set materialized_view_rewrite_enable_contain_external_table=true;"""
            sql """use ${dbName}"""
            mv_rewrite_success_without_check_chosen(sql2, mtmvName2)
            compare_res(sql2 + " order by 1,2,3")
        }

        connect('root', context.config.jdbcPassword, master_jdbc_url) {
            sql """set materialized_view_rewrite_enable_contain_external_table=true;"""
            sql """use ${dbName}"""
            mv_rewrite_success_without_check_chosen(sql2, mtmvName2)
            compare_res(sql2 + " order by 1,2,3")
        }

        // An error occurred when refreshing the partition individually, and the partition was not deleted after the refresh.
        try {
            sql """refresh MATERIALIZED VIEW ${mtmvName2} partition(p_20180115)"""
        } catch (Exception e) {
            logger.info("refresh MATERIALIZED VIEW: ${mtmvName2}")
            logger.info(e.getMessage())
        }

        // When refreshing the entire MTMV, the partition will be deleted.
        sql """refresh MATERIALIZED VIEW ${mtmvName2} complete"""
        waitingMTMVTaskFinishedByMvName(mtmvName2)

        state_mtmv2 = sql """select State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name = '${mtmvName2}';"""
        logger.info("state_mtmv2:" + state_mtmv2)
        assertTrue(state_mtmv2[0][0] == "NORMAL")
        assertTrue(state_mtmv2[0][1] == "SUCCESS")
        assertTrue(state_mtmv2[0][2] == true)

        connect('root', context.config.jdbcPassword, follower_jdbc_url) {
            sql """set materialized_view_rewrite_enable_contain_external_table=true;"""
            sql """use ${dbName}"""
            mv_rewrite_success_without_check_chosen(sql2, mtmvName2)
            compare_res(sql2 + " order by 1,2,3")
        }

        connect('root', context.config.jdbcPassword, master_jdbc_url) {
            sql """set materialized_view_rewrite_enable_contain_external_table=true;"""
            sql """use ${dbName}"""
            mv_rewrite_success_without_check_chosen(sql2, mtmvName2)
            compare_res(sql2 + " order by 1,2,3")
        }
    } else if (step == 4) {

        assertTrue(state_mtmv2[0][0] == "SCHEMA_CHANGE")
        assertTrue(state_mtmv2[0][1] == "SUCCESS")
        assertTrue(state_mtmv2[0][2] == false)
        def mtmv_part_res = sql """show partitions from ${mtmvName2}"""
        logger.info("mtmv_part_res[0][18]: " + mtmv_part_res[0][18])
        logger.info("mtmv_part_res[0][19]: " + mtmv_part_res[0][19])
        logger.info("mtmv_part_res:" + mtmv_part_res)
        def part_1 = mtmv_part_res.size()
        def diff_part = 0
        for (int i = 0; i < mtmv_part_res.size(); i++) {
            if (mtmv_part_res[i][18] == "false" && mtmv_part_res[i][19] as String == "[${tableName2}]") {
                diff_part = diff_part + 1
            }
        }

        connect('root', context.config.jdbcPassword, follower_jdbc_url) {
            sql """set materialized_view_rewrite_enable_contain_external_table=true;"""
            sql """use ${dbName}"""
            mv_not_part_in(sql2, mtmvName2)
        }

        connect('root', context.config.jdbcPassword, master_jdbc_url) {
            sql """set materialized_view_rewrite_enable_contain_external_table=true;"""
            sql """use ${dbName}"""
            mv_not_part_in(sql2, mtmvName2)
        }

        // An error occurred when refreshing the partition individually, and the partition was not deleted after the refresh.
        try {
            sql """refresh MATERIALIZED VIEW ${mtmvName2} partition(${mtmv_part_res[0][1]})"""
        } catch (Exception e) {
            logger.info("refresh MATERIALIZED VIEW: ${mtmvName2}")
            logger.info(e.getMessage())
        }

        // When refreshing the entire MTMV, the partition will be deleted.
        sql """refresh MATERIALIZED VIEW ${mtmvName2} complete"""
        waitingMTMVTaskFinishedByMvName(mtmvName2)
        mtmv_part_res = sql """show partitions from ${mtmvName2}"""
        logger.info("mtmv_part_res:" + mtmv_part_res)
        def part_2 = mtmv_part_res.size()
        assertTrue(part_1 == part_2 + diff_part)

        state_mtmv2 = sql """select State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name = '${mtmvName2}';"""
        logger.info("state_mtmv2:" + state_mtmv2)
        assertTrue(state_mtmv2[0][0] == "NORMAL")
        assertTrue(state_mtmv2[0][1] == "SUCCESS")
        assertTrue(state_mtmv2[0][2] == true)

        connect('root', context.config.jdbcPassword, follower_jdbc_url) {
            sql """set materialized_view_rewrite_enable_contain_external_table=true;"""
            sql """use ${dbName}"""
            mv_rewrite_success_without_check_chosen(sql2, mtmvName2)
            compare_res(sql2 + " order by 1,2,3")
        }

        connect('root', context.config.jdbcPassword, master_jdbc_url) {
            sql """set materialized_view_rewrite_enable_contain_external_table=true;"""
            sql """use ${dbName}"""
            mv_rewrite_success_without_check_chosen(sql2, mtmvName2)
            compare_res(sql2 + " order by 1,2,3")
        }

    }

}
