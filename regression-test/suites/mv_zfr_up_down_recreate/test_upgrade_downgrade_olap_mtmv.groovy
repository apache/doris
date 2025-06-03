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

suite("test_upgrade_downgrade_olap_mtmv_zfr_recreate","p0,mtmv,restart_fe") {

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

    String suiteName = "mtmv_up_down_olap"
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
    def url_tmp1 = tokens[0] + "//" + tokens[2] + "/" + "information_schema" + "?"
    def follower_jdbc_url = url_tmp1.replaceAll(/\/\/[0-9.]+:/, "//${follower_ip}:")
    logger.info("follower_jdbc_url: " + follower_jdbc_url)

    def master_jdbc_url = url_tmp1.replaceAll(/\/\/[0-9.]+:/, "//${master_ip}:")
    logger.info("master_jdbc_url: " + master_jdbc_url)



    // 重建表table1
    sql """
        CREATE TABLE `${tableName1}` (
          `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `date` DATE NOT NULL COMMENT '\"数据灌入日期时间\"',
          `num` SMALLINT NOT NULL COMMENT '\"数量\"'
        ) ENGINE=OLAP
        DUPLICATE KEY(`user_id`, `date`, `num`)
        COMMENT 'OLAP'
        PARTITION BY RANGE(`date`)
        (PARTITION p201701_1000 VALUES [('0000-01-01'), ('2017-02-01')),
        PARTITION p201702_2000 VALUES [('2017-02-01'), ('2017-03-01')),
        PARTITION p201703_3000 VALUES [('2017-03-01'), ('2017-04-01')),
        PARTITION p201704_4000 VALUES [('2017-04-01'), ('2017-05-01')),
        PARTITION p201705_5000 VALUES [('2017-05-01'), ('2017-06-01')),
        PARTITION p201706_6000 VALUES [('2017-06-01'), ('2017-07-01')),
        PARTITION p201707_7000 VALUES [('2017-07-01'), ('2017-08-01')),
        PARTITION p201708_8000 VALUES [('2017-08-01'), ('2017-09-01')),
        PARTITION p201709_9000 VALUES [('2017-09-01'), ('2017-10-01')),
        PARTITION p201710_1000 VALUES [('2017-10-01'), ('2017-11-01')),
        PARTITION p201711_1100 VALUES [('2017-11-01'), ('2017-12-01')),
        PARTITION p201712_1200 VALUES [('2017-12-01'), ('2018-01-01')))
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """
    sql """
        insert into ${tableName1} values(1,"2017-01-15",1),(2,"2017-02-15",2),(3,"2017-03-15",3),(4,"2017-04-15",4),(5,"2017-05-15",5),(6,"2017-06-15",6),(7,"2017-07-15",7),(8,"2017-08-15",8),(9,"2017-09-15",9),(10,"2017-10-15",10),(11,"2017-11-15",11),(12,"2017-12-15",12);
        """

    sql """refresh MATERIALIZED VIEW ${mtmvName1} auto"""
    waitingMTMVTaskFinishedByMvName(mtmvName1)

    def test_sql1 = """SELECT a.* FROM ${tableName1} a inner join ${tableName10} b on a.user_id=b.user_id"""

    connect('root', context.config.jdbcPassword, follower_jdbc_url) {
        def state_mtmv1 = sql """select State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name = '${mtmvName1}';"""
        assertTrue(state_mtmv1[0][0] == "NORMAL")
        assertTrue(state_mtmv1[0][1] == "SUCCESS")
        assertTrue(state_mtmv1[0][2] == true)

        sql """use ${dbName}"""
        mv_rewrite_success(test_sql1, mtmvName1)
        compare_res(test_sql1 + " order by 1,2,3")
    }

    connect('root', context.config.jdbcPassword, master_jdbc_url) {
        def state_mtmv1 = sql """select State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name = '${mtmvName1}';"""
        assertTrue(state_mtmv1[0][0] == "NORMAL")
        assertTrue(state_mtmv1[0][1] == "SUCCESS")
        assertTrue(state_mtmv1[0][2] == true)

        sql """use ${dbName}"""
        mv_rewrite_success(test_sql1, mtmvName1)
        compare_res(test_sql1 + " order by 1,2,3")
    }

    def cur_mtmvName3 = mtmvName3 + UUID.randomUUID().toString().replaceAll("-", "")
    sql """
        CREATE MATERIALIZED VIEW ${cur_mtmvName3}
            REFRESH AUTO ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT user_id, age FROM ${tableName10};
        """
    waitingMTMVTaskFinishedByMvName(cur_mtmvName3)


    // 重建分区
    sql """ALTER TABLE ${tableName2} DROP PARTITION p201701_1000 VALUES [('0000-01-01'), ('2017-02-01'));"""
    sql """
        insert into ${tableName2} values(1,"2017-01-15",1);
        """
    sql """refresh MATERIALIZED VIEW ${mtmvName2} auto"""
    waitingMTMVTaskFinishedByMvName(mtmvName2)

    def state_mtmv2 = sql """select State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name = '${mtmvName2}';"""
    logger.info("state_mtmv2:" + state_mtmv2)
    assertTrue(state_mtmv2[0][0] == "NORMAL")
    assertTrue(state_mtmv2[0][1] == "SUCCESS")
    assertTrue(state_mtmv2[0][2] == true)

    connect('root', context.config.jdbcPassword, follower_jdbc_url) {
        sql """use ${dbName}"""
        mv_rewrite_success(sql2, mtmvName2)
        compare_res(sql2 + " order by 1,2,3")
    }

    connect('root', context.config.jdbcPassword, master_jdbc_url) {
        sql """use ${dbName}"""
        mv_rewrite_success(sql2, mtmvName2)
        compare_res(sql2 + " order by 1,2,3")
    }


    // 重建表table7
    sql """
        CREATE TABLE `${tableName7}` (
          `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `age` SMALLINT NOT NULL COMMENT '\"年龄\"'
        ) ENGINE=OLAP
        DUPLICATE KEY(`user_id`, `age`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """
    sql """
        insert into ${tableName7} values(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10),(11,11),(12,12);
        """

    sql """refresh MATERIALIZED VIEW ${mtmvName6} auto"""
    waitingMTMVTaskFinishedByMvName(mtmvName6)

    def test_sql6 = """SELECT a.* FROM ${tableName6} a inner join ${tableName7} b on a.user_id=b.user_id"""

    connect('root', context.config.jdbcPassword, follower_jdbc_url) {
        def state_mtmv6 = sql """select State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name = '${mtmvName6}';"""
        assertTrue(state_mtmv6[0][0] == "NORMAL")
        assertTrue(state_mtmv6[0][1] == "SUCCESS")
        assertTrue(state_mtmv6[0][2] == true)

        sql """use ${dbName}"""
        mv_rewrite_success(test_sql6, mtmvName6)
        compare_res(test_sql6 + " order by 1,2,3")
    }

    connect('root', context.config.jdbcPassword, master_jdbc_url) {
        def state_mtmv6 = sql """select State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name = '${mtmvName6}';"""
        assertTrue(state_mtmv6[0][0] == "NORMAL")
        assertTrue(state_mtmv6[0][1] == "SUCCESS")
        assertTrue(state_mtmv6[0][2] == true)

        sql """use ${dbName}"""
        mv_rewrite_success(test_sql6, state_mtmv6)
        compare_res(test_sql6 + " order by 1,2,3")
    }

    def cur_mtmvName6 = mtmvName6 + UUID.randomUUID().toString().replaceAll("-", "")
    sql """
        CREATE MATERIALIZED VIEW ${cur_mtmvName6}
            REFRESH AUTO ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT user_id, date, num FROM ${tableName6};
        """
    waitingMTMVTaskFinishedByMvName(cur_mtmvName6)

}
