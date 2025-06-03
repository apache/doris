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

/*
Build the basic information. When conceiving upgrade and downgrade test cases, you should firmly grasp the object
representation of the function in the FE meta.
Taking MTMV as an example, the main function points are creation, refresh, and rewriting, and the involved entities
are the base table and MTMV.
1.When creating an MTMV, check if the rewriting meets the expectations.
2.When refreshing an MTMV, check if the rewriting meets the expectations.
3.When deleting an MTMV, check if the rewriting meets the expectations.
4.When deleting a base table, check if the rewriting meets the expectations; then trigger a refresh and check if the
rewriting meets the expectations.
5.When deleting a partition of a base table, check if the rewriting meets the expectations; then trigger a refresh and
check if the rewriting meets the expectations.
6.Design a slightly more complex scenario. For example: Build an MTMV with two base tables. When deleting one of the
base tables, check if the refresh of the MTMV meets the expectations and if the rewriting meets the expectations;
create an MTMV with the undeleted base table and check if it can be created and refreshed normally, and if the
corresponding rewriting meets the expectations.
 */
suite("test_upgrade_downgrade_prepare_olap_mtmv_zfr_hive","p0,mtmv,restart_fe") {
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

    sql """drop table if exists `${tableName1}`"""
    sql """drop table if exists `${tableName2}`"""
    sql """drop table if exists `${tableName3}`"""
    sql """drop table if exists `${tableName4}`"""
    sql """drop table if exists `${tableName4_rn}`"""
    sql """drop table if exists `${tableName5}`"""
    sql """drop table if exists `${tableName6}`"""
    sql """drop table if exists `${tableName7}`"""
    sql """drop table if exists `${tableName8}`"""
    sql """drop table if exists `${tableName9}`"""
    sql """drop table if exists `${tableName10}`"""
    sql """drop materialized view if exists ${mtmvName1};"""
    sql """drop materialized view if exists ${mtmvName2};"""
    sql """drop materialized view if exists ${mtmvName3};"""
    sql """drop materialized view if exists ${mtmvName4};"""
    sql """drop materialized view if exists ${mtmvName5};"""
    sql """drop materialized view if exists ${mtmvName6};"""


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

    sql """
        CREATE TABLE `${tableName2}` (
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
        insert into ${tableName2} values(1,"2017-01-15",1),(2,"2017-02-15",2),(3,"2017-03-15",3),(4,"2017-04-15",4),(5,"2017-05-15",5),(6,"2017-06-15",6),(7,"2017-07-15",7),(8,"2017-08-15",8),(9,"2017-09-15",9),(10,"2017-10-15",10),(11,"2017-11-15",11),(12,"2017-12-15",12);
        """

    sql """
        CREATE TABLE `${tableName3}` (
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
        insert into ${tableName3} values(1,"2017-01-15",1),(2,"2017-02-15",2),(3,"2017-03-15",3),(4,"2017-04-15",4),(5,"2017-05-15",5),(6,"2017-06-15",6),(7,"2017-07-15",7),(8,"2017-08-15",8),(9,"2017-09-15",9),(10,"2017-10-15",10),(11,"2017-11-15",11),(12,"2017-12-15",12);
        """

    sql """
        CREATE TABLE `${tableName4}` (
          `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `age` SMALLINT NOT NULL COMMENT '\"年龄\"'
        ) ENGINE=OLAP
        DUPLICATE KEY(`user_id`, `age`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """
    sql """
        insert into ${tableName4} values(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10),(11,11),(12,12);
        """

    sql """
        CREATE TABLE `${tableName5}` (
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
        insert into ${tableName5} values(1,"2017-01-15",1),(2,"2017-02-15",2),(3,"2017-03-15",3),(4,"2017-04-15",4),(5,"2017-05-15",5),(6,"2017-06-15",6),(7,"2017-07-15",7),(8,"2017-08-15",8),(9,"2017-09-15",9),(10,"2017-10-15",10),(11,"2017-11-15",11),(12,"2017-12-15",12);
        """

    sql """
        CREATE TABLE `${tableName6}` (
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
        insert into ${tableName6} values(1,"2017-01-15",1),(2,"2017-02-15",2),(3,"2017-03-15",3),(4,"2017-04-15",4),(5,"2017-05-15",5),(6,"2017-06-15",6),(7,"2017-07-15",7),(8,"2017-08-15",8),(9,"2017-09-15",9),(10,"2017-10-15",10),(11,"2017-11-15",11),(12,"2017-12-15",12);
        """
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
    sql """
        CREATE TABLE `${tableName8}` (
          `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `age` SMALLINT NOT NULL COMMENT '\"年龄\"'
        ) ENGINE=OLAP
        DUPLICATE KEY(`user_id`, `age`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """
    sql """
        insert into ${tableName8} values(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10),(11,11),(12,12);
        """
    sql """
        CREATE TABLE `${tableName9}` (
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
        insert into ${tableName9} values(1,"2017-01-15",1),(2,"2017-02-15",2),(3,"2017-03-15",3),(4,"2017-04-15",4),(5,"2017-05-15",5),(6,"2017-06-15",6),(7,"2017-07-15",7),(8,"2017-08-15",8),(9,"2017-09-15",9),(10,"2017-10-15",10),(11,"2017-11-15",11),(12,"2017-12-15",12);
        """
    sql """
        CREATE TABLE `${tableName10}` (
          `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `age` SMALLINT NOT NULL COMMENT '\"年龄\"'
        ) ENGINE=OLAP
        DUPLICATE KEY(`user_id`, `age`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """
    sql """
        insert into ${tableName10} values(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10),(11,11),(12,12);
        """


    sql """
        CREATE MATERIALIZED VIEW ${mtmvName1}
            REFRESH AUTO ON MANUAL
            partition by(`date`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT a.* FROM ${tableName1} a inner join ${tableName10} b on a.user_id=b.user_id;
        """
    waitingMTMVTaskFinishedByMvName(mtmvName1)

    sql """
        CREATE MATERIALIZED VIEW ${mtmvName2}
            REFRESH AUTO ON MANUAL
            partition by(`date`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT a.* FROM ${tableName2} a inner join ${tableName10} b on a.user_id=b.user_id;
        """
    waitingMTMVTaskFinishedByMvName(mtmvName2)

    sql """
        CREATE MATERIALIZED VIEW ${mtmvName3}
            REFRESH AUTO ON MANUAL
            partition by(`date`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT a.* FROM ${tableName3} a inner join ${tableName10} b on a.user_id=b.user_id;
        """
    waitingMTMVTaskFinishedByMvName(mtmvName3)

    sql """
        CREATE MATERIALIZED VIEW ${mtmvName4}
            REFRESH AUTO ON MANUAL
            partition by(`date`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT a.* FROM ${tableName9} a inner join ${tableName4} b on a.user_id=b.user_id;
        """
    waitingMTMVTaskFinishedByMvName(mtmvName4)

    sql """
        CREATE MATERIALIZED VIEW ${mtmvName5}
            REFRESH AUTO ON MANUAL
            partition by(`date`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT a.* FROM ${tableName5} a inner join ${tableName8} b on a.user_id=b.user_id;
        """
    waitingMTMVTaskFinishedByMvName(mtmvName5)

    sql """
        CREATE MATERIALIZED VIEW ${mtmvName6}
            REFRESH AUTO ON MANUAL
            partition by(`date`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT a.* FROM ${tableName6} a inner join ${tableName7} b on a.user_id=b.user_id;
        """
    waitingMTMVTaskFinishedByMvName(mtmvName6)


    def state_mtmv1 = sql """select State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name = '${mtmvName1}';"""
    assertTrue(state_mtmv1[0][0] == "NORMAL")
    assertTrue(state_mtmv1[0][1] == "SUCCESS")
    assertTrue(state_mtmv1[0][2] == true)
    def state_mtmv2 = sql """select State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name = '${mtmvName2}';"""
    assertTrue(state_mtmv2[0][0] == "NORMAL")
    assertTrue(state_mtmv2[0][1] == "SUCCESS")
    assertTrue(state_mtmv2[0][2] == true)
    def state_mtmv3 = sql """select State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name = '${mtmvName3}';"""
    assertTrue(state_mtmv3[0][0] == "NORMAL")
    assertTrue(state_mtmv3[0][1] == "SUCCESS")
    assertTrue(state_mtmv3[0][2] == true)
    def state_mtmv4 = sql """select State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name = '${mtmvName4}';"""
    assertTrue(state_mtmv4[0][0] == "NORMAL")
    assertTrue(state_mtmv4[0][1] == "SUCCESS")
    assertTrue(state_mtmv4[0][2] == true)
    def state_mtmv5 = sql """select State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name = '${mtmvName5}';"""
    assertTrue(state_mtmv5[0][0] == "NORMAL")
    assertTrue(state_mtmv5[0][1] == "SUCCESS")
    assertTrue(state_mtmv5[0][2] == true)
    def state_mtmv6 = sql """select State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name = '${mtmvName6}';"""
    assertTrue(state_mtmv6[0][0] == "NORMAL")
    assertTrue(state_mtmv6[0][1] == "SUCCESS")
    assertTrue(state_mtmv6[0][2] == true)

}
