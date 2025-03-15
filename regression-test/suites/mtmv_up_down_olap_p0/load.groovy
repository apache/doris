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
suite("test_upgrade_downgrade_prepare_olap_mtmv","p0,mtmv,restart_fe") {
    String suiteName = "mtmv_up_down_olap"
    String dbName = context.config.getDbNameByFile(context.file)
    String mvName = "${suiteName}_mtmv"
    String tableName = "${suiteName}_table"
    String tableName2 = "${suiteName}_table2"

    sql """drop materialized view if exists ${mvName};"""
    sql """drop table if exists `${tableName}`"""
    sql """drop table if exists `${tableName2}`"""

    sql """
        CREATE TABLE `${tableName}` (
          `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `date` DATE NOT NULL COMMENT '\"数据灌入日期时间\"',
          `num` SMALLINT NOT NULL COMMENT '\"数量\"'
        ) ENGINE=OLAP
        DUPLICATE KEY(`user_id`, `date`, `num`)
        COMMENT 'OLAP'
        PARTITION BY RANGE(`date`)
        (PARTITION p201701_1000 VALUES [('0000-01-01'), ('2017-02-01')),
        PARTITION p201702_2000 VALUES [('2017-02-01'), ('2017-03-01')),
        PARTITION p201703_all VALUES [('2017-03-01'), ('2017-04-01')))
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """
    sql """
        insert into ${tableName} values(1,"2017-01-15",1),(2,"2017-02-15",2),(3,"2017-03-15",3);
        """

    sql """
        CREATE TABLE `${tableName2}` (
          `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `age` SMALLINT NOT NULL COMMENT '\"年龄\"'
        ) ENGINE=OLAP
        DUPLICATE KEY(`user_id`, `age`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """
    sql """
        insert into ${tableName2} values(1,1),(2,2),(3,3);
        """

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
            REFRESH AUTO ON MANUAL
            partition by(`date`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT a.* FROM ${tableName} a inner join ${tableName2} b on a.user_id=b.user_id;
    """
    waitingMTMVTaskFinishedByMvName(mvName)


    String dropTableName1 = """${suiteName}_DropTableName1"""
    String dropTableName2 = """${suiteName}_DropTableName2"""
    String dropTableName4 = """${suiteName}_DropTableName4"""
    String dropMtmvName1 = """${suiteName}_dropMtmvName1"""
    String dropMtmvName2 = """${suiteName}_dropMtmvName2"""
    String dropMtmvName3 = """${suiteName}_dropMtmvName3"""

    sql """drop materialized view if exists ${dropMtmvName1};"""
    sql """drop materialized view if exists ${dropMtmvName2};"""
    sql """drop materialized view if exists ${dropMtmvName3};"""
    sql """drop table if exists `${dropTableName1}`"""
    sql """drop table if exists `${dropTableName2}`"""
    sql """drop table if exists `${dropTableName4}`"""


    sql """
        CREATE TABLE `${dropTableName1}` (
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
        insert into ${dropTableName1} values(1,"2017-01-15",1),(2,"2017-02-15",2),(3,"2017-03-15",3),(4,"2017-04-15",4),(5,"2017-05-15",5),(6,"2017-06-15",6),(7,"2017-07-15",7),(8,"2017-08-15",8),(9,"2017-09-15",9),(10,"2017-10-15",10),(11,"2017-11-15",11),(12,"2017-12-15",12);
        """

    sql """
        CREATE TABLE `${dropTableName2}` (
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
        insert into ${dropTableName2} values(1,"2017-01-15",1),(2,"2017-02-15",2),(3,"2017-03-15",3),(4,"2017-04-15",4),(5,"2017-05-15",5),(6,"2017-06-15",6),(7,"2017-07-15",7),(8,"2017-08-15",8),(9,"2017-09-15",9),(10,"2017-10-15",10),(11,"2017-11-15",11),(12,"2017-12-15",12);
        """

    sql """
        CREATE TABLE `${dropTableName4}` (
          `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `age` SMALLINT NOT NULL COMMENT '\"年龄\"'
        ) ENGINE=OLAP
        DUPLICATE KEY(`user_id`, `age`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """
    sql """
        insert into ${dropTableName4} values(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10),(11,11),(12,12);
        """


    sql """
        CREATE MATERIALIZED VIEW ${dropMtmvName1}
            REFRESH AUTO ON MANUAL
            partition by(`date`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT a.* FROM ${dropTableName1} a inner join ${dropTableName4} b on a.user_id=b.user_id;
    """
    waitingMTMVTaskFinishedByMvName(dropMtmvName1)

    sql """
        CREATE MATERIALIZED VIEW ${dropMtmvName2}
            REFRESH AUTO ON MANUAL
            partition by(`date`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT a.* FROM ${dropTableName2} a inner join ${dropTableName4} b on a.user_id=b.user_id;
    """
    waitingMTMVTaskFinishedByMvName(dropMtmvName2)

    def state_mtmv1 = sql """select State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name = '${dropMtmvName1}';"""
    assertTrue(state_mtmv1[0][0] == "NORMAL")
    assertTrue(state_mtmv1[0][1] == "SUCCESS")
    assertTrue(state_mtmv1[0][2] == true)
    def state_mtmv2 = sql """select State,RefreshState,SyncWithBaseTables from mv_infos('database'='${dbName}') where Name = '${dropMtmvName2}';"""
    assertTrue(state_mtmv2[0][0] == "NORMAL")
    assertTrue(state_mtmv2[0][1] == "SUCCESS")
    assertTrue(state_mtmv2[0][2] == true)

}
