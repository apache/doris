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
    String suiteName = "mtmv_up_down_olap_hive"
    String ctlName = "${suiteName}_ctl"
    String dbName = context.config.getDbNameByFile(context.file)

//    sql """create catalog if not exists ${ctlName} properties (
//        "type"="hms",
//        'hive.metastore.uris' = 'thrift://172.20.48.119:9383',
//        'fs.defaultFS' = 'hdfs://172.20.48.119:8320',
//        'hadoop.username' = 'hadoop',
//        'enable.auto.analyze' = 'false'
//        );"""
//    sql """switch ${ctlName}"""
//    sql """create database if not exists ${dbName}"""
//    sql """use ${dbName}"""

    String hivePrefix = "hive3"
    setHivePrefix(hivePrefix)

    hive_docker """ set hive.stats.column.autogather = false; """


    String tableName1 = """${suiteName}_tb1"""
    String tableName2 = """${suiteName}_tb2"""
    String tableName3 = """${suiteName}_tb3"""
    String tableName4 = """${suiteName}_tb4"""
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

    hive_docker """ drop table if exists ${dbName}.${tableName1} """
    hive_docker """ drop table if exists ${dbName}.${tableName2} """
    hive_docker """ drop table if exists ${dbName}.${tableName3} """
    hive_docker """ drop table if exists ${dbName}.${tableName4} """
    hive_docker """ drop table if exists ${dbName}.${tableName5} """
    hive_docker """ drop table if exists ${dbName}.${tableName6} """
    hive_docker """ drop table if exists ${dbName}.${tableName7} """
    hive_docker """ drop table if exists ${dbName}.${tableName8} """
    hive_docker """ drop table if exists ${dbName}.${tableName9} """
    hive_docker """ drop table if exists ${dbName}.${tableName10} """

    hive_docker """ drop database if exists ${dbName}"""
    hive_docker """ create database ${dbName}"""

    hive_docker """
        CREATE TABLE ${dbName}.${tableName1} (
          `user_id` INT,
          `num` INT
        ) 
        partitioned by(dt STRING ) 
        STORED AS ORC;
        """
    hive_docker """insert into ${dbName}.${tableName1} PARTITION(dt='2017-01-15') values (1,1)"""
    hive_docker """insert into ${dbName}.${tableName1} PARTITION(dt='2017-02-15') values (2,2)"""
    hive_docker """insert into ${dbName}.${tableName1} PARTITION(dt='2017-03-15') values (3,3)"""
    hive_docker """insert into ${dbName}.${tableName1} PARTITION(dt='2017-04-15') values (4,4)"""
    hive_docker """insert into ${dbName}.${tableName1} PARTITION(dt='2017-05-15') values (5,5)"""
    hive_docker """insert into ${dbName}.${tableName1} PARTITION(dt='2017-06-15') values (6,6)"""
    hive_docker """insert into ${dbName}.${tableName1} PARTITION(dt='2017-07-15') values (7,7)"""
    hive_docker """insert into ${dbName}.${tableName1} PARTITION(dt='2017-08-15') values (8,8)"""
    hive_docker """insert into ${dbName}.${tableName1} PARTITION(dt='2017-09-15') values (9,9)"""
    hive_docker """insert into ${dbName}.${tableName1} PARTITION(dt='2017-10-15') values (10,10)"""
    hive_docker """insert into ${dbName}.${tableName1} PARTITION(dt='2017-11-15') values (11,11)"""
    hive_docker """insert into ${dbName}.${tableName1} PARTITION(dt='2017-12-15') values (12,12)"""

    hive_docker """
        CREATE TABLE ${dbName}.${tableName2} (
          `user_id` INT,
          `num` INT
        ) 
        partitioned by(dt STRING ) 
        STORED AS ORC;
        """
    hive_docker """insert into ${dbName}.${tableName2} PARTITION(dt='2017-01-15') values (1,1)"""
    hive_docker """insert into ${dbName}.${tableName2} PARTITION(dt='2017-02-15') values (2,2)"""
    hive_docker """insert into ${dbName}.${tableName2} PARTITION(dt='2017-03-15') values (3,3)"""
    hive_docker """insert into ${dbName}.${tableName2} PARTITION(dt='2017-04-15') values (4,4)"""
    hive_docker """insert into ${dbName}.${tableName2} PARTITION(dt='2017-05-15') values (5,5)"""
    hive_docker """insert into ${dbName}.${tableName2} PARTITION(dt='2017-06-15') values (6,6)"""
    hive_docker """insert into ${dbName}.${tableName2} PARTITION(dt='2017-07-15') values (7,7)"""
    hive_docker """insert into ${dbName}.${tableName2} PARTITION(dt='2017-08-15') values (8,8)"""
    hive_docker """insert into ${dbName}.${tableName2} PARTITION(dt='2017-09-15') values (9,9)"""
    hive_docker """insert into ${dbName}.${tableName2} PARTITION(dt='2017-10-15') values (10,10)"""
    hive_docker """insert into ${dbName}.${tableName2} PARTITION(dt='2017-11-15') values (11,11)"""
    hive_docker """insert into ${dbName}.${tableName2} PARTITION(dt='2017-12-15') values (12,12)"""


    hive_docker """
        CREATE TABLE ${dbName}.${tableName3} (
          `user_id` INT,
          `num` INT
        ) 
        partitioned by(dt STRING ) 
        STORED AS ORC;
        """
    hive_docker """insert into ${dbName}.${tableName3} PARTITION(dt='2017-01-15') values (1,1)"""
    hive_docker """insert into ${dbName}.${tableName3} PARTITION(dt='2017-02-15') values (2,2)"""
    hive_docker """insert into ${dbName}.${tableName3} PARTITION(dt='2017-03-15') values (3,3)"""
    hive_docker """insert into ${dbName}.${tableName3} PARTITION(dt='2017-04-15') values (4,4)"""
    hive_docker """insert into ${dbName}.${tableName3} PARTITION(dt='2017-05-15') values (5,5)"""
    hive_docker """insert into ${dbName}.${tableName3} PARTITION(dt='2017-06-15') values (6,6)"""
    hive_docker """insert into ${dbName}.${tableName3} PARTITION(dt='2017-07-15') values (7,7)"""
    hive_docker """insert into ${dbName}.${tableName3} PARTITION(dt='2017-08-15') values (8,8)"""
    hive_docker """insert into ${dbName}.${tableName3} PARTITION(dt='2017-09-15') values (9,9)"""
    hive_docker """insert into ${dbName}.${tableName3} PARTITION(dt='2017-10-15') values (10,10)"""
    hive_docker """insert into ${dbName}.${tableName3} PARTITION(dt='2017-11-15') values (11,11)"""
    hive_docker """insert into ${dbName}.${tableName3} PARTITION(dt='2017-12-15') values (12,12)"""

    hive_docker """
        CREATE TABLE ${dbName}.${tableName4} (
          `user_id` INT ,
          `age` INT 
        ) 
        STORED AS ORC;
        """
    hive_docker """
        insert into ${dbName}.${tableName4} values (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10),(11,11),(12,12);
        """

    hive_docker """
        CREATE TABLE ${dbName}.${tableName5} (
          `user_id` INT,
          `num` INT
        ) 
        partitioned by(dt STRING ) 
        STORED AS ORC;
        """
    hive_docker """insert into ${dbName}.${tableName5} PARTITION(dt='2017-01-15') values (1,1)"""
    hive_docker """insert into ${dbName}.${tableName5} PARTITION(dt='2017-02-15') values (2,2)"""
    hive_docker """insert into ${dbName}.${tableName5} PARTITION(dt='2017-03-15') values (3,3)"""
    hive_docker """insert into ${dbName}.${tableName5} PARTITION(dt='2017-04-15') values (4,4)"""
    hive_docker """insert into ${dbName}.${tableName5} PARTITION(dt='2017-05-15') values (5,5)"""
    hive_docker """insert into ${dbName}.${tableName5} PARTITION(dt='2017-06-15') values (6,6)"""
    hive_docker """insert into ${dbName}.${tableName5} PARTITION(dt='2017-07-15') values (7,7)"""
    hive_docker """insert into ${dbName}.${tableName5} PARTITION(dt='2017-08-15') values (8,8)"""
    hive_docker """insert into ${dbName}.${tableName5} PARTITION(dt='2017-09-15') values (9,9)"""
    hive_docker """insert into ${dbName}.${tableName5} PARTITION(dt='2017-10-15') values (10,10)"""
    hive_docker """insert into ${dbName}.${tableName5} PARTITION(dt='2017-11-15') values (11,11)"""
    hive_docker """insert into ${dbName}.${tableName5} PARTITION(dt='2017-12-15') values (12,12)"""


    hive_docker """
        CREATE TABLE ${dbName}.${tableName6} (
          `user_id` INT,
          `num` INT
        ) 
        partitioned by(dt STRING ) 
        STORED AS ORC;
        """
    hive_docker """insert into ${dbName}.${tableName6} PARTITION(dt='2017-01-15') values (1,1)"""
    hive_docker """insert into ${dbName}.${tableName6} PARTITION(dt='2017-02-15') values (2,2)"""
    hive_docker """insert into ${dbName}.${tableName6} PARTITION(dt='2017-03-15') values (3,3)"""
    hive_docker """insert into ${dbName}.${tableName6} PARTITION(dt='2017-04-15') values (4,4)"""
    hive_docker """insert into ${dbName}.${tableName6} PARTITION(dt='2017-05-15') values (5,5)"""
    hive_docker """insert into ${dbName}.${tableName6} PARTITION(dt='2017-06-15') values (6,6)"""
    hive_docker """insert into ${dbName}.${tableName6} PARTITION(dt='2017-07-15') values (7,7)"""
    hive_docker """insert into ${dbName}.${tableName6} PARTITION(dt='2017-08-15') values (8,8)"""
    hive_docker """insert into ${dbName}.${tableName6} PARTITION(dt='2017-09-15') values (9,9)"""
    hive_docker """insert into ${dbName}.${tableName6} PARTITION(dt='2017-10-15') values (10,10)"""
    hive_docker """insert into ${dbName}.${tableName6} PARTITION(dt='2017-11-15') values (11,11)"""
    hive_docker """insert into ${dbName}.${tableName6} PARTITION(dt='2017-12-15') values (12,12)"""

    hive_docker """
        CREATE TABLE ${dbName}.${tableName7} (
          `user_id` INT ,
          `age` INT 
        ) 
        STORED AS ORC;
        """
    hive_docker """
        insert into ${dbName}.${tableName7} values (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10),(11,11),(12,12);
        """

    hive_docker """
        CREATE TABLE ${dbName}.${tableName8} (
          `user_id` INT ,
          `age` INT 
        ) 
        STORED AS ORC;
        """
    hive_docker """
        insert into ${dbName}.${tableName8} values (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10),(11,11),(12,12);
        """

    hive_docker """
        CREATE TABLE ${dbName}.${tableName9} (
          `user_id` INT,
          `num` INT
        ) 
        partitioned by(dt STRING ) 
        STORED AS ORC;
        """
    hive_docker """insert into ${dbName}.${tableName9} PARTITION(dt='2017-01-15') values (1,1)"""
    hive_docker """insert into ${dbName}.${tableName9} PARTITION(dt='2017-02-15') values (2,2)"""
    hive_docker """insert into ${dbName}.${tableName9} PARTITION(dt='2017-03-15') values (3,3)"""
    hive_docker """insert into ${dbName}.${tableName9} PARTITION(dt='2017-04-15') values (4,4)"""
    hive_docker """insert into ${dbName}.${tableName9} PARTITION(dt='2017-05-15') values (5,5)"""
    hive_docker """insert into ${dbName}.${tableName9} PARTITION(dt='2017-06-15') values (6,6)"""
    hive_docker """insert into ${dbName}.${tableName9} PARTITION(dt='2017-07-15') values (7,7)"""
    hive_docker """insert into ${dbName}.${tableName9} PARTITION(dt='2017-08-15') values (8,8)"""
    hive_docker """insert into ${dbName}.${tableName9} PARTITION(dt='2017-09-15') values (9,9)"""
    hive_docker """insert into ${dbName}.${tableName9} PARTITION(dt='2017-10-15') values (10,10)"""
    hive_docker """insert into ${dbName}.${tableName9} PARTITION(dt='2017-11-15') values (11,11)"""
    hive_docker """insert into ${dbName}.${tableName9} PARTITION(dt='2017-12-15') values (12,12)"""


    hive_docker """
        CREATE TABLE ${dbName}.${tableName10} (
          `user_id` INT ,
          `age` INT 
        ) 
        STORED AS ORC;
        """
    hive_docker """
        insert into ${dbName}.${tableName10} values (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10),(11,11),(12,12);
        """

    hive_docker """ set hive.stats.column.autogather = true; """

    sql """drop catalog if exists ${ctlName}"""
    sql """create catalog if not exists ${ctlName} properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://172.20.48.119:9383'
        );"""

    sql """use ${dbName}"""
    sql """drop materialized view if exists ${mtmvName1};"""
    sql """drop materialized view if exists ${mtmvName2};"""
    sql """drop materialized view if exists ${mtmvName3};"""
    sql """drop materialized view if exists ${mtmvName4};"""
    sql """drop materialized view if exists ${mtmvName5};"""
    sql """drop materialized view if exists ${mtmvName6};"""
    sql """
        CREATE MATERIALIZED VIEW ${mtmvName1}
            REFRESH AUTO ON MANUAL
            partition by(`dt`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT a.* FROM ${ctlName}.${dbName}.${tableName1} a inner join ${ctlName}.${dbName}.${tableName10} b on a.user_id=b.user_id;
        """
    waitingMTMVTaskFinishedByMvName(mtmvName1)

    sql """
        CREATE MATERIALIZED VIEW ${mtmvName2}
            REFRESH AUTO ON MANUAL
            partition by(`dt`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT a.* FROM ${ctlName}.${dbName}.${tableName2} a left join ${ctlName}.${dbName}.${tableName10} b on a.user_id=b.user_id;
        """
    waitingMTMVTaskFinishedByMvName(mtmvName2)

    sql """
        CREATE MATERIALIZED VIEW ${mtmvName3}
            REFRESH AUTO ON MANUAL
            partition by(`dt`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT a.* FROM ${ctlName}.${dbName}.${tableName3} a inner join ${ctlName}.${dbName}.${tableName10} b on a.user_id=b.user_id;
        """
    waitingMTMVTaskFinishedByMvName(mtmvName3)

    sql """
        CREATE MATERIALIZED VIEW ${mtmvName4}
            REFRESH AUTO ON MANUAL
            partition by(`dt`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT a.* FROM ${ctlName}.${dbName}.${tableName9} a inner join ${ctlName}.${dbName}.${tableName4} b on a.user_id=b.user_id;
        """
    waitingMTMVTaskFinishedByMvName(mtmvName4)

    sql """
        CREATE MATERIALIZED VIEW ${mtmvName5}
            REFRESH AUTO ON MANUAL
            partition by(`dt`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT a.* FROM ${ctlName}.${dbName}.${tableName5} a inner join ${ctlName}.${dbName}.${tableName8} b on a.user_id=b.user_id;
        """
    waitingMTMVTaskFinishedByMvName(mtmvName5)

    sql """
        CREATE MATERIALIZED VIEW ${mtmvName6}
            REFRESH AUTO ON MANUAL
            partition by(`dt`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT a.* FROM ${ctlName}.${dbName}.${tableName6} a inner join ${ctlName}.${dbName}.${tableName7} b on a.user_id=b.user_id;
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
