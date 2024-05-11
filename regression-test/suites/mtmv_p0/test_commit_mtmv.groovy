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

suite("test_commit_mtmv") {
    def tableName = "test_commit_mtmv_table"
    def mvName1 = "test_commit_mtmv1"
    def mvName2 = "test_commit_mtmv2"
    def dbName = "regression_test_mtmv_p0"
    sql """drop materialized view if exists ${mvName1};"""
    sql """drop materialized view if exists ${mvName2};"""
    sql """drop table if exists `${tableName}`"""
    sql """
        CREATE TABLE IF NOT EXISTS `${tableName}` (
          `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `date` DATE NOT NULL COMMENT '\"数据灌入日期时间\"',
          `num` SMALLINT NOT NULL COMMENT '\"数量\"'
        ) ENGINE=OLAP
        DUPLICATE KEY(`user_id`, `date`, `num`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """

    sql """
        CREATE MATERIALIZED VIEW ${mvName1}
        BUILD DEFERRED REFRESH AUTO ON COMMIT
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
        AS 
        SELECT * FROM ${tableName};
    """
     sql """
         CREATE MATERIALIZED VIEW ${mvName2}
         BUILD DEFERRED REFRESH AUTO ON COMMIT
         DISTRIBUTED BY RANDOM BUCKETS 2
         PROPERTIES ('replication_num' = '1')
         AS
         SELECT * FROM ${mvName1};
     """
      sql """
         insert into ${tableName} values(1,"2017-01-15",1),(2,"2017-02-15",2),(3,"2017-03-15",3);;
     """
    def jobName1 = getJobName(dbName, mvName1);
    waitingMTMVTaskFinished(jobName1)
    order_qt_mv1 "SELECT * FROM ${mvName1}"
    order_qt_task1 "SELECT TaskContext from tasks('type'='mv') where MvName='${mvName1}' order by CreateTime desc limit 1"

    def jobName2 = getJobName(dbName, mvName2);
    waitingMTMVTaskFinished(jobName2)
    order_qt_mv2 "SELECT * FROM ${mvName2}"
    order_qt_task2 "SELECT TaskContext from tasks('type'='mv') where MvName='${mvName2}' order by CreateTime desc limit 1"

    // on manual can not trigger by commit
    sql """
            alter MATERIALIZED VIEW ${mvName2} REFRESH ON MANUAL;
        """

     sql """
          insert into ${tableName} values(1,"2017-01-15",1);;
      """
    waitingMTMVTaskFinished(jobName1)
    order_qt_mv1_2 "SELECT * FROM ${mvName1}"
    waitingMTMVTaskFinished(jobName2)
    order_qt_mv2_2 "SELECT * FROM ${mvName2}"

    sql """drop materialized view if exists ${mvName1};"""
    sql """drop materialized view if exists ${mvName2};"""
    sql """drop table if exists `${tableName}`"""

    // test drop partition
    sql """
        CREATE TABLE IF NOT EXISTS `${tableName}` (
          `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `date` DATE NOT NULL COMMENT '\"数据灌入日期时间\"',
          `num` SMALLINT NOT NULL COMMENT '\"数量\"'
        ) ENGINE=OLAP
        DUPLICATE KEY(`user_id`, `date`, `num`)
        COMMENT 'OLAP'
        PARTITION BY RANGE(`date`)
        (PARTITION p201701 VALUES [('0000-01-01'), ('2017-02-01')),
        PARTITION p201702 VALUES [('2017-02-01'), ('2017-03-01')),
        PARTITION p201703 VALUES [('2017-03-01'), ('2017-04-01')))
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
    """
    sql """
            CREATE MATERIALIZED VIEW ${mvName1}
            BUILD DEFERRED REFRESH AUTO ON COMMIT
            PARTITION BY (`date`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT * FROM ${tableName};
        """
     sql """
          insert into ${tableName} values(1,"2017-01-15",1),(2,"2017-02-15",2),(3,"2017-03-15",3);;
      """
     jobName1 = getJobName(dbName, mvName1);
     waitingMTMVTaskFinished(jobName1)
     order_qt_mv1_init "SELECT * FROM ${mvName1}"

     sql """alter table ${tableName} drop PARTITION p201701"""
     waitingMTMVTaskFinished(jobName1)
     order_qt_mv1_drop "SELECT * FROM ${mvName1}"

    // test replace partition
    sql """ALTER TABLE ${tableName} ADD TEMPORARY PARTITION p201702_t VALUES [('2017-02-01'), ('2017-03-01'));"""
    sql """ALTER TABLE ${tableName} REPLACE PARTITION (p201702) WITH TEMPORARY PARTITION (p201702_t);"""
    waitingMTMVTaskFinished(jobName1)
    order_qt_mv1_replace "SELECT * FROM ${mvName1}"

    sql """drop materialized view if exists ${mvName1};"""
    sql """drop materialized view if exists ${mvName2};"""
    sql """drop table if exists `${tableName}`"""

}
