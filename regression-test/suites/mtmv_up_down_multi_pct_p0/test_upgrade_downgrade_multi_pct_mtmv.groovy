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

suite("test_upgrade_downgrade_multi_pct_mtmv","p0,mtmv,restart_fe") {
    String suiteName = "mtmv_up_down_multi_pct"
    String dbName = context.config.getDbNameByFile(context.file)
    String mvName = "${suiteName}_mtmv"
    String tableName = "${suiteName}_table"
    String tableName2 = "${suiteName}_table2"
    // test data is normal
    order_qt_refresh_init "SELECT * FROM ${mvName}"
    // test is sync
    order_qt_mtmv_sync "select SyncWithBaseTables from mv_infos('database'='${dbName}') where Name='${mvName}'"
     sql """
            REFRESH MATERIALIZED VIEW ${mvName} complete
        """
    // test can refresh success
    waitingMTMVTaskFinishedByMvName(mvName)

    // test schema change
    sql """drop table if exists `${tableName}`"""
    sql """
        CREATE TABLE `${tableName}` (
          `date` DATE NOT NULL COMMENT '\"数据灌入日期时间\"',
          `num` SMALLINT NOT NULL COMMENT '\"数量\"'
        ) ENGINE=OLAP
        DUPLICATE KEY(`date`, `num`)
        COMMENT 'OLAP'
        PARTITION BY RANGE(`date`)
        (
        PARTITION p201701 VALUES [('2017-01-01'), ('2017-02-01')),
        PARTITION p201702 VALUES [('2017-02-01'), ('2017-03-01'))
        )
        DISTRIBUTED BY HASH(`date`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """
    sql """
        insert into ${tableName} values("2017-01-01",1),("2017-02-01",2);
        """

     sql """
         REFRESH MATERIALIZED VIEW ${mvName} auto
     """
     // test can refresh success
     waitingMTMVTaskFinishedByMvName(mvName)
     order_qt_refresh_sc "SELECT * FROM ${mvName}"
}

