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

import org.junit.Assert;

suite("test_partition_version_mtmv","mtmv") {
    String suiteName = "test_partition_version_mtmv"
    String dbName = context.config.getDbNameByFile(context.file)
    String tableName = "${suiteName}_table"
    String mvName = "${suiteName}_mv"
    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""


    // default
    sql """
        CREATE TABLE ${tableName}
        (
            k2 TINYINT,
            k3 INT not null
        )
        COMMENT "my first table"
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """
    sql """
        insert into ${tableName} values(1,1);
        """
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        SELECT * from ${tableName};
        """
    order_qt_default_init_version "select VisibleVersion from partitions('catalog'='internal','database'='${dbName}','table'='${mvName}');";

     sql """
        REFRESH MATERIALIZED VIEW ${mvName} COMPLETE;
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_default_first_version "select VisibleVersion from partitions('catalog'='internal','database'='${dbName}','table'='${mvName}');";

     sql """
        REFRESH MATERIALIZED VIEW ${mvName} COMPLETE;
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_default_second_version "select VisibleVersion from partitions('catalog'='internal','database'='${dbName}','table'='${mvName}');";

    sql """
        REFRESH MATERIALIZED VIEW ${mvName} COMPLETE;
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_default_third_version "select VisibleVersion from partitions('catalog'='internal','database'='${dbName}','table'='${mvName}');";

    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""


    // list
    sql """
        CREATE TABLE ${tableName}
        (
            k2 TINYINT,
            k3 INT not null
        )
        COMMENT "my first table"
        PARTITION BY LIST(`k3`)
        (
            PARTITION `p1` VALUES IN ('1')
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """
    sql """
        insert into ${tableName} values(1,1);
        """
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        partition by(`k3`)
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        SELECT * from ${tableName};
        """
    order_qt_list_init_version "select VisibleVersion from partitions('catalog'='internal','database'='${dbName}','table'='${mvName}');";

     sql """
        REFRESH MATERIALIZED VIEW ${mvName} COMPLETE;
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_list_first_version "select VisibleVersion from partitions('catalog'='internal','database'='${dbName}','table'='${mvName}');";

     sql """
        REFRESH MATERIALIZED VIEW ${mvName} COMPLETE;
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_list_second_version "select VisibleVersion from partitions('catalog'='internal','database'='${dbName}','table'='${mvName}');";

    sql """
        REFRESH MATERIALIZED VIEW ${mvName} COMPLETE;
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_list_third_version "select VisibleVersion from partitions('catalog'='internal','database'='${dbName}','table'='${mvName}');";

    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""

     // range
    sql """
        CREATE TABLE `${tableName}` (
           `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
           `date` DATE NOT NULL COMMENT '\"数据灌入日期时间\"',
           `num` SMALLINT NOT NULL COMMENT '\"数量\"'
         ) ENGINE=OLAP
         DUPLICATE KEY(`user_id`, `date`, `num`)
         COMMENT 'OLAP'
         PARTITION BY RANGE(`date`)
         (PARTITION p201701_1000 VALUES [('0000-01-01'), ('2017-02-01'))
         DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
         PROPERTIES ('replication_num' = '1') ;
        """
    sql """
        insert into ${tableName} values(1,"2017-01-15",1);
        """
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        partition by(`date`)
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        SELECT * from ${tableName};
        """
    order_qt_range_init_version "select VisibleVersion from partitions('catalog'='internal','database'='${dbName}','table'='${mvName}');";

     sql """
        REFRESH MATERIALIZED VIEW ${mvName} COMPLETE;
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_range_first_version "select VisibleVersion from partitions('catalog'='internal','database'='${dbName}','table'='${mvName}');";

     sql """
        REFRESH MATERIALIZED VIEW ${mvName} COMPLETE;
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_range_second_version "select VisibleVersion from partitions('catalog'='internal','database'='${dbName}','table'='${mvName}');";

    sql """
        REFRESH MATERIALIZED VIEW ${mvName} COMPLETE;
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_range_third_version "select VisibleVersion from partitions('catalog'='internal','database'='${dbName}','table'='${mvName}');";

    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""
}
