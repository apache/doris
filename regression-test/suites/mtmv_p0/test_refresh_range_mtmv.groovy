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

suite("test_refresh_range_mtmv","mtmv") {
    String suiteName = "test_refresh_range_mtmv"
    String tableName = "${suiteName}_table"
    String mvName = "${suiteName}_mv"
    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE TABLE ${tableName}
        (
            k2 TINYINT,
            k3 INT not null
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
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

    // test self manage
    test {
         sql """
            REFRESH MATERIALIZED VIEW ${mvName} partition start '2017-02-01' end '2017-02-02';
            """
         exception "SELF_MANAGE"
    }

    // test illegal date type
    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE TABLE ${tableName}
        (
            k2 TINYINT,
            k3 INT not null
        )
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

     sql """
        REFRESH MATERIALIZED VIEW ${mvName} partition start '2017-02-01' end '2017-02-02';
        """
     waitingMTMVTaskFinishedByMvNameNotNeedSuccess(mvName)
     order_qt_int_error "select Status,ErrorMsg from tasks('type'='mv') where MvName = '${mvName}' order by CreateTime DESC limit 1"

    test {
         sql """
            REFRESH MATERIALIZED VIEW ${mvName} partition start '2' end '3';
            """
         exception "cannot convert to date type"
    }

    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""

    // test range partition
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
        insert into ${tableName} values(1,"2017-01-15",1),(1,"2017-02-15",2),(1,"2017-03-15",3);
        """

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by(`date`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT * FROM ${tableName};
    """

    // test start = end
    test {
         sql """
            REFRESH MATERIALIZED VIEW ${mvName} partition start '2017-02-01' end '2017-02-01';
            """
         exception "should be greater than"
    }

    // test refresh 0201
    sql """
        REFRESH MATERIALIZED VIEW ${mvName} partition start '2017-02-01' end '2017-02-02';
        """
     waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_0201 "select * from ${mvName}"


    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""


    // test list string partition
     sql """
        CREATE TABLE `${tableName}` (
          `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `date` varchar(100) NOT NULL COMMENT '\"数据灌入日期时间\"',
          `num` SMALLINT NOT NULL COMMENT '\"数量\"'
        ) ENGINE=OLAP
        COMMENT 'OLAP'
        PARTITION BY list(`date`)
        (
        PARTITION p_20170101 VALUES IN ("2017-01-01"),
        PARTITION p_20170102_20170103 VALUES IN ("2017-01-02","2017-01-03")
        )
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """
    sql """
        insert into ${tableName} values(1,"2017-01-01",1),(1,"2017-01-02",2),(1,"2017-01-03",3);
        """

     sql """
        CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            partition by(`date`)
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT * FROM ${tableName};
    """

    // test refresh 0101
    sql """
        REFRESH MATERIALIZED VIEW ${mvName} partition start '2017-01-01' end '2017-01-02';
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_0101 "select * from ${mvName}"

    // test refresh 0102
    sql """
        REFRESH MATERIALIZED VIEW ${mvName} partition start '2017-01-02' end '2017-01-03';
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_0102 "select * from ${mvName}"

    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""
}
