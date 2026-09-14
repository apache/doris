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

suite("test_excluded_trigger_table_mtmv","mtmv") {
    String suiteName = "test_excluded_trigger_table_mtmv"
    String dbName = context.config.getDbNameByFile(context.file)
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

    sql """
        insert into ${tableName} values(1,1);
        """
     sql """
        REFRESH MATERIALIZED VIEW ${mvName} AUTO
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_init "SELECT * FROM ${mvName}"
    sql """
        insert into ${tableName} values(2,2);
        """
    sql """
            alter Materialized View ${mvName} set("excluded_trigger_tables"="${tableName}");
        """
    sql """
        REFRESH MATERIALIZED VIEW ${mvName} AUTO
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    // should refresh because excluded_trigger_tables changed and refresh baseline should be rebuilt
    order_qt_true_table "SELECT * FROM ${mvName}"

    sql """
            insert into ${tableName} values(3,3);
            """
    sql """
            alter Materialized View ${mvName} set("excluded_trigger_tables"="${dbName}.${tableName}");
        """
     sql """
         REFRESH MATERIALIZED VIEW ${mvName} AUTO
         """
    waitingMTMVTaskFinishedByMvName(mvName)
     // should refresh because excluded_trigger_tables changed and refresh baseline should be rebuilt
    order_qt_true_db_table "SELECT * FROM ${mvName}"

    sql """
            insert into ${tableName} values(4,4);
            """
    sql """
            alter Materialized View ${mvName} set("excluded_trigger_tables"="internal.${dbName}.${tableName}");
        """
     sql """
         REFRESH MATERIALIZED VIEW ${mvName} AUTO
         """
    waitingMTMVTaskFinishedByMvName(mvName)
     // should refresh because excluded_trigger_tables changed and refresh baseline should be rebuilt
    order_qt_true_ctl_db_table "SELECT * FROM ${mvName}"

    sql """
            insert into ${tableName} values(5,5);
            """
    sql """
            alter Materialized View ${mvName} set("excluded_trigger_tables"="internal1.${dbName}.${tableName}");
        """
     sql """
         REFRESH MATERIALIZED VIEW ${mvName} AUTO
         """
    waitingMTMVTaskFinishedByMvName(mvName)
     // should refresh
    order_qt_false_ctl_db_table "SELECT * FROM ${mvName}"

     sql """
            insert into ${tableName} values(6,6);
            """
    sql """
            alter Materialized View ${mvName} set("excluded_trigger_tables"="${dbName}1.${tableName}");
        """
     sql """
         REFRESH MATERIALIZED VIEW ${mvName} AUTO
         """
    waitingMTMVTaskFinishedByMvName(mvName)
     // should refresh
    order_qt_false_db_table "SELECT * FROM ${mvName}"

    sql """
            insert into ${tableName} values(7,7);
            """
    sql """
            alter Materialized View ${mvName} set("excluded_trigger_tables"="${tableName}1");
        """
     sql """
         REFRESH MATERIALIZED VIEW ${mvName} AUTO
         """
    waitingMTMVTaskFinishedByMvName(mvName)
     // should refresh
    order_qt_false_table "SELECT * FROM ${mvName}"

    sql """drop materialized view if exists test_excluded_trigger_table_mtmv_partition_mv;"""
    sql """drop table if exists test_excluded_trigger_table_mtmv_partition_num;"""
    sql """drop table if exists test_excluded_trigger_table_mtmv_partition_user;"""

    sql """
        CREATE TABLE test_excluded_trigger_table_mtmv_partition_num (
            user_id LARGEINT,
            date DATE,
            num INT
        )
        DUPLICATE KEY(user_id)
        PARTITION BY RANGE(date) (
            PARTITION p201701 VALUES [('2017-01-01'), ('2017-02-01')),
            PARTITION p201702 VALUES [('2017-02-01'), ('2017-03-01'))
        )
        DISTRIBUTED BY HASH(user_id) BUCKETS 2
        PROPERTIES ('replication_num' = '1');
    """
    sql """
        INSERT INTO test_excluded_trigger_table_mtmv_partition_num VALUES
            (1, '2017-01-15', 1),
            (1, '2017-02-15', 2);
    """
    sql """
        CREATE TABLE test_excluded_trigger_table_mtmv_partition_user (
            user_id LARGEINT,
            age INT
        )
        DUPLICATE KEY(user_id)
        DISTRIBUTED BY HASH(user_id) BUCKETS 2
        PROPERTIES ('replication_num' = '1');
    """
    sql """INSERT INTO test_excluded_trigger_table_mtmv_partition_user VALUES (1, 10);"""

    sql """
        CREATE MATERIALIZED VIEW test_excluded_trigger_table_mtmv_partition_mv
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        PARTITION BY(date)
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1',
            'excluded_trigger_tables' = 'test_excluded_trigger_table_mtmv_partition_user'
        )
        AS
        SELECT
            test_excluded_trigger_table_mtmv_partition_user.user_id,
            test_excluded_trigger_table_mtmv_partition_user.age,
            test_excluded_trigger_table_mtmv_partition_num.date,
            test_excluded_trigger_table_mtmv_partition_num.num
        FROM test_excluded_trigger_table_mtmv_partition_user
        JOIN test_excluded_trigger_table_mtmv_partition_num
          ON test_excluded_trigger_table_mtmv_partition_user.user_id
             = test_excluded_trigger_table_mtmv_partition_num.user_id;
    """

    sql """REFRESH MATERIALIZED VIEW test_excluded_trigger_table_mtmv_partition_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_excluded_trigger_table_mtmv_partition_mv")
    order_qt_partition_exclude_init """
        SELECT * FROM test_excluded_trigger_table_mtmv_partition_mv ORDER BY user_id, age, date, num
    """

    sql """INSERT INTO test_excluded_trigger_table_mtmv_partition_user VALUES (1, 9);"""
    sql """REFRESH MATERIALIZED VIEW test_excluded_trigger_table_mtmv_partition_mv PARTITIONS"""
    waitingMTMVTaskFinishedByMvName("test_excluded_trigger_table_mtmv_partition_mv")
    order_qt_partition_exclude_not_change """
        SELECT * FROM test_excluded_trigger_table_mtmv_partition_mv ORDER BY user_id, age, date, num
    """

    sql """REFRESH MATERIALIZED VIEW test_excluded_trigger_table_mtmv_partition_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_excluded_trigger_table_mtmv_partition_mv")
    order_qt_partition_exclude_complete """
        SELECT * FROM test_excluded_trigger_table_mtmv_partition_mv ORDER BY user_id, age, date, num
    """
}
