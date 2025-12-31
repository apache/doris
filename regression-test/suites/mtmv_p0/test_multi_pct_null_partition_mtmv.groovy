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

suite("test_multi_pct_null_partition_mtmv","mtmv") {
    String suiteName = "test_multi_pct_null_partition_mtmv"
    String dbName = context.config.getDbNameByFile(context.file)
    String tableName1 = "${suiteName}_table1"
    String tableName2 = "${suiteName}_table2"
    String mvName = "${suiteName}_mv"
    sql """drop table if exists `${tableName1}`"""
    sql """drop table if exists `${tableName2}`"""
    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE TABLE ${tableName1}
        (
            k1 INT,
            k2 INT
        )
        PARTITION BY RANGE(`k1`)
        (
            PARTITION p20 values less than (20),
            PARTITION p100 values less than (100),
            PARTITION pMAX values less than (maxvalue)
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """

    sql """
        CREATE TABLE ${tableName2}
        (
            k1 INT,
            k2 INT
        )
        PARTITION BY RANGE(`k1`)
        (
            PARTITION p10 values less than (10),
            PARTITION p100 values less than (100),
            PARTITION pMAX values less than (maxvalue)
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """

    String mvSql = "SELECT * from ${tableName1} t1 union all select * from ${tableName2} t2;";
    test {
         sql """
             CREATE MATERIALIZED VIEW ${mvName}
                     BUILD DEFERRED REFRESH AUTO ON MANUAL
                     partition by(k1)
                     DISTRIBUTED BY RANDOM BUCKETS 2
                     PROPERTIES (
                     'replication_num' = '1'
                     )
                     AS
                     ${mvSql}
             """
         exception "intersected"
     }

     sql """drop table if exists `${tableName1}`"""

    sql """
        CREATE TABLE ${tableName1}
        (
            k1 INT,
            k2 INT
        )
        PARTITION BY RANGE(`k1`)
        (
            PARTITION p10 values less than (10),
            PARTITION p100 values less than (100),
            PARTITION pMAX values less than (maxvalue)
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """
    sql """
         CREATE MATERIALIZED VIEW ${mvName}
                 BUILD DEFERRED REFRESH AUTO ON MANUAL
                 partition by(k1)
                 DISTRIBUTED BY RANDOM BUCKETS 2
                 PROPERTIES (
                 'replication_num' = '1'
                 )
                 AS
                 ${mvSql}
         """

    sql """
        insert into ${tableName1} values(null,1);
        """
    sql """
        REFRESH MATERIALIZED VIEW ${mvName} partitions(p_2147483648_10);
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_select "select * from ${mvName}"
}
