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

suite("test_start_time_mtmv","mtmv") {
    String suiteName = "test_start_time_mtmv"
    String tableName = "${suiteName}_table"
    String mvName = "${suiteName}_mv"

    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE TABLE ${tableName}
        (
            k2 INT,
            k3 varchar(32)
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """

    sql """
        insert into ${tableName} values(1,1),(2,2),(3,3);
        """

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD immediate REFRESH AUTO ON SCHEDULE EVERY 2 HOUR STARTS "9999-12-13 21:07:09"
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        SELECT * from ${tableName};
        """

    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_init "SELECT * FROM ${mvName}"

    order_qt_create "select RecurringStrategy from jobs('type'='mv') where MvName='${mvName}'"

    sql """
       alter MATERIALIZED VIEW ${mvName} REFRESH auto ON SCHEDULE EVERY 2 HOUR STARTS "9998-12-13 21:07:09";
        """

    order_qt_alter "select RecurringStrategy from jobs('type'='mv') where MvName='${mvName}'"

   // refresh mv
    sql """
       REFRESH MATERIALIZED VIEW ${mvName} complete
       """

    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_refresh "SELECT * FROM ${mvName}"

    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""
}
