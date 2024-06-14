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

suite("test_row_column_mtmv","mtmv") {
    String suiteName = "test_row_column_mtmv"
    String tableName = "${suiteName}_table"
    String mvName = "${suiteName}_mv"
    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE TABLE ${tableName}
        (
            k1 TINYINT,
            k2 INT not null
        )
        COMMENT "my first table"
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
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1',
        'store_row_column' = 'true'
        )
        AS
        SELECT * from ${tableName};
        """

     sql """
        REFRESH MATERIALIZED VIEW ${mvName} AUTO
        """
    waitingMTMVTaskFinishedByMvName(mvName)

    order_qt_k1 "SELECT * FROM ${mvName} order by k1 limit 1"
    order_qt_k2 "SELECT * FROM ${mvName} order by k2 desc limit 1"
    order_qt_k1_k2 "SELECT * FROM ${mvName} order by k1,k2 limit 1"
    order_qt_k2_k1 "SELECT * FROM ${mvName} order by k2,k1 limit 1"

    sql """SET show_hidden_columns=true;"""
    def colRes = sql """desc ${mvName};"""
    logger.info("colRes: " + colRes.toString())
    assertTrue(colRes.toString().contains("__DORIS_ROW_STORE_COL__"))

    sql """SET show_hidden_columns=false;"""
    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""
}
