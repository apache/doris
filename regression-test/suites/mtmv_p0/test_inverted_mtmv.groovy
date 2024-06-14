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

suite("test_inverted_mtmv","mtmv") {
    String suiteName = "test_inverted_mtmv"
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
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1',
        'inverted_index_storage_format'='V2'
        )
        AS
        SELECT * from ${tableName};
        """
    // add index
    sql """
        CREATE INDEX idx1 ON ${mvName} (k3) USING INVERTED;
        """
    assertEquals("FINISHED", getAlterColumnFinalState("${mvName}"))
    def showIndexResult = sql """show index from ${mvName};"""
    logger.info("showIndexResult: " + showIndexResult.toString())
    assertTrue(showIndexResult.toString().contains('idx1'))

    // refresh mv
    sql """
        insert into ${tableName} values(1,1),(2,2),(3,3);
        """
     sql """
        REFRESH MATERIALIZED VIEW ${mvName} complete
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_refresh_mv "SELECT * FROM ${mvName}"

    // drop index
    sql """
        DROP INDEX idx1 ON ${mvName};
        """
    assertEquals("FINISHED", getAlterColumnFinalState("${mvName}"))
    showIndexResult = sql """show index from ${mvName};"""
    logger.info("showIndexResult: " + showIndexResult.toString())
    assertFalse(showIndexResult.toString().contains('idx1'))

    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""
}
