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

suite("test_set_partition_version_mtmv","mtmv") {
    // cloud not support set partition version
    if (isCloudMode()) {
        return;
    }

    String suiteName = "test_set_partition_version_mtmv"
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
        'replication_num' = '1'
        )
        AS
        SELECT * from ${tableName};
        """

    def res = sql """ show partitions from ${mvName}; """
    logger.info("res: " + res.toString())
    def partitionId = res[0][0].toString()

    // set partition visible version to 20
    sql """ ADMIN SET TABLE ${mvName} PARTITION VERSION PROPERTIES ("partition_id" = "${partitionId}", "visible_version" = "20"); """
    res = sql """ show partitions from ${mvName}; """
    logger.info("res: " + res.toString())
    assertEquals(res[0][2].toString(), "20")

    sql """
        insert into ${tableName} values(1,1),(2,2),(3,3);
        """

     // refresh mv
     sql """
        REFRESH MATERIALIZED VIEW ${mvName} complete
        """
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_refresh_mv "SELECT * FROM ${mvName}"

    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""
}
