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

suite("test_storage_medium_mtmv","mtmv") {
    String suiteName = "test_storage_medium_mtmv"
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
        COMMENT "my first table"
        PARTITION BY LIST(`k3`)
        (
            PARTITION `p1` VALUES IN ('1'),
            PARTITION `p2` VALUES IN ('2'),
            PARTITION `p3` VALUES IN ('3')
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
        'replication_num' = '1',
        'storage_medium' = 'SSD'
        )
        AS
        SELECT * from ${tableName};
        """

    // test init
    def res = sql """show partitions from ${mvName}"""
    logger.info("res: " + res.toString())
    if (!isCloudMode()) {
        assertTrue(res.toString().contains("SSD"))
        assertFalse(res.toString().contains("HDD"))
    }

    sql """
        insert into ${tableName} values(1,1),(2,2),(3,3);
        """
     sql """
        REFRESH MATERIALIZED VIEW ${mvName} AUTO
        """
    waitingMTMVTaskFinishedByMvName(mvName)

    // test after refresh
    res = sql """show partitions from ${mvName}"""
    logger.info("res: " + res.toString())
    assertTrue(res.toString().contains("SSD"))
    assertFalse(res.toString().contains("HDD"))

    if (!isCloudMode()) {
        sql """drop table if exists `${tableName}`"""
        sql """drop materialized view if exists ${mvName};"""
    }
}
