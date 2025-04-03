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

suite("test_iot_recycle_mtmv","mtmv") {
    String suiteName = "test_iot_recycle_mtmv"
    String tableName = "${suiteName}_table"
    String mvName = "${suiteName}_mv"
    String value = UUID.randomUUID().toString().replaceAll("-", "")

    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""
    sql """
        CREATE TABLE ${tableName}
        (
            k2 BIGINT,
            k3 VARCHAR(100)
        )
        PARTITION BY LIST(`k3`)
        (
            PARTITION p1 VALUES IN ('${value}')
        )
        DISTRIBUTED BY HASH(k3) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """
     sql """
        insert into ${tableName} values(1,"${value}");
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
    def showPartitionsResult = sql """show partitions from ${mvName}"""
    logger.info("showPartitionsResult: " + showPartitionsResult.toString())
    assertTrue(showPartitionsResult.toString().contains("p_${value}"))
    def recycleResultBefore = sql """SHOW CATALOG RECYCLE BIN where name ='p_${value}';"""
    assertEquals(0, recycleResultBefore.size());
    sql """
        REFRESH MATERIALIZED VIEW ${mvName} AUTO
    """
    waitingMTMVTaskFinishedByMvName(mvName)
    def recycleResultAfter = sql """SHOW CATALOG RECYCLE BIN where name ='p_${value}';"""
    assertEquals(0, recycleResultAfter.size());
}
