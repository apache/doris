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

suite("test_recover_mtmv") {
    def tableName = "t_test_recover_mtmv_user"
    def mvName = "test_recover_mtmv"
    def dbName = "regression_test_mtmv_p0"
    def dbName1 = "test_recover_mtmv_db";
    sql """drop table if exists `${tableName}`"""
    sql """drop database if exists `${dbName1}`"""
    sql """
        CREATE TABLE IF NOT EXISTS `${tableName}` (
            event_day DATE,
            id BIGINT,
            username VARCHAR(20)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 10 
        PROPERTIES (
            "replication_num" = "1"
        );
        """
    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS 
        SELECT * FROM ${tableName};
    """

    // test recover partition of MTMV
    try {
        sql """
            RECOVER PARTITION p1 FROM ${mvName};
        """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

    // test recover mv
     sql """
            DROP MATERIALIZED VIEW ${mvName}
        """
    try {
        sql """
            recover table ${mvName};
        """
        Assert.fail();
    } catch (Exception e) {
        log.info(e.getMessage())
    }

    sql """create database `${dbName1}`"""

    sql """
        CREATE MATERIALIZED VIEW ${dbName1}.${mvName}
        BUILD DEFERRED REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT * FROM ${tableName};
    """

    sql """drop database if exists `${dbName1}`"""

    sql """recover database ${dbName1};"""

    def result =  sql """show tables from ${dbName1};"""
    log.info(result.toString())
    assertEquals(0, result.size())

    sql """drop database if exists `${dbName1}`"""
    sql """drop table if exists `${tableName}`"""
}
