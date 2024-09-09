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

suite("test_enable_date_non_deterministic_function_mtmv","mtmv") {
    String suiteName = "test_enable_date_non_deterministic_function_mtmv"
    String tableName = "${suiteName}_table"
    String mvName = "${suiteName}_mv"
    String db = context.config.getDbNameByFile(context.file)
    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE TABLE ${tableName}
        (
            k1 TINYINT,
            k2 INT not null,
            k3 DATE NOT NULL
        )
        COMMENT "my first table"
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """
     sql """
        insert into ${tableName} values(1,1, '2024-05-01'),(2,2, '2024-05-02'),(3,3, '2024-05-03');
        """

    // when not enable date nondeterministic function, create mv should fail
    // because contains uuid, unix_timestamp, current_date
    sql """drop materialized view if exists ${mvName};"""
    try {
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
                BUILD DEFERRED REFRESH AUTO ON MANUAL
                DISTRIBUTED BY RANDOM BUCKETS 2
                PROPERTIES ('replication_num' = '1')
                AS
                SELECT uuid(), unix_timestamp() FROM ${tableName} where current_date() > k3;
        """
        Assert.fail();
    } catch (Exception e) {
        logger.info(e.getMessage())
        assertTrue(e.getMessage().contains("can not contain nonDeterministic expression"));
    }
    sql """drop materialized view if exists ${mvName};"""


    // when not enable date nondeterministic function, create mv should fail
    try {
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
                BUILD DEFERRED REFRESH AUTO ON MANUAL
                DISTRIBUTED BY RANDOM BUCKETS 2
                PROPERTIES ('replication_num' = '1')
                AS
                SELECT * FROM ${tableName} where current_date() > k3;
        """
        Assert.fail();
    } catch (Exception e) {
        logger.info(e.getMessage())
        assertTrue(e.getMessage().contains("can not contain nonDeterministic expression"));
    }
    sql """drop materialized view if exists ${mvName};"""

    // when enable date nondeterministic function, create mv with current_date should success
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1',
        'enable_nondeterministic_function' = 'true'
        )
        AS
        SELECT *, unix_timestamp(k3, '%Y-%m-%d %H:%i-%s') from ${tableName} where current_date() > k3;
        """

    waitingMTMVTaskFinished(getJobName(db, mvName))
    order_qt_with_current_date """select * from ${mvName}"""
    sql """drop materialized view if exists ${mvName};"""


    sql """drop materialized view if exists ${mvName};"""

    // when disable date nondeterministic function, create mv with param unix_timestamp should success
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        SELECT *, unix_timestamp(k3, '%Y-%m-%d %H:%i-%s') from ${tableName};
        """

    waitingMTMVTaskFinished(getJobName(db, mvName))
    order_qt_with_unix_timestamp_format """select * from ${mvName}"""
    sql """drop materialized view if exists ${mvName};"""

    // when enable date nondeterministic function, create mv with orther fuction except current_date() should fail
    try {
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
                BUILD DEFERRED REFRESH AUTO ON MANUAL
                DISTRIBUTED BY RANDOM BUCKETS 2
                PROPERTIES ('replication_num' = '1')
                AS
                SELECT * FROM ${tableName} where now() > k3 and current_time() > k3;
        """
        Assert.fail();
    } catch (Exception e) {
        logger.info(e.getMessage())
        assertTrue(e.getMessage().contains("can not contain nonDeterministic expression"));
    }

    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""
}
