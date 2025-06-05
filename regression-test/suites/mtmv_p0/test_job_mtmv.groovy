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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.Instant;
import java.time.ZoneId;
import org.junit.Assert;

suite("test_job_mtmv","mtmv") {
    String suiteName = "test_job_mtmv"
    String dbName = context.config.getDbNameByFile(context.file)
    String tableName = "${suiteName}_table"
    String mvName = "${suiteName}_mv"
    String mvNameReplace = "${suiteName}_mv_replace"
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
            BUILD DEFERRED REFRESH AUTO ON commit
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
            'replication_num' = '1'
            )
            AS
            SELECT * from ${tableName};
            """

    order_qt_deferred_commit "select MvName,ExecuteType,RecurringStrategy,Status from jobs('type'='mv') where MvName='${mvName}' and MvDatabaseName='${dbName}';"
    sql """drop materialized view if exists ${mvName};"""

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
    order_qt_deferred_manual "select MvName,ExecuteType,RecurringStrategy,Status from jobs('type'='mv') where MvName='${mvName}' and MvDatabaseName='${dbName}';"
    sql """drop materialized view if exists ${mvName};"""

    sql """
            CREATE MATERIALIZED VIEW ${mvName}
            BUILD immediate REFRESH AUTO ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
            'replication_num' = '1'
            )
            AS
            SELECT * from ${tableName};
            """
    order_qt_immediate_manual "select MvName,ExecuteType,RecurringStrategy,Status from jobs('type'='mv') where MvName='${mvName}' and MvDatabaseName='${dbName}';"
    sql """drop materialized view if exists ${mvName};"""

    sql """
            CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON SCHEDULE EVERY 10 second
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
            'replication_num' = '1'
            )
            AS
            SELECT * from ${tableName};
            """
    // can not select RecurringStrategy, because startTime will change
    order_qt_deferred_schedule "select MvName,ExecuteType,Status from jobs('type'='mv') where MvName='${mvName}' and MvDatabaseName='${dbName}';"
    sql """drop materialized view if exists ${mvName};"""

    sql """
            CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON SCHEDULE EVERY 10 second STARTS "9999-12-13 21:07:09"
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
            'replication_num' = '1'
            )
            AS
            SELECT * from ${tableName};
            """
    order_qt_deferred_schedule_start "select MvName,ExecuteType,RecurringStrategy,Status from jobs('type'='mv') where MvName='${mvName}' and MvDatabaseName='${dbName}';"

    sql """
        alter MATERIALIZED VIEW ${mvName} REFRESH AUTO ON manual
        """
    order_qt_alter "select MvName,ExecuteType,RecurringStrategy,Status from jobs('type'='mv') where MvName='${mvName}' and MvDatabaseName='${dbName}';"

    sql """
        pause MATERIALIZED VIEW job on ${mvName};
        """
    order_qt_pause "select MvName,ExecuteType,RecurringStrategy,Status from jobs('type'='mv') where MvName='${mvName}' and MvDatabaseName='${dbName}';"

    sql """
        resume MATERIALIZED VIEW job on ${mvName};
        """
    order_qt_resume "select MvName,ExecuteType,RecurringStrategy,Status from jobs('type'='mv') where MvName='${mvName}' and MvDatabaseName='${dbName}';"


    sql """
            CREATE MATERIALIZED VIEW ${mvNameReplace}
            BUILD DEFERRED REFRESH AUTO ON SCHEDULE EVERY 10 second STARTS "9999-12-13 21:07:09"
            DISTRIBUTED BY RANDOM BUCKETS 2
            PROPERTIES (
            'replication_num' = '1'
            )
            AS
            SELECT * from ${tableName};
            """
    sql """
        alter MATERIALIZED VIEW ${mvName} replace with  MATERIALIZED VIEW ${mvNameReplace} PROPERTIES('swap' = 'true');
        """
    order_qt_replace_true_1 "select MvName,ExecuteType,RecurringStrategy,Status from jobs('type'='mv') where MvName='${mvName}' and MvDatabaseName='${dbName}';"
    order_qt_replace_true_2 "select MvName,ExecuteType,RecurringStrategy,Status from jobs('type'='mv') where MvName='${mvNameReplace}' and MvDatabaseName='${dbName}';"

    sql """
        alter MATERIALIZED VIEW ${mvName} replace with  MATERIALIZED VIEW ${mvNameReplace} PROPERTIES('swap' = 'false');
        """
    order_qt_replace_false_1 "select MvName,ExecuteType,RecurringStrategy,Status from jobs('type'='mv') where MvName='${mvName}' and MvDatabaseName='${dbName}';"
    order_qt_replace_false_2 "select MvName,ExecuteType,RecurringStrategy,Status from jobs('type'='mv') where MvName='${mvNameReplace}' and MvDatabaseName='${dbName}';"

    sql """drop materialized view if exists ${mvName};"""
    order_qt_drop "select MvName,ExecuteType,RecurringStrategy,Status from jobs('type'='mv') where MvName='${mvName}' and MvDatabaseName='${dbName}';"
}
