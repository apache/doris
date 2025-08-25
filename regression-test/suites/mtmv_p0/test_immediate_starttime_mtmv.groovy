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

suite("test_immediate_starttime_mtmv","mtmv") {
    String suiteName = "test_immediate_starttime_mtmv"
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
            insert into ${tableName} values (2,1),(2,2);
        """
    def currentMs = System.currentTimeMillis() + 10000;
    def dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(currentMs), ZoneId.systemDefault());
    def formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    def startTime= dateTime.format(formatter);
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        REFRESH AUTO ON SCHEDULE EVERY 1 DAY STARTS '${startTime}'
        DISTRIBUTED BY hash(k2) BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        SELECT * from ${tableName};
        """
    Thread.sleep(20000)
    order_qt_immediate "SELECT count(*) from tasks('type'='mv') where MvName='${mvName}'"

    sql """drop materialized view if exists ${mvName};"""
    currentMs = System.currentTimeMillis() + 10000;
    dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(currentMs), ZoneId.systemDefault());
    formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
     startTime= dateTime.format(formatter);
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        build deferred REFRESH AUTO ON SCHEDULE EVERY 1 DAY STARTS '${startTime}'
        DISTRIBUTED BY hash(k2) BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        SELECT * from ${tableName};
        """
    Thread.sleep(20000)
     order_qt_deferred "SELECT count(*) from tasks('type'='mv') where MvName='${mvName}'"
     sql """drop table if exists `${tableName}`"""
     sql """drop materialized view if exists ${mvName};"""
}
