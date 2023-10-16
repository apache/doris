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

suite("test_create_table_with_binlog_config") {
    def syncer = getSyncer()
    if (!syncer.checkEnableFeatureBinlog()) {
        logger.info("fe enable_feature_binlog is false, skip case test_create_table_with_binlog_config")
        return
    }
    sql "drop database if exists test_table_binlog"

    sql """
        create database test_table_binlog
        """
    def result = sql "show create database test_table_binlog"
    logger.info("${result}")

    // Case 1: database disable binlog, create table with binlog disable
    sql """
        CREATE TABLE test_table_binlog.t1 ( k1 INT ) ENGINE = olap DISTRIBUTED BY HASH(k1) BUCKETS 3 PROPERTIES ( "replication_num" = "1", "binlog.enable" = "false" );
        """
    result = sql "show create table test_table_binlog.t1"
    logger.info("${result}")
    assertTrue(result.toString().containsIgnoreCase('"binlog.enable" = "false"'))
    sql """
        drop table if exists test_table_binlog.t1
        """

    // Case 2: database disable binlog, create table with binlog enable
    sql """
        CREATE TABLE test_table_binlog.t1 ( k1 INT ) ENGINE = olap DISTRIBUTED BY HASH(k1) BUCKETS 3 PROPERTIES ( "replication_num" = "1", "binlog.enable" = "true" );
        """
    result = sql "show create table test_table_binlog.t1"
    logger.info("${result}")
    assertTrue(result.toString().containsIgnoreCase('"binlog.enable" = "true"'))
    sql """
        drop table if exists test_table_binlog.t1
        """

    // Case 3: database enable binlog, create table with binlog disable
    sql """
        alter database test_table_binlog set properties ("binlog.enable" = "true")
        """
    assertThrows(Exception.class, {
        sql """
            CREATE TABLE test_table_binlog.t1 ( k1 INT ) ENGINE = olap DISTRIBUTED BY HASH(k1) BUCKETS 3 PROPERTIES ( "replication_num" = "1", "binlog.enable" = "false" );
            """
    })
    sql """
        drop table if exists test_table_binlog.t1
        """

    // Case 4: database enable binlog, create table with binlog enable
    sql """
        alter database test_table_binlog set properties ("binlog.enable" = "true")
        """
    sql """
        CREATE TABLE test_table_binlog.t1 ( k1 INT ) ENGINE = olap DISTRIBUTED BY HASH(k1) BUCKETS 3 PROPERTIES ( "replication_num" = "1", "binlog.enable" = "true" );
        """
    result = sql "show create table test_table_binlog.t1"
    logger.info("${result}")
    assertTrue(result.toString().containsIgnoreCase('"binlog.enable" = "true"'))
    sql """
        drop table if exists test_table_binlog.t1
        """

    // Case 5: database enable binlog, create table inherit database binlog config
    sql """
        CREATE TABLE test_table_binlog.t1 ( k1 INT ) ENGINE = olap DISTRIBUTED BY HASH(k1) BUCKETS 3 PROPERTIES ( "replication_num" = "1" );
        """
    result = sql "show create table test_table_binlog.t1"
    logger.info("${result}")
    assertTrue(result.toString().containsIgnoreCase('"binlog.enable" = "true"'))
    sql """
        drop table if exists test_table_binlog.t1
        """

    sql "drop database if exists test_table_binlog"
}
