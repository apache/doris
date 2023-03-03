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

suite("test_delete") {
    def tableName = "delete_regression_test"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE IF NOT EXISTS ${tableName} (c1 varchar(190) NOT NULL COMMENT "",c2 bigint(20) NOT NULL COMMENT "", c3 varchar(160) NULL COMMENT "" ) ENGINE=OLAP DUPLICATE KEY(c1, c2) COMMENT "OLAP" DISTRIBUTED BY HASH(c3) BUCKETS 3 
    PROPERTIES ( "replication_num" = "1" );"""

    sql """INSERT INTO ${tableName} VALUES ('abcdef',1,'fjdsajfldjafljdslajfdl'),('abcdef',2,'fjdsajfldjafljdslajfdl'),('abcdef',4,'fjdsajfldjafljdslajfdl'),('abcdef',5,'fjdsajfldjafljdslajfdl')"""
    sql """delete from ${tableName} where c1 = 'fjdsajfldjafljdslajfdl';"""
    sql """INSERT INTO ${tableName} VALUES ('abcdef',1,'fjdsajfldjafljdslajfdl'),('abcdef',2,'fjdsajfldjafljdslajfdl'),('abcdef',4,'fjdsajfldjafljdslajfdl'),('abcdef',5,'fjdsajfldjafljdslajfdl');"""
    
    qt_sql """select count(*) from ${tableName};"""
    qt_sql """select count(c2) from ${tableName};"""
    qt_sql """select count(c2) from ${tableName} where c1 = 'abcdef';"""
    qt_sql """select count(c1) from ${tableName};"""
    qt_sql """select count(c1) from ${tableName} where c1 = 'abcdef';"""

    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """ CREATE TABLE IF NOT EXISTS delete_regression_test (k1 varchar(190) NOT NULL COMMENT "", k2 DATEV2 NOT NULL COMMENT "", k3 DATETIMEV2 NOT NULL COMMENT "", k4 DATETIMEV2(3) NOT NULL COMMENT "", v1 DATEV2 NOT NULL COMMENT "", v2 DATETIMEV2 NOT NULL COMMENT "", v3 DATETIMEV2(3) NOT NULL COMMENT "" ) ENGINE=OLAP DUPLICATE KEY(k1, k2, k3, k4) COMMENT "OLAP" DISTRIBUTED BY HASH(k1, k2, k3, k4) BUCKETS 3
    PROPERTIES ( "replication_num" = "1" );"""

    sql """ INSERT INTO delete_regression_test VALUES ('abcdef','2022-08-16','2022-08-16 11:11:11.111111','2022-08-16 11:11:11.111111','2022-08-16','2022-08-16 11:11:11.111111','2022-08-16 11:11:11.111111'),('abcdef','2022-08-12','2022-08-16 12:11:11.111111','2022-08-16 12:11:11.111111','2022-08-12','2022-08-16 12:11:11.111111','2022-08-16 12:11:11.111111'); """
    sql """ delete from ${tableName} where k2 = '2022-08-16' """
    qt_sql1 """select * from ${tableName} ORDER BY k2;"""
    sql """ delete from ${tableName} where k1 = 'abcdef' """

    sql """ INSERT INTO ${tableName} VALUES ('abcdef','2022-08-16','2022-08-16 11:11:11.111111','2022-08-16 11:11:11.111111','2022-08-16','2022-08-16 11:11:11.111111','2022-08-16 11:11:11.111111'),('abcdef','2022-08-12','2022-08-16 12:11:11.111111','2022-08-16 12:11:11.111111','2022-08-12','2022-08-16 12:11:11.111111','2022-08-16 12:11:11.111111'); """
    sql """ delete from ${tableName} where k3 = '2022-08-16 11:11:11' """
    qt_sql2 """select * from ${tableName} ORDER BY k2;"""
    sql """ delete from ${tableName} where k1 = 'abcdef' """

    sql """ INSERT INTO ${tableName} VALUES ('abcdef','2022-08-16','2022-08-16 11:11:11.111111','2022-08-16 11:11:11.111111','2022-08-16','2022-08-16 11:11:11.111111','2022-08-16 11:11:11.111111'),('abcdef','2022-08-12','2022-08-16 12:11:11.111111','2022-08-16 12:11:11.111111','2022-08-12','2022-08-16 12:11:11.111111','2022-08-16 12:11:11.111111'); """
    sql """ delete from ${tableName} where k4 = '2022-08-16 11:11:11' """
    qt_sql3 """select * from ${tableName} ORDER BY k2;"""
    sql """ delete from ${tableName} where k4 = '2022-08-16 11:11:11.111' """
    qt_sql4 """select * from ${tableName} ORDER BY k2;"""
    sql """ delete from delete_regression_test where k1 = 'abcdef' """

    sql """ INSERT INTO ${tableName} VALUES ('abcdef','2022-08-16','2022-08-16 11:11:11.111111','2022-08-16 11:11:11.111111','2022-08-16','2022-08-16 11:11:11.111111','2022-08-16 11:11:11.111111'),('abcdef','2022-08-12','2022-08-16 12:11:11.111111','2022-08-16 12:11:11.111111','2022-08-12','2022-08-16 12:11:11.111111','2022-08-16 12:11:11.111111'); """
    sql """ delete from ${tableName} where v1 = '2022-08-16' """
    qt_sql5 """select * from ${tableName} ORDER BY k2;"""
    sql """ delete from delete_regression_test where k1 = 'abcdef' """

    sql """ INSERT INTO ${tableName} VALUES ('abcdef','2022-08-16','2022-08-16 11:11:11.111111','2022-08-16 11:11:11.111111','2022-08-16','2022-08-16 11:11:11.111111','2022-08-16 11:11:11.111111'),('abcdef','2022-08-12','2022-08-16 12:11:11.111111','2022-08-16 12:11:11.111111','2022-08-12','2022-08-16 12:11:11.111111','2022-08-16 12:11:11.111111'); """
    sql """ delete from ${tableName} where v2 = '2022-08-16 11:11:11' """
    qt_sql6 """select * from ${tableName} ORDER BY k2;"""
    sql """ delete from delete_regression_test where k1 = 'abcdef' """

    sql """ INSERT INTO ${tableName} VALUES ('abcdef','2022-08-16','2022-08-16 11:11:11.111111','2022-08-16 11:11:11.111111','2022-08-16','2022-08-16 11:11:11.111111','2022-08-16 11:11:11.111111'),('abcdef','2022-08-12','2022-08-16 12:11:11.111111','2022-08-16 12:11:11.111111','2022-08-12','2022-08-16 12:11:11.111111','2022-08-16 12:11:11.111111'); """
    sql """ delete from ${tableName} where v3 = '2022-08-16 11:11:11' """
    qt_sql7 """select * from ${tableName} ORDER BY k2;"""
    sql """ delete from ${tableName} where v3 = '2022-08-16 11:11:11.111' """
    qt_sql8 """select * from ${tableName} ORDER BY k2;"""
    sql """ delete from delete_regression_test where k1 = 'abcdef' """

    sql """ DROP TABLE IF EXISTS ${tableName} """
}
