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

suite("test_schema_change_datev2_with_delete") {
    sql """ SET enable_profile = true """
    def tbName = "test_schema_change_datev2_with_delete"
    def getJobState = { tableName ->
         def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
         return jobStateResult[0][9]
    }

    sql """ DROP TABLE IF EXISTS ${tbName} FORCE"""
    // Create table and disable light weight schema change
    sql """
           CREATE TABLE IF NOT EXISTS ${tbName}
           (
               `datek1` date DEFAULT '2022-01-01',
               `datek2` datetime DEFAULT '2022-01-01 11:11:11',
               `datev1` date DEFAULT '2022-01-01',
               `datev2` datetime DEFAULT '2022-01-01 11:11:11'
           )
           DUPLICATE  KEY(`datek1`,`datek2`)
           DISTRIBUTED BY HASH(`datek1`) BUCKETS 1
           PROPERTIES("replication_num" = "1", "light_schema_change" = "false");
        """
    sql """ insert into ${tbName} values('2022-01-02', '2022-01-02 11:11:11', '2022-01-02', '2022-01-02 11:11:11');"""
    sql """ insert into ${tbName} (`datek1`, `datek2`, `datev1`) values('2022-01-03', '2022-01-03 11:11:11', '2022-01-03');"""
    sql """ insert into ${tbName} (`datek1`, `datek2`, `datev2`) values('2022-01-04', '2022-01-04 11:11:11', '2022-01-04 11:11:11');"""
    sql """ insert into ${tbName} (`datek1`, `datev1`, `datev2`) values('2022-01-05', '2022-01-05', '2022-01-05 11:11:11');"""
    sql """ insert into ${tbName} (`datek2`, `datev1`, `datev2`) values('2022-01-06 11:11:11', '2022-01-06', '2022-01-06 11:11:11');"""
    qt_sql """select * from ${tbName} ORDER BY `datek1`;"""

    sql """ alter table ${tbName} modify column `datev1` datev2 DEFAULT '2022-01-01' """
    int max_try_time = 1000
    while(max_try_time--){
        String result = getJobState(tbName)
        if (result == "FINISHED") {
            sleep(3000)
            break
        } else {
            sleep(100)
            if (max_try_time < 1){
                assertEquals(1,2)
            }
        }
    }
    qt_sql """select * from ${tbName} ORDER BY `datek1`;"""

    sql """ alter  table ${tbName} modify column `datev2` datetimev2 DEFAULT '2022-01-01 11:11:11' """
    max_try_time = 1000
    while(max_try_time--){
        String result = getJobState(tbName)
        if (result == "FINISHED") {
            sleep(3000)
            break
        } else {
            sleep(100)
            if (max_try_time < 1){
                assertEquals(1,2)
            }
        }
    }
    qt_sql """select * from ${tbName} ORDER BY `datek1`;"""

    sql """ alter  table ${tbName} modify column `datev2` datetimev2(3) DEFAULT '2022-01-01 11:11:11' """
    max_try_time = 1000
    while(max_try_time--){
        String result = getJobState(tbName)
        if (result == "FINISHED") {
            sleep(3000)
            break
        } else {
            sleep(100)
            if (max_try_time < 1){
                assertEquals(1,2)
            }
        }
    }
    qt_sql """select * from ${tbName} ORDER BY `datek1`;"""

    sql """ delete from ${tbName} where `datev1`='2022-01-02';"""
    qt_sql """select * from ${tbName} ORDER BY `datek1`;"""

    sql """ delete from ${tbName} where `datev2`='2022-01-04 11:11:11.111';"""
    qt_sql """select * from ${tbName} ORDER BY `datek1`;"""

    sql """ delete from ${tbName} where `datev2`='2022-01-04 11:11:11';"""
    qt_sql """select * from ${tbName} ORDER BY `datek1`;"""

    sql """ insert into ${tbName} values('2022-01-02', '2022-01-02 11:11:11', '2022-01-02', '2022-01-02 11:11:11');"""
    sql """ insert into ${tbName} (`datek1`, `datek2`, `datev1`) values('2022-01-03', '2022-01-03 11:11:11', '2022-01-03');"""
    sql """ insert into ${tbName} (`datek1`, `datek2`, `datev2`) values('2022-01-04', '2022-01-04 11:11:11', '2022-01-04 11:11:11');"""
    sql """ insert into ${tbName} (`datek1`, `datev1`, `datev2`) values('2022-01-05', '2022-01-05', '2022-01-05 11:11:11');"""
    sql """ insert into ${tbName} (`datek2`, `datev1`, `datev2`) values('2022-01-06 11:11:11', '2022-01-06', '2022-01-06 11:11:11');"""

    qt_sql """select * from ${tbName} ORDER BY `datek1`;"""
    sql """ delete from ${tbName} where `datev1`='2022-01-01';"""
    qt_sql """select * from ${tbName} ORDER BY `datek1`;"""

    sql """ delete from ${tbName} where `datev2`='2022-01-01 11:11:11.111';"""
    qt_sql """select * from ${tbName} ORDER BY `datek1`;"""

    sql """ delete from ${tbName} where `datev2`='2022-01-01 11:11:11';"""
    qt_sql """select * from ${tbName} ORDER BY `datek1`;"""

    sql """ DROP TABLE  ${tbName} force"""
}
