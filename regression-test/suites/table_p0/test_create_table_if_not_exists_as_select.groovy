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

suite("test_create_table_if_not_exists_as_select") {
    def base_table_name = "test_create_table_if_not_exists_as_select_base_table"
    def table_name = "test_create_table_if_not_exists_as_select_table"
    sql """drop table if exists `${base_table_name}`"""
    sql """drop table if exists `${table_name}`"""
    sql """SET enable_fallback_to_original_planner=false"""

    sql """
        CREATE TABLE `${base_table_name}` (
          `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `date` DATE NOT NULL COMMENT '\"数据灌入日期时间\"',
          `num` SMALLINT NOT NULL COMMENT '\"数量\"'
        ) ENGINE=OLAP
        DUPLICATE KEY(`user_id`, `date`, `num`)
        COMMENT 'OLAP'
        PARTITION BY RANGE(`date`)
        (PARTITION p201701_1000 VALUES [('0000-01-01'), ('2017-02-01')),
        PARTITION p201702_2000 VALUES [('2017-02-01'), ('2017-03-01')),
        PARTITION p201703_3000 VALUES [('2017-03-01'), ('2017-04-01')),
        PARTITION p201704_all VALUES [('2017-04-01'), ('2017-05-01')))
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """
    try{
        sql """
        CREATE TABLE `${base_table_name}` (
          `user_id` LARGEINT NOT NULL COMMENT '\"用户id\"',
          `date` DATE NOT NULL COMMENT '\"数据灌入日期时间\"',
          `num` SMALLINT NOT NULL COMMENT '\"数量\"'
        ) ENGINE=OLAP
        DUPLICATE KEY(`user_id`, `date`, `num`)
        COMMENT 'OLAP'
        PARTITION BY RANGE(`date`)
        (PARTITION p201701_1000 VALUES [('0000-01-01'), ('2017-02-01')),
        PARTITION p201702_2000 VALUES [('2017-02-01'), ('2017-03-01')),
        PARTITION p201703_3000 VALUES [('2017-03-01'), ('2017-04-01')),
        PARTITION p201704_all VALUES [('2017-04-01'), ('2017-05-01')))
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES ('replication_num' = '1') ;
        """
    }catch (Exception e){
        println(e.getMessage())
        Assert.assertTrue(e.getMessage().contains("Table 'test_create_table_if_not_exists_as_select_base_table' already exists"))
    }
    sql """
        insert into ${base_table_name} values(1,"2017-01-15",1);
        """
    
   sql """
       create table if not exists ${table_name} PROPERTIES("replication_num"="1") as select * from ${base_table_name}
       """
    def firstExecuteCount = sql """select count(*) from ${table_name}"""
    assertEquals(1, firstExecuteCount[0][0]);
    sql """
        create table if not exists ${table_name} PROPERTIES("replication_num"="1") as select * from ${base_table_name}
        """
    def secondExecuteCount = sql """select count(*) from ${table_name}"""
    assertEquals(1, secondExecuteCount[0][0]);
    sql """
         SET enable_nereids_planner=false;
        """
    sql """drop table if exists `${table_name}`"""
    sql """
       create table if not exists ${table_name} PROPERTIES("replication_num"="1") as select * from ${base_table_name}
       """
    def originalFirstExecuteCount = sql """select count(*) from ${table_name}"""
    assertEquals(1, originalFirstExecuteCount[0][0]);
    sql """
        create table if not exists ${table_name} PROPERTIES("replication_num"="1") as select * from ${base_table_name}
        """
    def originalSecondExecuteCount = sql """select count(*) from ${table_name}"""
    assertEquals(1, originalSecondExecuteCount[0][0]);
    try{
        sql """ create table ${table_name} PROPERTIES("replication_num"="1") as select * from ${base_table_name} """
    }catch (Exception e){
        Assert.assertTrue(e.getMessage().contains("Table 'test_create_table_if_not_exists_as_select_table' already exists"));
    }
}
