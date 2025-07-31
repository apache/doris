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

suite("schema_change_check") {
    sql """ drop database if exists schema_change_check;"""
    sql """ create database schema_change_check
            PROPERTIES (
              "replication_allocation" = "tag.location.default:1"
            );
        """
    sql """ use schema_change_check;"""

    sql """ drop table if exists test_base; """
    // build base table
    sql """ 
            CREATE TABLE `test_base` (
              `k1` int NULL,
              `k2` varchar(20) NULL,
              `v1` int NULL,
              `v2` int NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`, `k2`);
        """ 

    sql """ insert into test_base values(1,2,3,4),(2,3,5,6);""" 
    // build MTMV
    sql """ CREATE MATERIALIZED VIEW test_asy_mv BUILD IMMEDIATE REFRESH AUTO ON SCHEDULE EVERY 1 MINUTE AS select * from test_base where k1 > 1; """
    sql """ REFRESH MATERIALIZED VIEW test_asy_mv complete;"""
    def job_name = getJobName("schema_change_check", "test_asy_mv");
    waitingMTMVTaskFinished(job_name)
    explain {
        sql("select * from test_base where k1 > 1")
        // hit mtmv
        contains "test_asy_mv"
    }

    // schema change
    sql """ alter table test_base modify column k2 int key;"""
    sql """ REFRESH MATERIALIZED VIEW test_asy_mv complete;"""
    waitingMTMVTaskFinished(job_name)
    explain {
        sql("select * from test_base where k1 > 1")
        // should be failed to hit mtmv
        notContains "test_asy_mv"
    }
    // make sure the schema_change job is finished   
    sleep(10000)
    sql """ REFRESH MATERIALIZED VIEW test_asy_mv complete;"""
    sleep(10000)
    explain {
        sql("select * from test_base where k1 > 1")
        // should be failed to hit mtmv
        notContains "test_asy_mv"
    }
  
    sql """ drop MATERIALIZED VIEW test_asy_mv; """
    sql """ drop table test_base;"""
    sql """ drop database schema_change_check;"""
}
